/* rtp.h - user-space reliable transport over UDP ("UDP-to-TCP engine"), C port.
 *
 * Single-threaded, event-driven: the application owns the loop and calls
 * rt_run_once() to drive sockets and timers. No locks inside the library.
 * Wire format is identical to the Python version (the two interoperate).
 */
#ifndef RTP_H
#define RTP_H

#include <stdint.h>
#include <stddef.h>
#include <netinet/in.h>

#define RT_MSS_MAX 1400

/* ------------------------------------------------------------ codec */
#define F_SYN   0x01
#define F_ACK   0x02
#define F_FIN   0x04
#define F_RST   0x08
#define F_SACK  0x10
#define F_PROBE 0x20
#define F_FEC   0x40   /* parity packet (forward error correction) */
#define F_STREAMS 0x80 /* on SYN / SYN-ACK: peer speaks the multi-stream mode */
#define RT_MAX_STREAMS 16

#define RT_HDR_LEN 20
#define RT_MAX_SACK 4

typedef struct {
    uint32_t conn_id, seq, ack;
    uint8_t flags;
    uint32_t window;                 /* bytes (wire unit = 256 B) */
    uint16_t plen;
    const uint8_t *payload;
    int nsack;
    uint32_t sack[RT_MAX_SACK][2];   /* [start,end) ranges */
} rt_pkt;

uint16_t rt_cksum(const uint8_t *d, size_t n);
size_t rt_pkt_encode(const rt_pkt *p, uint8_t *out);          /* returns wire length */
int rt_pkt_decode(const uint8_t *buf, size_t len, rt_pkt *p); /* 0 ok, <0 malformed/checksum */

/* ------------------------------------------------------------ congestion control */
enum { CC_RENO = 0, CC_CUBIC = 1 };

typedef struct {
    int kind;
    double mss, cwnd, ssthresh, max_cwnd;
    int in_recovery;
    /* cubic */
    double w_max, epoch_start, k, w_tcp, min_rtt;
    int have_epoch;
} rt_cc;

void rt_cc_init(rt_cc *c, int kind, int mss, int init_cwnd_mss, double init_ssthresh);
void rt_cc_on_ack(rt_cc *c, double acked, int have_rtt, double rtt, double now);
void rt_cc_on_dup_ack(rt_cc *c);
void rt_cc_on_fast_retransmit(rt_cc *c, double flight, double now);
void rt_cc_on_partial_ack(rt_cc *c, double acked);
void rt_cc_on_recovery_exit(rt_cc *c);
void rt_cc_on_timeout(rt_cc *c, double flight, double now);
uint32_t rt_cc_window(const rt_cc *c);
const char *rt_cc_phase(const rt_cc *c);

/* ------------------------------------------------------------ transport */
typedef struct {
    int mss;                 /* <= RT_MSS_MAX */
    int init_cwnd_mss;
    int init_ssthresh_mss;
    int cc;                  /* CC_RENO | CC_CUBIC */
    uint32_t recv_buf;       /* max advertised window */
    uint32_t send_buf;       /* bounded send buffer */
    double init_rto, min_rto, max_rto;
    int max_retries;
    int dupack_thresh;
    double time_wait;
    int trace;               /* record cwnd/RTT time series */
    int verbose;
    int tlp;                 /* tail loss probes (default on) */
    int rack;                /* RACK-style time-based loss detection (default on) */
    int fec;                 /* forward error correction: 1 parity packet per N data packets (0 = off) */
    int fec_auto;            /* adapt N to the measured loss rate */
    int streams;             /* ask the peer for the multi-stream mode (independent streams, no head-of-line blocking) */
    double tlp_min;          /* minimum probe timeout, seconds */
} rt_cfg;

void rt_cfg_default(rt_cfg *cfg);

typedef enum {
    ST_CLOSED, ST_LISTEN, ST_SYN_SENT, ST_SYN_RCVD, ST_ESTABLISHED,
    ST_FIN_WAIT_1, ST_FIN_WAIT_2, ST_CLOSING, ST_TIME_WAIT, ST_CLOSE_WAIT, ST_LAST_ACK
} rt_state;
const char *rt_state_name(rt_state s);

typedef struct rt_ep rt_ep;
typedef struct rt_conn rt_conn;

typedef struct {
    uint64_t packets_sent, packets_received, retransmissions, fast_retransmits, timeouts;
    uint64_t dup_acks, dup_packets, ooo_packets, bytes_sent, bytes_received;
    uint64_t window_probes, window_drops, checksum_failures, tlp_probes, rack_retx, spurious, fec_sent, fec_recovered;
    int cwnd, ssthresh, fec_k, streams;
    double srtt_ms, rttvar_ms, rto_ms, duration_s;
    const char *cc, *state;
} rt_stats;

rt_ep *rt_ep_new(const char *bind_ip, int port, const rt_cfg *cfg);   /* NULL on error */
void rt_ep_free(rt_ep *ep);
int rt_ep_port(rt_ep *ep);
void rt_listen(rt_ep *ep);
rt_conn *rt_accept(rt_ep *ep);                       /* NULL if none pending */
rt_conn *rt_connect(rt_ep *ep, const struct sockaddr_in *addr);
int rt_run_once(rt_ep *ep, int timeout_ms);          /* poll + process packets + timers */

int rt_send(rt_conn *c, const void *data, size_t len);   /* bytes queued (0 = buffer full), -1 error */
int rt_recv(rt_conn *c, void *buf, size_t cap);          /* >0 bytes, 0 none now, -1 EOF, -2 error */
void rt_close(rt_conn *c);                               /* graceful: FIN queued behind data */
int rt_flushed(rt_conn *c);                              /* all data acknowledged */
rt_state rt_conn_state(rt_conn *c);
int rt_conn_error(rt_conn *c);                           /* errno-style, 0 = none */
uint32_t rt_conn_id(rt_conn *c);
void rt_conn_peer(rt_conn *c, struct sockaddr_in *out);
void rt_stats_get(rt_conn *c, rt_stats *s);
int rt_trace_write(rt_conn *c, const char *path);        /* CSV */
void rt_conn_free(rt_conn *c);

/* time series + loss events recorded per connection (for graphs / the web UI) */
typedef struct { double t, cwnd, ssthresh, flight, srtt, rto, rwnd; int phase; } rt_trace_pt; /* phase: 0 slow start, 1 cong. avoidance, 2 fast recovery */
typedef struct { double t; int kind; } rt_event;                                         /* 0 fast retransmit, 1 RTO, 2 tail-loss probe */
size_t rt_trace_len(rt_conn *c);
const rt_trace_pt *rt_trace_data(rt_conn *c);
size_t rt_event_len(rt_conn *c);
const rt_event *rt_event_data(rt_conn *c);
uint64_t rt_bytes_acked(rt_conn *c);
/* per data segment: how many times it had to be retransmitted (0 = delivered first time). Valid once acked. */
size_t rt_segment_history(rt_conn *c, const uint8_t **out);

/* Multi-stream mode (negotiated in the handshake when cfg.streams is set on the connecting side).
 * Reliability (ACKs, retransmission, congestion control) stays per connection, but DELIVERY is per stream:
 * a lost packet only stalls the stream it belonged to, not every stream behind it. */
int rt_stream_send(rt_conn *c, unsigned sid, const void *data, size_t len, int fin);  /* bytes queued, -1 error */
int rt_stream_recv(rt_conn *c, unsigned sid, void *buf, size_t cap);                  /* >0 bytes, 0 none now, -1 stream finished */
int rt_conn_is_streams(rt_conn *c);
/* bytes received but not yet deliverable to the application because earlier data is still missing
 * (ordered mode: the out-of-order buffer; stream mode: out-of-order frames inside each stream) */
size_t rt_held_bytes(rt_conn *c);

double rt_now(void);
int rt_resolve(const char *hostport, struct sockaddr_in *out);   /* "host:port" */

/* ------------------------------------------------------------ impairment proxy */
typedef struct {
    double loss, dup, reorder, reorder_ms, corrupt;   /* percents / ms */
    double delay_ms, jitter_ms, rate_mbps;
    int queue;                                        /* bottleneck queue, packets */
    uint64_t seed;
} imp_cfg;
typedef struct { uint64_t rx, dropped_loss, dropped_queue, dup, reordered, corrupted; } imp_dir_stats;
typedef struct proxy proxy_t;

void imp_cfg_default(imp_cfg *c);
int imp_active(const imp_cfg *c);
int imp_preset(const char *name, imp_cfg *c);        /* 0 ok, -1 unknown */
const char *imp_preset_names(void);
int imp_preset_at(int idx, const char **name, imp_cfg *c);   /* 0 ok, -1 past the end */
void imp_describe(const imp_cfg *c, char *out, size_t cap);
proxy_t *proxy_new(const char *bind_ip, int listen_port, const struct sockaddr_in *target, const imp_cfg *cfg);
int proxy_port(proxy_t *p);
void proxy_run(proxy_t *p, volatile int *stop);          /* blocking loop */
void proxy_stats(proxy_t *p, imp_dir_stats *fwd, imp_dir_stats *rev);
void proxy_free(proxy_t *p);

/* ------------------------------------------------------------ sha256 */
typedef struct { uint32_t h[8]; uint64_t len; uint8_t buf[64]; size_t n; } sha256_ctx;
void sha256_init(sha256_ctx *c);
void sha256_update(sha256_ctx *c, const void *d, size_t n);
void sha256_final(sha256_ctx *c, char hex[65]);

#endif
