/* conn.c - transport core: endpoint (UDP socket, demux, timers) and connection
 * (11-state machine, sliding window, cumulative ACK + SACK, out-of-order buffer,
 * SRTT/RTTVAR RTO with Karn + backoff, fast retransmit/recovery, flow control,
 * zero-window probes). Single-threaded: everything runs inside rt_run_once()
 * or the rt_send/rt_recv/rt_close calls made by the same thread. */
#define _GNU_SOURCE
#include "rtp.h"
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#define SEG_MAX 2048          /* max outstanding segments (ring) */
#define OOO_MAX 1024          /* max buffered out-of-order segments */
#define MAX_CONNS 64
#define ACCEPTQ 16
#define TRACE_MAX 400000

/* serial-number arithmetic: sequence numbers may wrap */
#define SEQ_LT(a, b)  ((int32_t)((a) - (b)) < 0)
#define SEQ_LEQ(a, b) ((int32_t)((a) - (b)) <= 0)
#define SEQ_GT(a, b)  SEQ_LT(b, a)
#define SEQ_GEQ(a, b) SEQ_LEQ(b, a)

double rt_now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

void rt_cfg_default(rt_cfg *c) {
    *c = (rt_cfg){.mss = 1400, .init_cwnd_mss = 2, .init_ssthresh_mss = 64, .cc = CC_RENO,
                  .recv_buf = 512 * 1024, .send_buf = 1024 * 1024, .init_rto = 0.5, .min_rto = 0.02,
                  .max_rto = 8.0, .max_retries = 12, .dupack_thresh = 3, .time_wait = 0.5,
                  .trace = 1, .verbose = 0, .tlp = 1, .rack = 1, .tlp_min = 0.004};
}

static const char *STN[] = {"CLOSED", "LISTEN", "SYN_SENT", "SYN_RCVD", "ESTABLISHED", "FIN_WAIT_1",
                            "FIN_WAIT_2", "CLOSING", "TIME_WAIT", "CLOSE_WAIT", "LAST_ACK"};
const char *rt_state_name(rt_state s) { return STN[s]; }

struct seg {
    uint32_t seq, end;
    uint16_t len;
    uint8_t flags, sacked;
    int retx, rec;
    uint32_t idx;                  /* data-segment number (for the repair map), or UINT32_MAX */
    double sent;
    uint8_t data[RT_MSS_MAX];
};
struct ooo {
    uint32_t seq;
    uint16_t len;
    uint8_t fin, used;
    uint8_t data[RT_MSS_MAX];
};
typedef rt_trace_pt trace_row;

#define STREAM_BUF (128 * 1024)
#define STREAM_PEND 384
#define FH_SLOTS 512
#define FEC_MAX_K 16
struct fhe { uint32_t seq; uint16_t len; uint8_t used; uint8_t data[RT_MSS_MAX]; };       /* FEC receiver: recent segments */
struct pend { uint32_t off; uint16_t len; uint8_t used; uint8_t data[RT_MSS_MAX]; };
struct sstream {
    uint8_t *sq; uint32_t sq_head, sq_len, snd_off; int fin_pending, fin_sent;           /* send side */
    uint8_t *rb; uint32_t rb_head, rb_len, rcv_off; int fin_known; uint32_t fin_off;     /* receive side */
    struct pend *pend;                                                                    /* out-of-order frames of this stream */
};

struct rt_conn {
    rt_ep *ep;
    struct sockaddr_in peer;
    uint32_t id;
    rt_cfg cfg;
    int in_use, passive;
    rt_state state;
    uint32_t local_isn, peer_isn;
    /* send side */
    uint32_t snd_una, snd_nxt;
    uint8_t *sq; uint32_t sq_head, sq_len;                 /* unsent application bytes (ring) */
    struct seg *segs; uint32_t seg_head, seg_cnt;          /* outstanding segments (ring) */
    uint32_t highest_sacked; int have_sacked;
    int fin_pending, fin_sent, fin_acked; uint32_t fin_seq;
    /* receive side */
    uint32_t rcv_nxt;
    uint8_t *rq; uint32_t rq_head, rq_len;                 /* in-order bytes for the app (ring) */
    struct ooo *ooo; uint32_t ooo_cnt, ooo_bytes; uint32_t last_ooo; int have_last_ooo;
    int peer_fin; uint32_t last_adv, rwnd;
    /* congestion / loss recovery */
    rt_cc cc;
    int dupacks, in_recovery; uint32_t recover; int epoch;
    /* timers (absolute monotonic seconds, 0 = off) */
    double srtt, rttvar, rto, base_rto; int have_srtt;
    double rto_deadline, tw_deadline, persist_deadline, tlp_deadline; int retries, persist_n;
    double rack_sent, rack_rtt, min_rtt, rack_deadline; int reo_mult;      /* RACK state */
    int error;
    double t0, t_established, t_closed, last_trace;
    rt_stats st;
    trace_row *trace; size_t trace_n, trace_cap;
    rt_event *ev; size_t ev_n, ev_cap;
    uint8_t *hist; uint32_t hist_n, hist_cap;                  /* retransmit count per data segment */
    /* forward error correction */
    int fec_k, fec_n; uint32_t fec_seq0; uint16_t fec_lens[FEC_MAX_K]; uint8_t fec_acc[RT_MSS_MAX]; uint16_t fec_max;
    uint32_t win_sent; uint64_t win_retx0; double loss_ewma;
    struct fhe *fh;
    /* multi-stream mode */
    int streams, rr; uint32_t stream_buffered;
    struct sstream strm[RT_MAX_STREAMS];
};

struct rt_ep {
    int fd;
    rt_cfg cfg;
    rt_conn *conns[MAX_CONNS];
    int listening;
    rt_conn *accq[ACCEPTQ]; int acc_n;
    uint64_t bad_packets;
    uint8_t rxbuf[65600];
};

/* ====================================================== helpers */
static void logc(rt_conn *c, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#include <stdarg.h>
static void logc(rt_conn *c, const char *fmt, ...) {
    if (!c->cfg.verbose) return;
    va_list ap;
    fprintf(stderr, "[%8.4fs conn %08x %11s] ", rt_now() - c->t0, c->id, STN[c->state]);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

static void set_state(rt_conn *c, rt_state s) {
    if (s == c->state) return;
    logc(c, "state %s -> %s", STN[c->state], STN[s]);
    c->state = s;
}

static uint32_t adv_window(rt_conn *c) {
    uint32_t used = c->rq_len + c->ooo_bytes + c->stream_buffered;
    uint32_t fr = used >= c->cfg.recv_buf ? 0 : c->cfg.recv_buf - used;
    uint32_t th = (uint32_t)c->cfg.mss < c->cfg.recv_buf / 2 ? (uint32_t)c->cfg.mss : c->cfg.recv_buf / 2;
    if (fr < th) fr = 0;                                /* receiver-side SWS avoidance */
    return fr;
}

static void emit(rt_conn *c, uint8_t flags, uint32_t seq, const uint8_t *pl, uint16_t plen,
                 uint32_t (*sack)[2], int nsack) {
    rt_pkt p = {.conn_id = c->id, .seq = seq, .flags = flags, .plen = plen, .payload = pl, .nsack = nsack};
    p.window = adv_window(c);
    c->last_adv = p.window;
    p.ack = (flags & F_ACK) ? c->rcv_nxt : 0;
    for (int i = 0; i < nsack && i < RT_MAX_SACK; i++) { p.sack[i][0] = sack[i][0]; p.sack[i][1] = sack[i][1]; }
    uint8_t buf[RT_HDR_LEN + 8 * RT_MAX_SACK + 1 + 2 * 16 + RT_MSS_MAX + 64];
    size_t n = rt_pkt_encode(&p, buf);
    if (c->cfg.verbose && (flags & (F_SYN | F_FIN | F_RST)))
        logc(c, "TX %s%s%s%s seq=%u ack=%u len=%u", flags & F_SYN ? "SYN|" : "", flags & F_ACK ? "ACK|" : "",
             flags & F_FIN ? "FIN|" : "", flags & F_RST ? "RST|" : "", seq, p.ack, plen);
    sendto(c->ep->fd, buf, n, 0, (struct sockaddr *)&c->peer, sizeof c->peer);
    c->st.packets_sent++;
}

/* ---- byte rings (memcpy in at most two pieces) */
static void ring_in(uint8_t *ring, uint32_t cap, uint32_t pos, const uint8_t *src, uint32_t n) {
    pos %= cap;
    uint32_t first = cap - pos < n ? cap - pos : n;
    memcpy(ring + pos, src, first);
    if (n > first) memcpy(ring, src + first, n - first);
}
static void ring_out(const uint8_t *ring, uint32_t cap, uint32_t pos, uint8_t *dst, uint32_t n) {
    pos %= cap;
    uint32_t first = cap - pos < n ? cap - pos : n;
    memcpy(dst, ring + pos, first);
    if (n > first) memcpy(dst + first, ring, n - first);
}

/* ---- segment ring */
static struct seg *seg_at(rt_conn *c, uint32_t i) { return &c->segs[(c->seg_head + i) % SEG_MAX]; }
static void seg_pop(rt_conn *c) { c->seg_head = (c->seg_head + 1) % SEG_MAX; c->seg_cnt--; }
static struct seg *seg_push(rt_conn *c, uint32_t seq, const uint8_t *data, uint16_t len, uint8_t flags) {
    if (c->seg_cnt >= SEG_MAX) return NULL;
    struct seg *s = seg_at(c, c->seg_cnt++);
    s->seq = seq; s->len = len; s->flags = flags; s->sacked = 0; s->retx = 0; s->rec = -1; s->sent = 0; s->idx = 0xFFFFFFFFu;
    s->end = seq + len + ((flags & F_SYN) ? 1 : 0) + ((flags & F_FIN) ? 1 : 0);
    if (len) memcpy(s->data, data, len);
    return s;
}

static void transmit(rt_conn *c, struct seg *s) {
    uint8_t fl = s->flags;
    if (c->state != ST_SYN_SENT) fl |= F_ACK;
    s->sent = rt_now();
    emit(c, fl, s->seq, s->data, s->len, NULL, 0);
}

static void retransmit(rt_conn *c, struct seg *s) {
    s->retx++;
    s->rec = c->epoch;
    c->st.retransmissions++;
    transmit(c, s);
}

static void add_event(rt_conn *c, double now, int kind) {
    if (c->ev_n >= 100000) return;
    if (c->ev_n == c->ev_cap) { c->ev_cap = c->ev_cap ? c->ev_cap * 2 : 256; c->ev = realloc(c->ev, c->ev_cap * sizeof(rt_event)); }
    c->ev[c->ev_n++] = (rt_event){now - c->t0, kind};
}

/* (Re)arm the retransmission timer and, if enabled, the tail-loss-probe timer (RFC 8985 style):
 * if ACKs stall for ~2*SRTT, resend the LAST segment to provoke an ACK/SACK instead of
 * waiting for the much longer RTO. */
static void arm_rto(rt_conn *c, double now) {
    c->rto_deadline = now + c->rto;
    c->tlp_deadline = 0;
    if (c->cfg.tlp && c->have_srtt && !c->in_recovery && c->retries == 0 &&
        (c->state == ST_ESTABLISHED || c->state == ST_CLOSE_WAIT || c->state == ST_FIN_WAIT_1 || c->state == ST_LAST_ACK)) {
        double pto = 2 * c->srtt;
        if (pto < c->cfg.tlp_min) pto = c->cfg.tlp_min;
        if (pto < c->rto) c->tlp_deadline = now + pto;
    }
}

static void record(rt_conn *c, double now, int force) {
    if (!c->cfg.trace || c->trace_n >= TRACE_MAX) return;
    double t = now - c->t0;
    if (!force && t - c->last_trace < 0.0002) return;
    c->last_trace = t;
    if (c->trace_n == c->trace_cap) {
        c->trace_cap = c->trace_cap ? c->trace_cap * 2 : 1024;
        c->trace = realloc(c->trace, c->trace_cap * sizeof(trace_row));
    }
    c->trace[c->trace_n++] = (trace_row){t, c->cc.cwnd, c->cc.ssthresh < 1e17 ? c->cc.ssthresh : 0,
                                         (double)(c->snd_nxt - c->snd_una), c->have_srtt ? c->srtt : 0,
                                         c->rto, (double)c->rwnd,
                                         c->cc.in_recovery ? 2 : (c->cc.cwnd < c->cc.ssthresh ? 0 : 1)};
}

static int ooo_cmp_key;
struct rng { uint32_t s, e; };
static uint32_t cmp_base;
static int cmp_rng(const void *a, const void *b) {
    uint32_t x = ((const struct rng *)a)->s - cmp_base, y = ((const struct rng *)b)->s - cmp_base;
    return x < y ? -1 : x > y;
}

/* SACK blocks from the out-of-order buffer: merged ranges, most recent first (RFC 2018) */
static int build_sack(rt_conn *c, uint32_t (*out)[2]) {
    if (!c->ooo_cnt) return 0;
    static __thread struct rng tmp[OOO_MAX];
    int n = 0;
    for (int i = 0; i < OOO_MAX && n < (int)c->ooo_cnt; i++)
        if (c->ooo[i].used) {
            tmp[n].s = c->ooo[i].seq;
            tmp[n].e = c->ooo[i].seq + c->ooo[i].len + c->ooo[i].fin;
            n++;
        }
    cmp_base = c->rcv_nxt;
    qsort(tmp, n, sizeof tmp[0], cmp_rng);
    int m = 0;
    for (int i = 0; i < n; i++) {
        if (m && tmp[m - 1].e == tmp[i].s) tmp[m - 1].e = tmp[i].e;
        else tmp[m++] = tmp[i];
    }
    int first = -1;
    if (c->have_last_ooo)
        for (int i = 0; i < m; i++)
            if (SEQ_LEQ(tmp[i].s, c->last_ooo) && SEQ_LT(c->last_ooo, tmp[i].e)) { first = i; break; }
    int k = 0;
    if (first >= 0) { out[k][0] = tmp[first].s; out[k][1] = tmp[first].e; k++; }
    for (int i = 0; i < m && k < RT_MAX_SACK; i++)
        if (i != first) { out[k][0] = tmp[i].s; out[k][1] = tmp[i].e; k++; }
    (void)ooo_cmp_key;
    return k;
}

static void send_ack(rt_conn *c) {
    uint32_t sk[RT_MAX_SACK][2];
    int n = build_sack(c, sk);
    emit(c, F_ACK, c->snd_nxt, NULL, 0, sk, n);
}

/* ====================================================== open / close */
static void ep_remove_conn_notify(rt_conn *c);

static void close_now(rt_conn *c, int err) {
    if (c->state == ST_CLOSED) return;
    if (err && !c->error) c->error = err;
    set_state(c, ST_CLOSED);
    c->rto_deadline = c->tw_deadline = c->persist_deadline = c->tlp_deadline = c->rack_deadline = 0;
    c->t_closed = rt_now();
    ep_remove_conn_notify(c);
}

static void abort_conn(rt_conn *c, int err) {
    if (c->state != ST_CLOSED && c->state != ST_SYN_SENT && c->state != ST_LISTEN)
        emit(c, F_RST | F_ACK, c->snd_nxt, NULL, 0, NULL, 0);
    close_now(c, err);
}

static void established(rt_conn *c, double now) {
    set_state(c, ST_ESTABLISHED);
    c->t_established = now;
    if (c->passive && c->ep->acc_n < ACCEPTQ) c->ep->accq[c->ep->acc_n++] = c;
}

static void enter_time_wait(rt_conn *c, double now) {
    set_state(c, ST_TIME_WAIT);
    c->tw_deadline = now + c->cfg.time_wait;
    c->rto_deadline = 0;
}

static void check_fin_acked(rt_conn *c, double now) {
    if (c->fin_sent && !c->fin_acked && SEQ_GT(c->snd_una, c->fin_seq)) {
        c->fin_acked = 1;
        if (c->state == ST_FIN_WAIT_1) set_state(c, ST_FIN_WAIT_2);
        else if (c->state == ST_CLOSING) enter_time_wait(c, now);
        else if (c->state == ST_LAST_ACK) close_now(c, 0);
    }
}

/* ====================================================== sender */
static inline void put32(uint8_t *p, uint32_t v) { p[0] = v >> 24; p[1] = v >> 16; p[2] = v >> 8; p[3] = v; }
static inline uint32_t get32(const uint8_t *p) { return ((uint32_t)p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]; }

static int send_pending(rt_conn *c) {
    if (c->sq_len) return 1;
    if (c->streams)
        for (int i = 0; i < RT_MAX_STREAMS; i++) { struct sstream *s = &c->strm[i]; if (s->sq_len || (s->fin_pending && !s->fin_sent)) return 1; }
    return 0;
}

/* Produce the payload of the next data segment, from the single byte queue (classic mode) or from the
 * per-stream queues (round robin, one stream frame per segment: [sid:2][offset:4][fin:1][data]).
 * Returns 1 = produced, 0 = nothing to send, -1 = blocked by the window. */
static int next_payload(rt_conn *c, int64_t avail, uint8_t *tmp, uint32_t *len) {
    if (!c->streams) {
        if (!c->sq_len) return 0;
        uint32_t n = c->sq_len < (uint32_t)c->cfg.mss ? c->sq_len : (uint32_t)c->cfg.mss;
        if (avail < (int64_t)n || c->seg_cnt >= SEG_MAX) return -1;
        ring_out(c->sq, c->cfg.send_buf, c->sq_head, tmp, n);
        c->sq_head = (c->sq_head + n) % c->cfg.send_buf;
        c->sq_len -= n;
        *len = n;
        return 1;
    }
    for (int k = 0; k < RT_MAX_STREAMS; k++) {
        int sid = (c->rr + k) % RT_MAX_STREAMS;
        struct sstream *s = &c->strm[sid];
        if (!s->sq_len && !(s->fin_pending && !s->fin_sent)) continue;
        uint32_t room = (uint32_t)c->cfg.mss - 7, n = s->sq_len < room ? s->sq_len : room;
        if (avail < (int64_t)(7 + n) || c->seg_cnt >= SEG_MAX) return -1;
        int fin = s->fin_pending && n == s->sq_len;
        tmp[0] = (uint8_t)(sid >> 8); tmp[1] = (uint8_t)sid; put32(tmp + 2, s->snd_off); tmp[6] = (uint8_t)fin;
        if (n) ring_out(s->sq, STREAM_BUF, s->sq_head, tmp + 7, n);
        s->sq_head = (s->sq_head + n) % STREAM_BUF;
        s->sq_len -= n;
        s->snd_off += n;
        if (fin) s->fin_sent = 1;
        c->rr = (sid + 1) % RT_MAX_STREAMS;
        *len = 7 + n;
        return 1;
    }
    return 0;
}

/* ---- forward error correction (sender): one XOR parity packet per group of N consecutive data segments.
 * Parity packets use no sequence space and are never retransmitted; they only let the receiver rebuild ONE lost
 * segment of a group instantly instead of waiting a full round trip for a retransmission. */
static void fec_flush(rt_conn *c) {
    if (!c->fec_n) return;
    uint8_t pl[1 + 2 * FEC_MAX_K + RT_MSS_MAX];
    size_t o = 0;
    pl[o++] = (uint8_t)c->fec_n;
    for (int i = 0; i < c->fec_n; i++) { pl[o++] = c->fec_lens[i] >> 8; pl[o++] = (uint8_t)c->fec_lens[i]; }
    memcpy(pl + o, c->fec_acc, c->fec_max);
    o += c->fec_max;
    emit(c, F_ACK | F_FEC, c->fec_seq0, pl, (uint16_t)o, NULL, 0);
    c->st.fec_sent++;
    c->fec_n = 0;
}

static void fec_adapt(rt_conn *c) {      /* choose the group size from the loss measured over the last 128 segments */
    /* Note: the loss we can observe is what FEC did NOT repair, so it under-estimates the real loss. The thresholds
     * and the on/off hysteresis below are chosen so that the setting settles instead of flapping. */
    double p = (double)(c->st.retransmissions - c->win_retx0) / (double)c->win_sent;
    c->loss_ewma = c->loss_ewma < 0 ? p : 0.5 * c->loss_ewma + 0.5 * p;
    double e = c->loss_ewma;
    int k;
    if (c->fec_k == 0) k = e >= 0.004 ? 8 : 0;                 /* switch on only when loss is clearly there ... */
    else if (e < 0.0015) k = 0;                                /* ... and off only when it has clearly gone */
    else k = e < 0.008 ? 12 : e < 0.02 ? 8 : 4;
    if (k != c->fec_k) { fec_flush(c); c->fec_k = k; }
    c->win_sent = 0;
    c->win_retx0 = c->st.retransmissions;
}

static void fec_after_send(rt_conn *c, const struct seg *s) {
    c->win_sent++;
    if (c->fec_k > 0 && s->len) {
        if (c->fec_n == 0) { c->fec_seq0 = s->seq; memset(c->fec_acc, 0, sizeof c->fec_acc); c->fec_max = 0; }
        for (uint16_t i = 0; i < s->len; i++) c->fec_acc[i] ^= s->data[i];
        if (s->len > c->fec_max) c->fec_max = s->len;
        c->fec_lens[c->fec_n++] = s->len;
        if (c->fec_n >= c->fec_k) fec_flush(c);
    }
    if (c->cfg.fec_auto && c->win_sent >= 128) fec_adapt(c);
}

static void pump(rt_conn *c) {
    if (c->state != ST_ESTABLISHED && c->state != ST_CLOSE_WAIT) return;
    double now = rt_now();
    for (;;) {
        uint32_t flight = c->snd_nxt - c->snd_una;
        uint32_t w = rt_cc_window(&c->cc);
        if (c->rwnd < w) w = c->rwnd;
        int64_t avail = (int64_t)w - (int64_t)flight;
        uint8_t tmp[RT_MSS_MAX];
        uint32_t n = 0;
        int r = next_payload(c, avail, tmp, &n);
        if (r > 0) {
            struct seg *s = seg_push(c, c->snd_nxt, tmp, (uint16_t)n, 0);
            if (c->hist_n == c->hist_cap) {
                c->hist_cap = c->hist_cap ? c->hist_cap * 2 : 1024;
                c->hist = realloc(c->hist, c->hist_cap);
            }
            s->idx = c->hist_n++;
            c->hist[s->idx] = 0;
            c->snd_nxt = s->end;
            c->st.bytes_sent += n;
            transmit(c, s);
            if (c->rto_deadline == 0) arm_rto(c, now);
            fec_after_send(c, s);
        } else if (r < 0) break;
        else if (c->fin_pending && !c->fin_sent && c->seg_cnt < SEG_MAX) {
            fec_flush(c);
            struct seg *s = seg_push(c, c->snd_nxt, NULL, 0, F_FIN);
            c->fin_seq = s->seq;
            c->snd_nxt = s->end;
            c->fin_sent = 1;
            set_state(c, c->state == ST_ESTABLISHED ? ST_FIN_WAIT_1 : ST_LAST_ACK);
            transmit(c, s);
            if (c->rto_deadline == 0) arm_rto(c, now);
            break;
        } else break;
    }
    if (c->fec_n && !send_pending(c)) fec_flush(c);       /* protect the tail of a burst */
}

static void update_rtt(rt_conn *c, double r) {
    if (!c->have_srtt) { c->srtt = r; c->rttvar = r / 2; c->have_srtt = 1; }
    else {
        c->rttvar = 0.75 * c->rttvar + 0.25 * fabs(c->srtt - r);
        c->srtt = 0.875 * c->srtt + 0.125 * r;
    }
    double v = c->srtt + fmax(0.005, 4 * c->rttvar);
    c->base_rto = fmin(c->cfg.max_rto, fmax(c->cfg.min_rto, v));
    c->rto = c->base_rto;
}

/* RACK: remember the send time of the most recently *sent* segment that has been delivered.
 * A retransmission acknowledged faster than the minimum RTT cannot be what was delivered: the
 * original arrived late (reordering). That is a spurious retransmit, so widen the reordering window. */
static void rack_update(rt_conn *c, const struct seg *s, double now) {
    double rtt = now - s->sent;
    if (s->retx > 0 && c->min_rtt > 0 && rtt < c->min_rtt) {
        c->st.spurious++;
        if (c->reo_mult < 16) c->reo_mult *= 2;
        return;
    }
    if (s->sent > c->rack_sent) { c->rack_sent = s->sent; c->rack_rtt = rtt; }
}

static void process_sack(rt_conn *c, const rt_pkt *p, double now) {
    for (int b = 0; b < p->nsack; b++) {
        uint32_t s = p->sack[b][0], e = p->sack[b][1];
        for (uint32_t i = 0; i < c->seg_cnt; i++) {
            struct seg *g = seg_at(c, i);
            if (SEQ_GEQ(g->seq, e)) break;
            if (SEQ_GEQ(g->seq, s) && SEQ_LEQ(g->end, e) && !g->sacked) { g->sacked = 1; rack_update(c, g, now); }
        }
        if (!c->have_sacked || SEQ_GT(e, c->highest_sacked)) { c->highest_sacked = e; c->have_sacked = 1; }
    }
}

static void sack_retransmit(rt_conn *c, double now) {
    (void)now;
    int n = 0;
    for (uint32_t i = 0; i < c->seg_cnt && n < 2; i++) {
        struct seg *g = seg_at(c, i);
        if (!c->have_sacked || SEQ_GEQ(g->seq, c->highest_sacked)) break;
        if (!g->sacked && g->rec != c->epoch) { retransmit(c, g); n++; }
    }
}

static void enter_recovery(rt_conn *c, double now) {
    c->epoch++;
    c->in_recovery = 1;
    c->recover = c->snd_nxt;
    rt_cc_on_fast_retransmit(&c->cc, (double)(c->snd_nxt - c->snd_una), now);
    c->st.fast_retransmits++;
    add_event(c, now, 0);
    logc(c, "fast retransmit seq=%u cwnd=%.0f ssthresh=%.0f", c->snd_una, c->cc.cwnd, c->cc.ssthresh);
}

static void fast_retransmit(rt_conn *c, double now) {
    enter_recovery(c, now);
    retransmit(c, seg_at(c, 0));
    if (!c->cfg.rack) sack_retransmit(c, now);
    record(c, now, 1);
}

/* RACK (RFC 8985, simplified): a segment is lost once a segment sent LATER has been delivered and
 * an RTT plus a reordering window has elapsed without it being acknowledged. Unlike "3 duplicate
 * ACKs" this works for small windows and also catches lost *retransmissions*, which otherwise
 * can only be repaired by the (much longer) retransmission timeout. */
static void rack_detect(rt_conn *c, double now) {
    c->rack_deadline = 0;
    if (!c->cfg.rack || c->rack_sent == 0 || !c->seg_cnt) return;
    double reo = (c->min_rtt / 4 > 0.001 ? c->min_rtt / 4 : 0.001) * c->reo_mult;
    double cap = c->have_srtt && c->srtt > 0.001 ? c->srtt : 0.001;
    if (reo > cap) reo = cap;
    double rtt = c->rack_rtt > 0 ? c->rack_rtt : (c->have_srtt ? c->srtt : 0.001);
    struct seg *lost[4]; int nl = 0; double next = 0;
    for (uint32_t i = 0; i < c->seg_cnt; i++) {
        struct seg *g = seg_at(c, i);
        if (g->sacked || g->sent >= c->rack_sent) continue;
        double due = g->sent + rtt + reo;
        if (now >= due) { if (nl < 4) lost[nl++] = g; }
        else if (next == 0 || due < next) next = due;
    }
    c->rack_deadline = next;
    if (!nl) return;
    if (!c->in_recovery && SEQ_GEQ(c->snd_una, c->recover)) enter_recovery(c, now);
    for (int i = 0; i < nl; i++) { retransmit(c, lost[i]); c->st.rack_retx++; }
    record(c, now, 1);
}

static void on_ack(rt_conn *c, const rt_pkt *p, double now, uint32_t prev_w) {
    uint32_t ack = p->ack;
    if (SEQ_GT(ack, c->snd_nxt)) return;
    if (p->nsack) process_sack(c, p, now);
    if (SEQ_GT(ack, c->snd_una)) {
        uint32_t acked = ack - c->snd_una;
        int have_rtt = 0;
        double rtt = 0;
        while (c->seg_cnt) {
            struct seg *s = seg_at(c, 0);
            if (SEQ_GT(s->end, ack)) break;
            if (s->retx == 0) {                                      /* Karn's algorithm */
                have_rtt = 1; rtt = now - s->sent;
                if (c->min_rtt == 0 || rtt < c->min_rtt) c->min_rtt = rtt;
            }
            rack_update(c, s, now);
            if (s->idx != 0xFFFFFFFFu) c->hist[s->idx] = s->retx > 255 ? 255 : (uint8_t)s->retx;
            seg_pop(c);
        }
        c->snd_una = ack;
        c->retries = 0;
        c->dupacks = 0;
        c->rto = c->base_rto;                /* forward progress ends exponential backoff */
        if (have_rtt) update_rtt(c, rtt);
        if (c->state == ST_SYN_SENT && SEQ_GT(ack, c->local_isn)) established(c, now);
        else if (c->state == ST_SYN_RCVD && SEQ_GT(ack, c->local_isn)) established(c, now);
        else if (c->state == ST_ESTABLISHED || c->state == ST_CLOSE_WAIT || c->state == ST_FIN_WAIT_1 ||
                 c->state == ST_FIN_WAIT_2 || c->state == ST_CLOSING || c->state == ST_LAST_ACK) {
            if (c->in_recovery) {
                if (SEQ_GEQ(ack, c->recover)) {
                    c->in_recovery = 0;
                    rt_cc_on_recovery_exit(&c->cc);
                    logc(c, "fast recovery done");
                } else {                     /* partial ACK: next hole is lost too */
                    if (c->seg_cnt && seg_at(c, 0)->rec != c->epoch) retransmit(c, seg_at(c, 0));
                    rt_cc_on_partial_ack(&c->cc, acked);
                }
            } else {
                if (SEQ_LT(ack, c->recover) && c->seg_cnt && seg_at(c, 0)->rec != c->epoch)
                    retransmit(c, seg_at(c, 0));          /* recovering after RTO */
                rt_cc_on_ack(&c->cc, acked, have_rtt, rtt, now);
            }
        }
        if (c->seg_cnt) arm_rto(c, now); else c->rto_deadline = c->tlp_deadline = 0;
        check_fin_acked(c, now);
        c->persist_deadline = 0;
        c->persist_n = 0;
        record(c, now, 0);
    } else if (ack == c->snd_una && p->plen == 0 && !(p->flags & F_FIN) && c->seg_cnt &&
               (p->nsack || p->window <= prev_w)) {
        c->dupacks++;
        c->st.dup_acks++;
        if (c->in_recovery) {
            rt_cc_on_dup_ack(&c->cc);
            if (p->nsack && !c->cfg.rack) sack_retransmit(c, now);
        } else if (c->dupacks >= c->cfg.dupack_thresh) {
            if (SEQ_GEQ(c->snd_una, c->recover)) fast_retransmit(c, now);
            else if (p->nsack && !c->cfg.rack) sack_retransmit(c, now);
        }
        record(c, now, 0);
    }
    rack_detect(c, now);
}

/* ====================================================== receiver */
static void rq_put(rt_conn *c, const uint8_t *d, uint32_t n) {
    ring_in(c->rq, c->cfg.recv_buf, c->rq_head + c->rq_len, d, n);
    c->rq_len += n;
}

static void fin_received(rt_conn *c, double now) {
    switch (c->state) {
    case ST_ESTABLISHED: set_state(c, ST_CLOSE_WAIT); break;
    case ST_FIN_WAIT_1: set_state(c, ST_CLOSING); break;
    case ST_FIN_WAIT_2: enter_time_wait(c, now); break;
    default: break;
    }
}

/* ---- multi-stream receive: frames are delivered to their stream the moment they arrive, even if earlier
 * packets of OTHER streams are still missing. Returns 0 if the frame cannot be buffered (then it is not ACKed). */
static int stream_alloc(struct sstream *s) {
    if (!s->sq) {
        s->sq = malloc(STREAM_BUF); s->rb = malloc(STREAM_BUF); s->pend = calloc(STREAM_PEND, sizeof(struct pend));
        if (!s->sq || !s->rb || !s->pend) return 0;
    }
    return 1;
}

static int stream_deliver(rt_conn *c, const rt_pkt *p) {
    if (p->plen < 7) return 1;
    unsigned sid = (p->payload[0] << 8) | p->payload[1];
    if (sid >= RT_MAX_STREAMS) return 1;
    struct sstream *s = &c->strm[sid];
    if (!stream_alloc(s)) return 0;
    uint32_t off = get32(p->payload + 2), n = p->plen - 7;
    int fin = p->payload[6] & 1;
    const uint8_t *d = p->payload + 7;
    if (fin) { s->fin_known = 1; s->fin_off = off + n; }
    if (n == 0 || SEQ_LEQ(off + n, s->rcv_off)) return 1;                    /* nothing new / duplicate */
    if (SEQ_LT(off, s->rcv_off)) { uint32_t cut = s->rcv_off - off; d += cut; n -= cut; off = s->rcv_off; }
    if (off == s->rcv_off) {
        if (s->rb_len + n > STREAM_BUF) return 0;
        ring_in(s->rb, STREAM_BUF, s->rb_head + s->rb_len, d, n);
        s->rb_len += n; c->stream_buffered += n; s->rcv_off += n;
        for (;;) {                                                            /* absorb frames that are now contiguous */
            int idx = -1;
            for (int i = 0; i < STREAM_PEND; i++) if (s->pend[i].used && SEQ_LEQ(s->pend[i].off, s->rcv_off)) { idx = i; break; }
            if (idx < 0) break;
            struct pend *q = &s->pend[idx];
            q->used = 0; c->stream_buffered -= q->len;
            if (SEQ_GT(q->off + q->len, s->rcv_off)) {
                uint32_t cut = s->rcv_off - q->off, m = q->len - cut;
                if (s->rb_len + m <= STREAM_BUF) {
                    ring_in(s->rb, STREAM_BUF, s->rb_head + s->rb_len, q->data + cut, m);
                    s->rb_len += m; c->stream_buffered += m; s->rcv_off += m;
                }
            }
        }
        return 1;
    }
    int slot = -1;
    for (int i = 0; i < STREAM_PEND; i++) {
        if (s->pend[i].used && s->pend[i].off == off) return 1;               /* already stashed */
        if (!s->pend[i].used && slot < 0) slot = i;
    }
    if (slot < 0) return 0;
    s->pend[slot].off = off; s->pend[slot].len = (uint16_t)n; s->pend[slot].used = 1;
    memcpy(s->pend[slot].data, d, n);
    c->stream_buffered += n;
    return 1;
}

/* ---- FEC receiver: remember recent segments, and when a parity packet arrives with exactly ONE member of its
 * group missing, rebuild that member by XOR and process it as if it had arrived normally. */
static void fh_put(rt_conn *c, uint32_t seq, const uint8_t *d, uint16_t len) {
    if (!c->fh || !len) return;
    struct fhe *e = &c->fh[(seq * 2654435761u) >> 23];
    e->seq = seq; e->len = len; e->used = 1;
    memcpy(e->data, d, len);
}
static struct fhe *fh_get(rt_conn *c, uint32_t seq, uint16_t len) {
    struct fhe *e = &c->fh[(seq * 2654435761u) >> 23];
    return e->used && e->seq == seq && e->len == len ? e : NULL;
}

static void on_data(rt_conn *c, const rt_pkt *p, double now);

static void fec_receive(rt_conn *c, const rt_pkt *p, double now) {
    if (!c->fh) { c->fh = calloc(FH_SLOTS, sizeof(struct fhe)); return; }      /* start recording from now on */
    if (p->plen < 1) return;
    int k = p->payload[0];
    if (k < 1 || k > FEC_MAX_K || p->plen < 1 + 2 * k) return;
    uint16_t lens[FEC_MAX_K], maxl = 0;
    uint32_t seqs[FEC_MAX_K], sq = p->seq;
    for (int i = 0; i < k; i++) {
        lens[i] = (uint16_t)((p->payload[1 + 2 * i] << 8) | p->payload[2 + 2 * i]);
        seqs[i] = sq; sq += lens[i];
        if (lens[i] > maxl) maxl = lens[i];
    }
    if (p->plen != 1 + 2 * k + maxl) return;
    int miss = -1, nmiss = 0;
    for (int i = 0; i < k && nmiss < 2; i++) {
        if (fh_get(c, seqs[i], lens[i])) continue;
        int inooo = 0;
        for (int j = 0; j < OOO_MAX; j++) if (c->ooo[j].used && c->ooo[j].seq == seqs[i]) { inooo = 1; break; }
        if (inooo || SEQ_LEQ(seqs[i] + lens[i], c->rcv_nxt)) { nmiss = 2; break; }   /* have it, but not its bytes: cannot XOR */
        nmiss++; miss = i;
    }
    if (nmiss != 1) return;
    uint8_t buf[RT_MSS_MAX];
    memcpy(buf, p->payload + 1 + 2 * k, maxl);
    for (int i = 0; i < k; i++) {
        if (i == miss) continue;
        struct fhe *e = fh_get(c, seqs[i], lens[i]);
        for (uint16_t b = 0; b < lens[i]; b++) buf[b] ^= e->data[b];
    }
    rt_pkt q = {.conn_id = c->id, .seq = seqs[miss], .flags = F_ACK, .plen = lens[miss], .payload = buf};
    c->st.fec_recovered++;
    on_data(c, &q, now);
}

static void accept_data(rt_conn *c, const uint8_t *d, uint32_t n, int fin, double now) {
    if (!c->streams) rq_put(c, d, n);
    c->rcv_nxt += n;
    c->st.bytes_received += n;
    if (fin) {
        c->rcv_nxt += 1;
        c->peer_fin = 1;
        fin_received(c, now);
    }
}

static void on_data(rt_conn *c, const rt_pkt *p, double now) {
    uint32_t seq = p->seq, dlen = p->plen;
    const uint8_t *data = p->payload;
    int fin = (p->flags & F_FIN) != 0;
    uint32_t length = dlen + fin;
    if (c->fh && p->plen) fh_put(c, p->seq, p->payload, p->plen);
    if (c->streams && p->plen && !stream_deliver(c, p)) { c->st.window_drops++; return; }   /* no room: do not ACK, sender will retry */
    if (SEQ_LEQ(seq + length, c->rcv_nxt)) c->st.dup_packets++;
    else {
        if (SEQ_LT(seq, c->rcv_nxt)) {               /* partial overlap: trim */
            uint32_t cut = c->rcv_nxt - seq;
            if (cut >= dlen) { data += dlen; dlen = 0; } else { data += cut; dlen -= cut; }
            seq = c->rcv_nxt;
        }
        uint32_t used = c->rq_len + c->ooo_bytes + c->stream_buffered;
        uint32_t fr = used >= c->cfg.recv_buf ? 0 : c->cfg.recv_buf - used;
        if (!c->streams && dlen > fr) c->st.window_drops++;                 /* sender overran our window */
        else if (seq == c->rcv_nxt) {
            accept_data(c, data, dlen, fin, now);
            for (;;) {                                       /* drain contiguous buffered ranges */
                int idx = -1;
                for (int i = 0; i < OOO_MAX; i++)
                    if (c->ooo[i].used && SEQ_LEQ(c->ooo[i].seq, c->rcv_nxt)) { idx = i; break; }
                if (idx < 0) break;
                struct ooo *o = &c->ooo[idx];
                uint32_t end = o->seq + o->len + o->fin;
                uint32_t olen = o->len;
                c->ooo_bytes -= c->streams ? 0 : olen;
                c->ooo_cnt--;
                o->used = 0;
                if (SEQ_GT(end, c->rcv_nxt)) {
                    uint32_t off = c->rcv_nxt - o->seq;
                    uint32_t rem = off >= olen ? 0 : olen - off;
                    accept_data(c, o->data + (off >= olen ? olen : off), rem, o->fin, now);
                }
            }
        } else if (seq - c->rcv_nxt + dlen <= c->cfg.recv_buf) {
            int dup = 0, slot = -1;
            for (int i = 0; i < OOO_MAX; i++) {
                if (c->ooo[i].used && c->ooo[i].seq == seq) { dup = 1; break; }
                if (!c->ooo[i].used && slot < 0) slot = i;
            }
            if (dup) c->st.dup_packets++;
            else if (slot >= 0) {
                struct ooo *o = &c->ooo[slot];
                o->seq = seq; o->len = (uint16_t)dlen; o->fin = (uint8_t)fin; o->used = 1;
                memcpy(o->data, data, dlen);
                c->ooo_cnt++;
                c->ooo_bytes += c->streams ? 0 : dlen;
                c->last_ooo = seq;
                c->have_last_ooo = 1;
                c->st.ooo_packets++;
            }
        }
    }
    send_ack(c);
}

/* ====================================================== packet input */
static void handle_rst(rt_conn *c, const rt_pkt *p) {
    int valid;
    if (c->state == ST_SYN_SENT) valid = (p->flags & F_ACK) && p->ack == c->snd_nxt;
    else valid = SEQ_GEQ(p->seq, c->rcv_nxt) && SEQ_LEQ(p->seq, c->rcv_nxt + c->cfg.recv_buf);
    if (valid) {
        logc(c, "valid RST -> CLOSED");
        int benign = c->state == ST_LAST_ACK || c->state == ST_TIME_WAIT || c->state == ST_CLOSING;
        close_now(c, benign ? 0 : ECONNRESET);
    }
}

static void conn_on_packet(rt_conn *c, const rt_pkt *p) {
    double now = rt_now();
    c->st.packets_received++;
    if (c->cfg.verbose && (p->flags & (F_SYN | F_FIN | F_RST)))
        logc(c, "RX flags=0x%02x seq=%u ack=%u len=%u", p->flags, p->seq, p->ack, p->plen);
    rt_state st = c->state;
    if (st == ST_CLOSED) return;
    if (p->flags & F_RST) { handle_rst(c, p); return; }
    if (st == ST_SYN_SENT) {
        if ((p->flags & F_SYN) && (p->flags & F_ACK) && p->ack == c->snd_nxt) {
            c->peer_isn = p->seq;
            c->rcv_nxt = p->seq + 1;
            c->rwnd = p->window;
            c->streams = (p->flags & F_STREAMS) != 0;
            on_ack(c, p, now, 0);
            send_ack(c);
            pump(c);
        }
        return;
    }
    if (p->flags & F_SYN) {
        if (st == ST_SYN_RCVD && !(p->flags & F_ACK)) { if (c->seg_cnt) retransmit(c, seg_at(c, 0)); }
        else send_ack(c);                              /* duplicate SYN-ACK: re-ACK */
        return;
    }
    uint32_t prev_w = c->rwnd;
    if (p->flags & F_ACK) {
        if (SEQ_GEQ(p->ack, c->snd_una)) c->rwnd = p->window;
        on_ack(c, p, now, prev_w);
        if (c->state == ST_CLOSED) return;
    }
    if (p->flags & F_PROBE) send_ack(c);
    if (p->flags & F_FEC) fec_receive(c, p, now);
    else if (p->plen || (p->flags & F_FIN)) on_data(c, p, now);
    pump(c);
}

/* ====================================================== timers */
static void on_timeout(rt_conn *c, double now) {
    c->retries++;
    if (c->retries > c->cfg.max_retries) {
        logc(c, "too many retransmissions, aborting");
        abort_conn(c, ETIMEDOUT);
        return;
    }
    c->st.timeouts++;
    struct seg *s = seg_at(c, 0);
    if (c->state != ST_SYN_SENT && c->state != ST_SYN_RCVD) {
        c->epoch++;
        rt_cc_on_timeout(&c->cc, (double)(c->snd_nxt - c->snd_una), now);
        c->in_recovery = 0;
        c->dupacks = 0;
        c->recover = c->snd_nxt;
        add_event(c, now, 1);
        logc(c, "RTO seq=%u rto=%.3fs cwnd->%.0f ssthresh=%.0f", s->seq, c->rto, c->cc.cwnd, c->cc.ssthresh);
    } else logc(c, "handshake timeout, retrying (rto=%.2fs)", c->rto);
    c->rto = fmin(c->cfg.max_rto, c->rto * 2);           /* exponential backoff */
    retransmit(c, s);
    arm_rto(c, now);
    record(c, now, 1);
}

static void conn_tick(rt_conn *c, double now) {
    rt_state st = c->state;
    if (st == ST_CLOSED) return;
    if (st == ST_TIME_WAIT) {
        if (now >= c->tw_deadline) close_now(c, 0);
        return;
    }
    if (c->rto_deadline > 0 && now >= c->rto_deadline && c->seg_cnt) on_timeout(c, now);
    if (c->state == ST_CLOSED) return;
    if (c->rack_deadline > 0 && now >= c->rack_deadline && c->seg_cnt) rack_detect(c, now);
    if (c->tlp_deadline > 0 && now >= c->tlp_deadline && c->seg_cnt && !c->in_recovery) {
        c->tlp_deadline = 0;                              /* tail loss probe: resend the last segment */
        struct seg *last = seg_at(c, c->seg_cnt - 1);
        logc(c, "tail-loss probe seq=%u", last->seq);
        retransmit(c, last);
        c->st.tlp_probes++;
        add_event(c, now, 2);
        c->rto_deadline = now + c->rto;
    }
    st = c->state;
    if (send_pending(c) && !c->seg_cnt && (st == ST_ESTABLISHED || st == ST_CLOSE_WAIT) &&
        c->rwnd < (c->streams || c->sq_len > (uint32_t)c->cfg.mss ? (uint32_t)c->cfg.mss : c->sq_len)) {
        if (c->persist_deadline == 0) c->persist_deadline = now + c->rto;       /* zero-window probing */
        else if (now >= c->persist_deadline) {
            c->st.window_probes++;
            emit(c, F_ACK | F_PROBE, c->snd_nxt, NULL, 0, NULL, 0);
            c->persist_n++;
            double mult = (double)(1 << (c->persist_n < 4 ? c->persist_n : 4));
            c->persist_deadline = now + fmin(c->cfg.max_rto, c->rto * mult);
        }
    }
    if (send_pending(c)) pump(c);
}

static double conn_deadline(rt_conn *c) {
    double d = 0;
    double cands[5] = {c->rto_deadline, c->tw_deadline, c->persist_deadline, c->tlp_deadline, c->rack_deadline};
    for (int i = 0; i < 5; i++)
        if (cands[i] > 0 && (d == 0 || cands[i] < d)) d = cands[i];
    return d;
}

/* ====================================================== connection lifecycle */
static rt_conn *conn_new(rt_ep *ep, const struct sockaddr_in *peer, uint32_t id) {
    int slot = -1;
    for (int i = 0; i < MAX_CONNS; i++) if (!ep->conns[i]) { slot = i; break; }
    if (slot < 0) return NULL;
    rt_conn *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->ep = ep; c->peer = *peer; c->id = id; c->cfg = ep->cfg; c->in_use = 1;
    c->state = ST_CLOSED;
    c->local_isn = 1 + (uint32_t)(((uint64_t)random() << 8 ^ random()) % ((1u << 30) - 1));
    c->snd_una = c->snd_nxt = c->local_isn;
    c->sq = malloc(c->cfg.send_buf);
    c->rq = malloc(c->cfg.recv_buf);
    c->segs = calloc(SEG_MAX, sizeof(struct seg));
    c->ooo = calloc(OOO_MAX, sizeof(struct ooo));
    if (!c->sq || !c->rq || !c->segs || !c->ooo) { rt_conn_free(c); return NULL; }
    rt_cc_init(&c->cc, c->cfg.cc, c->cfg.mss, c->cfg.init_cwnd_mss, (double)c->cfg.init_ssthresh_mss * c->cfg.mss);
    c->rwnd = c->cfg.recv_buf;
    c->rto = c->base_rto = c->cfg.init_rto;
    c->reo_mult = 1;
    c->loss_ewma = -1;
    c->fec_k = c->cfg.fec < 0 ? 0 : (c->cfg.fec > FEC_MAX_K ? FEC_MAX_K : c->cfg.fec);
    if (c->cfg.fec_auto && !c->fec_k) c->fec_k = 8;          /* cheap insurance until the first loss measurement */
    if ((c->cfg.fec || c->cfg.fec_auto) && c->cfg.rack) c->cfg.dupack_thresh = 1000000;   /* give parity time to arrive: RACK (time-based) detects loss instead */
    c->t0 = rt_now();
    ep->conns[slot] = c;
    return c;
}

static void ep_remove_conn_notify(rt_conn *c) { (void)c; /* kept in table for stats until rt_conn_free */ }

void rt_conn_free(rt_conn *c) {
    if (!c) return;
    rt_ep *ep = c->ep;
    for (int i = 0; i < MAX_CONNS; i++) if (ep->conns[i] == c) ep->conns[i] = NULL;
    for (int i = 0; i < ep->acc_n; i++)
        if (ep->accq[i] == c) { memmove(&ep->accq[i], &ep->accq[i + 1], (ep->acc_n - i - 1) * sizeof(rt_conn *)); ep->acc_n--; break; }
    free(c->sq); free(c->rq); free(c->segs); free(c->ooo); free(c->trace); free(c->ev); free(c->hist); free(c->fh);
    for (int i = 0; i < RT_MAX_STREAMS; i++) { free(c->strm[i].sq); free(c->strm[i].rb); free(c->strm[i].pend); }
    free(c);
}

rt_ep *rt_ep_new(const char *bind_ip, int port, const rt_cfg *cfg) {
    static int seeded;
    if (!seeded) { srandom((unsigned)(rt_now() * 1e6) ^ (unsigned)getpid()); seeded = 1; }
    rt_ep *ep = calloc(1, sizeof *ep);
    if (!ep) return NULL;
    ep->fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (ep->fd < 0) { free(ep); return NULL; }
    int sz = 4 << 20;
    setsockopt(ep->fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
    setsockopt(ep->fd, SOL_SOCKET, SO_SNDBUF, &sz, sizeof sz);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
    a.sin_addr.s_addr = (bind_ip && *bind_ip) ? inet_addr(bind_ip) : htonl(INADDR_ANY);
    if (bind(ep->fd, (struct sockaddr *)&a, sizeof a) < 0) { close(ep->fd); free(ep); return NULL; }
    fcntl(ep->fd, F_SETFL, O_NONBLOCK);
    if (cfg) ep->cfg = *cfg; else rt_cfg_default(&ep->cfg);
    if (ep->cfg.mss > RT_MSS_MAX) ep->cfg.mss = RT_MSS_MAX;
    return ep;
}

void rt_ep_free(rt_ep *ep) {
    if (!ep) return;
    for (int i = 0; i < MAX_CONNS; i++) if (ep->conns[i]) rt_conn_free(ep->conns[i]);
    close(ep->fd);
    free(ep);
}

int rt_ep_port(rt_ep *ep) {
    struct sockaddr_in a; socklen_t l = sizeof a;
    getsockname(ep->fd, (struct sockaddr *)&a, &l);
    return ntohs(a.sin_port);
}

void rt_listen(rt_ep *ep) { ep->listening = 1; }

rt_conn *rt_accept(rt_ep *ep) {
    if (!ep->acc_n) return NULL;
    rt_conn *c = ep->accq[0];
    memmove(&ep->accq[0], &ep->accq[1], (--ep->acc_n) * sizeof(rt_conn *));
    return c;
}

rt_conn *rt_connect(rt_ep *ep, const struct sockaddr_in *addr) {
    uint32_t id = 1 + (uint32_t)(((uint64_t)random() << 8 ^ random()) % 0xFFFFFFFEu);
    rt_conn *c = conn_new(ep, addr, id);
    if (!c) return NULL;
    set_state(c, ST_SYN_SENT);
    struct seg *s = seg_push(c, c->snd_nxt, NULL, 0, F_SYN | (c->cfg.streams ? F_STREAMS : 0));
    c->snd_nxt = s->end;
    transmit(c, s);
    arm_rto(c, rt_now());
    return c;
}

static void passive_open(rt_conn *c, const rt_pkt *p) {
    c->passive = 1;
    c->peer_isn = p->seq;
    c->rcv_nxt = p->seq + 1;
    c->rwnd = p->window;
    set_state(c, ST_SYN_RCVD);
    c->streams = (p->flags & F_STREAMS) != 0;
    struct seg *s = seg_push(c, c->snd_nxt, NULL, 0, F_SYN | F_ACK | (c->streams ? F_STREAMS : 0));
    c->snd_nxt = s->end;
    transmit(c, s);
    arm_rto(c, rt_now());
}

static void rst_for(rt_ep *ep, const rt_pkt *p, const struct sockaddr_in *from) {
    if (p->flags & F_RST) return;
    rt_pkt r = {.conn_id = p->conn_id};
    if (p->flags & F_ACK) { r.seq = p->ack; r.flags = F_RST; }
    else { r.seq = 0; r.ack = p->seq + p->plen + ((p->flags & (F_SYN | F_FIN)) ? 1 : 0); r.flags = F_RST | F_ACK; }
    uint8_t buf[64];
    size_t n = rt_pkt_encode(&r, buf);
    sendto(ep->fd, buf, n, 0, (const struct sockaddr *)from, sizeof *from);
}

static void ep_dispatch(rt_ep *ep, const uint8_t *buf, size_t n, const struct sockaddr_in *from) {
    rt_pkt p;
    if (rt_pkt_decode(buf, n, &p) < 0) { ep->bad_packets++; return; }   /* dropped before any state change */
    rt_conn *c = NULL;
    for (int i = 0; i < MAX_CONNS; i++) {
        rt_conn *x = ep->conns[i];
        if (x && x->id == p.conn_id && x->state != ST_CLOSED && x->peer.sin_port == from->sin_port &&
            x->peer.sin_addr.s_addr == from->sin_addr.s_addr) { c = x; break; }
    }
    if (!c) {
        if (ep->listening && (p.flags & F_SYN) && !(p.flags & F_ACK) && ep->acc_n < ACCEPTQ) {
            c = conn_new(ep, from, p.conn_id);
            if (c) passive_open(c, &p);
            else rst_for(ep, &p, from);
        } else rst_for(ep, &p, from);
        return;
    }
    conn_on_packet(c, &p);
}

int rt_run_once(rt_ep *ep, int timeout_ms) {
    double now = rt_now(), next = now + timeout_ms / 1000.0;
    for (int i = 0; i < MAX_CONNS; i++)
        if (ep->conns[i]) { double d = conn_deadline(ep->conns[i]); if (d > 0 && d < next) next = d; }
    int ms = (int)ceil((next - now) * 1000.0);
    if (ms < 0) ms = 0;
    if (ms > timeout_ms) ms = timeout_ms;
    struct pollfd pf = {.fd = ep->fd, .events = POLLIN};
    poll(&pf, 1, ms);
    int cnt = 0;
    for (int k = 0; k < 512; k++) {
        struct sockaddr_in from; socklen_t fl = sizeof from;
        ssize_t n = recvfrom(ep->fd, ep->rxbuf, sizeof ep->rxbuf, 0, (struct sockaddr *)&from, &fl);
        if (n < 0) break;
        ep_dispatch(ep, ep->rxbuf, (size_t)n, &from);
        cnt++;
    }
    now = rt_now();
    for (int i = 0; i < MAX_CONNS; i++) if (ep->conns[i]) conn_tick(ep->conns[i], now);
    return cnt;
}

/* ====================================================== application API */
int rt_send(rt_conn *c, const void *data, size_t len) {
    if (c->error || c->streams) return -1;
    if ((c->state != ST_ESTABLISHED && c->state != ST_CLOSE_WAIT) || c->fin_pending) return -1;
    uint32_t space = c->cfg.send_buf - c->sq_len;
    uint32_t n = len < space ? (uint32_t)len : space;
    const uint8_t *d = data;
    ring_in(c->sq, c->cfg.send_buf, c->sq_head + c->sq_len, d, n);
    c->sq_len += n;
    pump(c);
    return (int)n;
}

int rt_recv(rt_conn *c, void *buf, size_t cap) {
    if (c->rq_len) {
        uint32_t n = c->rq_len < cap ? c->rq_len : (uint32_t)cap;
        ring_out(c->rq, c->cfg.recv_buf, c->rq_head, buf, n);
        c->rq_head = (c->rq_head + n) % c->cfg.recv_buf;
        c->rq_len -= n;
        uint32_t cur = adv_window(c);
        if (c->state != ST_CLOSED && c->state != ST_SYN_SENT && c->state != ST_SYN_RCVD &&
            ((c->last_adv == 0 && cur > 0) || cur - c->last_adv >= c->cfg.recv_buf / 2))
            send_ack(c);                                 /* window update */
        return (int)n;
    }
    if (c->error) return -2;
    if (c->peer_fin || c->state == ST_CLOSED) return -1;
    return 0;
}

void rt_close(rt_conn *c) {
    if (c->state == ST_ESTABLISHED || c->state == ST_CLOSE_WAIT) { c->fin_pending = 1; pump(c); }
    else if (c->state == ST_SYN_SENT || c->state == ST_SYN_RCVD) abort_conn(c, ECONNABORTED);
}

int rt_flushed(rt_conn *c) { return !send_pending(c) && c->seg_cnt == 0; }
rt_state rt_conn_state(rt_conn *c) { return c->state; }
int rt_conn_error(rt_conn *c) { return c->error; }
uint32_t rt_conn_id(rt_conn *c) { return c->id; }
void rt_conn_peer(rt_conn *c, struct sockaddr_in *out) { *out = c->peer; }

void rt_stats_get(rt_conn *c, rt_stats *s) {
    *s = c->st;
    double end = c->t_closed > 0 ? c->t_closed : rt_now();
    double start = c->t_established > 0 ? c->t_established : c->t0;
    s->cwnd = (int)c->cc.cwnd;
    s->fec_k = c->fec_k;
    s->streams = c->streams;
    s->ssthresh = c->cc.ssthresh > 1e9 ? 1 << 30 : (int)c->cc.ssthresh;
    s->srtt_ms = c->have_srtt ? c->srtt * 1000 : 0;
    s->rttvar_ms = c->rttvar * 1000;
    s->rto_ms = c->rto * 1000;
    s->duration_s = end - start;
    s->cc = c->cfg.cc == CC_CUBIC ? "cubic" : "reno";
    s->state = STN[c->state];
    s->checksum_failures = c->ep->bad_packets;
}

int rt_trace_write(rt_conn *c, const char *path) {
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "t,cwnd,ssthresh,flight,srtt,rto,rwnd,phase\n");
    for (size_t i = 0; i < c->trace_n; i++) {
        trace_row *r = &c->trace[i];
        static const char *PH[] = {"slow_start", "congestion_avoidance", "fast_recovery"};
        fprintf(f, "%.6f,%.1f,%.1f,%.0f,%.6f,%.6f,%.0f,%s\n", r->t, r->cwnd, r->ssthresh, r->flight, r->srtt,
                r->rto, r->rwnd, PH[r->phase]);
    }
    fclose(f);
    return 0;
}

int rt_resolve(const char *hp, struct sockaddr_in *out) {
    char host[256];
    const char *colon = strrchr(hp, ':');
    if (!colon) return -1;
    size_t hl = (size_t)(colon - hp);
    if (hl >= sizeof host) return -1;
    memcpy(host, hp, hl); host[hl] = 0;
    if (!*host) strcpy(host, "127.0.0.1");
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_DGRAM}, *res;
    if (getaddrinfo(host, NULL, &hints, &res) != 0) return -1;
    *out = *(struct sockaddr_in *)res->ai_addr;
    out->sin_port = htons((uint16_t)atoi(colon + 1));
    freeaddrinfo(res);
    return 0;
}

size_t rt_trace_len(rt_conn *c) { return c->trace_n; }
const rt_trace_pt *rt_trace_data(rt_conn *c) { return c->trace; }
size_t rt_event_len(rt_conn *c) { return c->ev_n; }
const rt_event *rt_event_data(rt_conn *c) { return c->ev; }
uint64_t rt_bytes_acked(rt_conn *c) { uint32_t d = c->snd_una - c->local_isn; return d > 0 ? d - 1 : 0; }
size_t rt_segment_history(rt_conn *c, const uint8_t **out) { *out = c->hist; return c->hist_n; }

int rt_conn_is_streams(rt_conn *c) { return c->streams; }

int rt_stream_send(rt_conn *c, unsigned sid, const void *data, size_t len, int fin) {
    if (c->error || !c->streams || sid >= RT_MAX_STREAMS) return -1;
    if ((c->state != ST_ESTABLISHED && c->state != ST_CLOSE_WAIT) || c->fin_pending) return -1;
    struct sstream *s = &c->strm[sid];
    if (s->fin_pending || !stream_alloc(s)) return -1;
    uint32_t space = STREAM_BUF - s->sq_len, n = len < space ? (uint32_t)len : space;
    ring_in(s->sq, STREAM_BUF, s->sq_head + s->sq_len, data, n);
    s->sq_len += n;
    if (fin && n == len) s->fin_pending = 1;
    pump(c);
    return (int)n;
}

int rt_stream_recv(rt_conn *c, unsigned sid, void *buf, size_t cap) {
    if (!c->streams || sid >= RT_MAX_STREAMS) return -2;
    struct sstream *s = &c->strm[sid];
    if (s->rb_len) {
        uint32_t n = s->rb_len < cap ? s->rb_len : (uint32_t)cap;
        ring_out(s->rb, STREAM_BUF, s->rb_head, buf, n);
        s->rb_head = (s->rb_head + n) % STREAM_BUF;
        s->rb_len -= n;
        c->stream_buffered -= n;
        uint32_t cur = adv_window(c);
        if (c->state != ST_CLOSED && c->state != ST_SYN_SENT && c->state != ST_SYN_RCVD &&
            ((c->last_adv == 0 && cur > 0) || cur - c->last_adv >= c->cfg.recv_buf / 2))
            send_ack(c);                                 /* window update */
        return (int)n;
    }
    if (s->fin_known && s->rcv_off == s->fin_off) return -1;
    if (c->error) return -2;
    return 0;
}

size_t rt_held_bytes(rt_conn *c) {
    size_t n = 0;
    if (c->streams) {
        for (int i = 0; i < RT_MAX_STREAMS; i++)
            if (c->strm[i].pend) for (int k = 0; k < STREAM_PEND; k++) if (c->strm[i].pend[k].used) n += c->strm[i].pend[k].len;
    } else
        for (int i = 0; i < OOO_MAX; i++) if (c->ooo[i].used) n += c->ooo[i].len;
    return n;
}
