#ifndef HARNESS_H
#define HARNESS_H
#include "rtp.h"
typedef struct {
    double t, frac, mbps, cwnd, ssthresh, flight, srtt_ms;
    size_t acked, total;
    unsigned long long retx;
} xfer_progress;

typedef struct {
    size_t size; const imp_cfg *imp; rt_cfg cfg; uint64_t seed; double timeout; const char *trace_path;
    int slow_kbps;       /* receiver reads at most this many KB/s (0 = unlimited): exercises flow control */
    int capture;         /* keep trace + events in the result */
    void (*progress)(void *user, const xfer_progress *p);   /* called ~every 40 ms from the client loop */
    void *user;
} xfer_args;
typedef struct {
    int ok; double duration_s, mbps, handshake_ms; rt_stats cli, srv; char sha[65], want[65];
    int proxied; imp_dir_stats pf, pr; char err[96];
    rt_trace_pt *trace; size_t ntrace; rt_event *events; size_t nevents; uint8_t *seg_hist; size_t nseg;   /* when capture=1; free with xfer_res_free */
} xfer_res;

typedef struct { int flows; double mbps[8]; int ok[8]; double total_mbps, jain; } fair_res;

void make_data(uint8_t *buf, size_t n, uint64_t seed);
int run_transfer(const xfer_args *a, xfer_res *r);     /* in-process client/server (+proxy) on loopback */
void xfer_res_free(xfer_res *r);
int run_fairness(size_t size, const imp_cfg *imp, const rt_cfg *cfg, int flows, fair_res *out);
/* Head-of-line-blocking experiment: N logical streams sent either multiplexed into ONE ordered byte stream
 * (what TCP forces on you) or as independent streams of the multi-stream mode. */
typedef struct {
    int ok, multiplexed, nstreams; double duration_s, mbps;
    double completion_s[8];
    double avg_held_kb, peak_held_kb, blocked_frac;         /* data held back by head-of-line blocking (time-weighted) */
    int ntl; double tl_t[240]; double tl_b[8][240]; double tl_h[240];   /* per-stream delivered bytes, and held-back bytes, over time */
    rt_stats cli; char err[96];
} hol_res;
int run_hol(size_t per_stream, int nstreams, const imp_cfg *imp, const rt_cfg *cfg, int multiplexed, hol_res *out);
int tcp_transfer(size_t size, uint64_t seed, double *mbps);   /* native kernel TCP baseline */
#endif
