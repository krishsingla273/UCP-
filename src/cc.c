/* Congestion control: Reno (default) and CUBIC. Windows in bytes (doubles). */
#include "rtp.h"
#include <math.h>

void rt_cc_init(rt_cc *c, int kind, int mss, int init_cwnd_mss, double init_ssthresh) {
    *(c) = (rt_cc){0};
    c->kind = kind;
    c->mss = mss;
    c->cwnd = (double)init_cwnd_mss * mss;
    c->ssthresh = init_ssthresh > 0 ? init_ssthresh : 1e18;
    c->max_cwnd = 64.0 * 1024 * 1024;
    c->min_rtt = -1;
}

uint32_t rt_cc_window(const rt_cc *c) {
    double w = c->cwnd < c->max_cwnd ? c->cwnd : c->max_cwnd;
    return (uint32_t)(w < c->mss ? c->mss : w);
}

const char *rt_cc_phase(const rt_cc *c) {
    if (c->in_recovery) return "fast_recovery";
    return c->cwnd < c->ssthresh ? "slow_start" : "congestion_avoidance";
}

static void slow_start(rt_cc *c, double acked) { c->cwnd += acked < c->mss ? acked : c->mss; }

#define CUBIC_C 0.4
#define CUBIC_BETA 0.7

static void cubic_loss(rt_cc *c) {
    double cw = c->cwnd / c->mss;
    c->w_max = cw < c->w_max ? cw * (1 + CUBIC_BETA) / 2 : cw;   /* fast convergence */
    c->have_epoch = 0;
    c->ssthresh = fmax(c->cwnd * CUBIC_BETA, 2.0 * c->mss);
}

void rt_cc_on_ack(rt_cc *c, double acked, int have_rtt, double rtt, double now) {
    if (c->kind == CC_RENO) {
        if (c->cwnd < c->ssthresh) slow_start(c, acked);
        else c->cwnd += c->mss * acked / c->cwnd;
    } else {
        if (have_rtt && (c->min_rtt < 0 || rtt < c->min_rtt)) c->min_rtt = rtt;
        if (c->cwnd < c->ssthresh) slow_start(c, acked);
        else {
            double cw = c->cwnd / c->mss;
            if (!c->have_epoch) {
                c->have_epoch = 1;
                c->epoch_start = now;
                if (c->w_max <= cw) c->w_max = cw;
                c->k = cbrt(fmax(0.0, (c->w_max - cw) / CUBIC_C));
                c->w_tcp = cw;
            }
            double t = now - c->epoch_start + (c->min_rtt > 0 ? c->min_rtt : 0.0);
            double target = CUBIC_C * pow(t - c->k, 3) + c->w_max;
            double segs = acked / c->mss;
            if (target > cw) cw += (target - cw) / cw * segs;
            else cw += 0.01 / cw * segs;
            c->w_tcp += 3 * (1 - CUBIC_BETA) / (1 + CUBIC_BETA) * segs / fmax(cw, 1.0);
            if (c->w_tcp > cw) cw = c->w_tcp;            /* TCP-friendly region */
            c->cwnd = cw * c->mss;
        }
    }
    if (c->cwnd > c->max_cwnd) c->cwnd = c->max_cwnd;
}

void rt_cc_on_dup_ack(rt_cc *c) { if (c->in_recovery) c->cwnd += c->mss; }   /* window inflation */

void rt_cc_on_fast_retransmit(rt_cc *c, double flight, double now) {
    (void)now;
    if (c->kind == CC_RENO) c->ssthresh = fmax(flight / 2.0, 2.0 * c->mss);
    else cubic_loss(c);
    c->cwnd = c->ssthresh + 3 * c->mss;
    c->in_recovery = 1;
}

void rt_cc_on_partial_ack(rt_cc *c, double acked) {
    c->cwnd = fmax(c->mss, c->cwnd - acked + c->mss);
}

void rt_cc_on_recovery_exit(rt_cc *c) {
    c->cwnd = fmax(c->ssthresh, c->mss);
    c->in_recovery = 0;
}

void rt_cc_on_timeout(rt_cc *c, double flight, double now) {
    (void)now;
    if (c->kind == CC_RENO) c->ssthresh = fmax(flight / 2.0, 2.0 * c->mss);
    else cubic_loss(c);
    c->cwnd = c->mss;
    c->in_recovery = 0;
}
