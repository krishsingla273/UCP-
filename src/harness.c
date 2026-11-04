/* harness.c - in-process transfer harness used by `selftest` and `bench`. */
#define _GNU_SOURCE
#include "harness.h"
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

void make_data(uint8_t *b, size_t n, uint64_t seed) {
    uint64_t x = seed * 0x9E3779B97F4A7C15ULL + 12345;
    for (size_t i = 0; i < n; i++) { x ^= x >> 12; x ^= x << 25; x ^= x >> 27; b[i] = (uint8_t)((x * 0x2545F4914F6CDD1DULL) >> 56); }
}

typedef struct { rt_ep *ep; double timeout; char sha[65]; size_t total; rt_stats st; char err[96]; int slow_kbps; } srv_t;

static void *srv_thread(void *arg) {
    srv_t *s = arg;
    double dl = rt_now() + s->timeout;
    rt_conn *c = NULL;
    while (!c && rt_now() < dl) { rt_run_once(s->ep, 20); c = rt_accept(s->ep); }
    if (!c) { strcpy(s->err, "server: accept timeout"); return NULL; }
    sha256_ctx h; sha256_init(&h);
    static __thread uint8_t buf[65536];
    double t_start = rt_now();
    for (;;) {
        size_t cap = sizeof buf;
        if (s->slow_kbps > 0) {                                   /* token bucket: reader limited to slow_kbps KB/s */
            double allowed = s->slow_kbps * 1000.0 * (rt_now() - t_start);
            size_t budget = allowed > (double)s->total ? (size_t)(allowed - (double)s->total) : 0;
            if (budget == 0) { rt_run_once(s->ep, 2); if (rt_now() > dl) { strcpy(s->err, "server: recv timeout"); break; } continue; }
            if (budget < cap) cap = budget;
        }
        int n = rt_recv(c, buf, cap);
        if (n > 0) { sha256_update(&h, buf, n); s->total += n; continue; }
        if (n < 0) { if (n == -2) strcpy(s->err, "server: connection error"); break; }
        rt_run_once(s->ep, 10);
        if (rt_now() > dl) { strcpy(s->err, "server: recv timeout"); break; }
    }
    sha256_final(&h, s->sha);
    rt_close(c);
    double d2 = rt_now() + 5;
    while (rt_conn_state(c) != ST_CLOSED && rt_now() < d2) rt_run_once(s->ep, 10);
    rt_stats_get(c, &s->st);
    return NULL;
}

typedef struct { proxy_t *p; volatile int stop; } px_t;
static void *px_thread(void *arg) { px_t *x = arg; proxy_run(x->p, &x->stop); return NULL; }

int run_transfer(const xfer_args *a, xfer_res *r) {
    memset(r, 0, sizeof *r);
    uint8_t *data = malloc(a->size ? a->size : 1);
    make_data(data, a->size, a->seed);
    char *want = r->want;
    sha256_ctx h; sha256_init(&h); sha256_update(&h, data, a->size); sha256_final(&h, want);

    srv_t srv = {.timeout = a->timeout, .slow_kbps = a->slow_kbps};
    srv.ep = rt_ep_new("127.0.0.1", 0, &a->cfg);
    rt_listen(srv.ep);
    struct sockaddr_in target = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK), .sin_port = htons(rt_ep_port(srv.ep))};
    px_t px = {0};
    pthread_t pt, st;
    if (a->imp && imp_active(a->imp)) {
        struct sockaddr_in srvaddr = target;
        px.p = proxy_new("127.0.0.1", 0, &srvaddr, a->imp);
        target.sin_port = htons(proxy_port(px.p));
        pthread_create(&pt, NULL, px_thread, &px);
        r->proxied = 1;
    }
    rt_ep *cep = rt_ep_new("127.0.0.1", 0, &a->cfg);
    pthread_create(&st, NULL, srv_thread, &srv);

    double t_conn = rt_now();
    rt_conn *c = rt_connect(cep, &target);
    double dl = rt_now() + 30;
    while (rt_conn_state(c) != ST_ESTABLISHED && rt_conn_state(c) != ST_CLOSED && rt_now() < dl) rt_run_once(cep, 5);
    if (rt_conn_state(c) != ST_ESTABLISHED) {
        strcpy(r->err, "connect failed");
    } else {
        r->handshake_ms = (rt_now() - t_conn) * 1000;
        double t0 = rt_now(), end = t0 + a->timeout;
        size_t off = 0;
        double last_prog = 0;
#define REPORT() do { if (a->progress && rt_now() - last_prog >= 0.04) { last_prog = rt_now(); \
            rt_stats ps; rt_stats_get(c, &ps); double ack = (double)rt_bytes_acked(c), el = rt_now() - t0; \
            xfer_progress xp = {el, a->size ? ack / a->size : 1, el > 0 ? ack * 8 / el / 1e6 : 0, ps.cwnd, ps.ssthresh, \
                                (double)ps.bytes_sent - ack, ps.srtt_ms, (size_t)ack, a->size, (unsigned long long)ps.retransmissions}; \
            a->progress(a->user, &xp); } } while (0)
        while (off < a->size && rt_now() < end) {
            int n = rt_send(c, data + off, a->size - off);
            if (n < 0) { strcpy(r->err, "send error"); break; }
            off += n;
            rt_run_once(cep, n > 0 ? 0 : 2);
            REPORT();
        }
        while (!rt_flushed(c) && rt_conn_state(c) != ST_CLOSED && rt_now() < end) { rt_run_once(cep, 2); REPORT(); }
        r->duration_s = rt_now() - t0;
        if (!rt_flushed(c)) strcpy(r->err, "flush timeout");
        rt_close(c);
        double d2 = rt_now() + 5;
        while (rt_conn_state(c) != ST_CLOSED && rt_now() < d2) rt_run_once(cep, 5);
        rt_stats_get(c, &r->cli);
        if (a->trace_path) rt_trace_write(c, a->trace_path);
        if (a->capture) {
            r->ntrace = rt_trace_len(c);
            r->trace = malloc((r->ntrace + 1) * sizeof(rt_trace_pt));
            if (r->ntrace) memcpy(r->trace, rt_trace_data(c), r->ntrace * sizeof(rt_trace_pt));
            const uint8_t *hs; r->nseg = rt_segment_history(c, &hs);
            r->seg_hist = malloc(r->nseg + 1);
            if (r->nseg) memcpy(r->seg_hist, hs, r->nseg);
            r->nevents = rt_event_len(c);
            r->events = malloc((r->nevents + 1) * sizeof(rt_event));
            if (r->nevents) memcpy(r->events, rt_event_data(c), r->nevents * sizeof(rt_event));
        }
    }
    pthread_join(st, NULL);
    strcpy(r->sha, srv.sha);
    r->srv = srv.st;
    if (srv.err[0] && !r->err[0]) strcpy(r->err, srv.err);
    r->ok = !r->err[0] && strcmp(srv.sha, want) == 0 && srv.total == a->size;
    r->mbps = r->duration_s > 0 ? a->size * 8.0 / r->duration_s / 1e6 : 0;
    if (r->proxied) {
        px.stop = 1;
        pthread_join(pt, NULL);
        proxy_stats(px.p, &r->pf, &r->pr);
        proxy_free(px.p);
    }
    rt_conn_free(c);
    rt_ep_free(cep);
    rt_ep_free(srv.ep);
    free(data);
    return r->ok ? 0 : -1;
}

void xfer_res_free(xfer_res *r) { free(r->trace); free(r->events); free(r->seg_hist); r->trace = NULL; r->events = NULL; r->seg_hist = NULL; }

/* ---- multi-flow fairness: N clients share one bottleneck toward one server */
typedef struct { rt_ep *ep; int n; size_t size; double timeout; char want[65]; int ok[8]; } fsrv_t;
static void *fsrv_thread(void *arg) {
    fsrv_t *s = arg;
    rt_conn *cs[8] = {0}; sha256_ctx h[8]; size_t tot[8] = {0}; int done[8] = {0}, acc = 0, ndone = 0;
    double dl = rt_now() + s->timeout;
    static __thread uint8_t buf[65536];
    while (ndone < s->n && rt_now() < dl) {
        rt_run_once(s->ep, 5);
        rt_conn *c;
        while (acc < s->n && (c = rt_accept(s->ep))) { cs[acc] = c; sha256_init(&h[acc]); acc++; }
        for (int i = 0; i < acc; i++) {
            if (done[i]) continue;
            for (;;) {
                int n = rt_recv(cs[i], buf, sizeof buf);
                if (n > 0) { sha256_update(&h[i], buf, n); tot[i] += n; continue; }
                if (n < 0) { done[i] = 1; ndone++; rt_close(cs[i]); }
                break;
            }
        }
    }
    for (int i = 0; i < acc; i++) {
        char hex[65]; sha256_final(&h[i], hex);
        s->ok[i] = done[i] && tot[i] == s->size && !strcmp(hex, s->want);
    }
    double d2 = rt_now() + 3;
    while (rt_now() < d2) rt_run_once(s->ep, 10);
    return NULL;
}

typedef struct { const struct sockaddr_in *to; const uint8_t *data; size_t size; rt_cfg cfg; double timeout, dur; int ok; } fcli_t;
static void *fcli_thread(void *arg) {
    fcli_t *f = arg;
    rt_ep *ep = rt_ep_new("127.0.0.1", 0, &f->cfg);
    rt_conn *c = rt_connect(ep, f->to);
    double dl = rt_now() + 30;
    while (rt_conn_state(c) != ST_ESTABLISHED && rt_now() < dl) rt_run_once(ep, 5);
    if (rt_conn_state(c) == ST_ESTABLISHED) {
        double t0 = rt_now(), end = t0 + f->timeout;
        size_t off = 0;
        while (off < f->size && rt_now() < end) { int n = rt_send(c, f->data + off, f->size - off); if (n < 0) break; off += n; rt_run_once(ep, n > 0 ? 0 : 2); }
        while (!rt_flushed(c) && rt_now() < end) rt_run_once(ep, 2);
        f->dur = rt_now() - t0;
        f->ok = rt_flushed(c);
        rt_close(c);
        double d2 = rt_now() + 3;
        while (rt_conn_state(c) != ST_CLOSED && rt_now() < d2) rt_run_once(ep, 5);
    }
    rt_conn_free(c);
    rt_ep_free(ep);
    return NULL;
}

int run_fairness(size_t size, const imp_cfg *imp, const rt_cfg *cfg, int flows, fair_res *out) {
    memset(out, 0, sizeof *out);
    if (flows > 8) flows = 8;
    uint8_t *data = malloc(size ? size : 1);
    make_data(data, size, 3);
    fsrv_t s = {.n = flows, .size = size, .timeout = 120};
    sha256_ctx h; sha256_init(&h); sha256_update(&h, data, size); sha256_final(&h, s.want);
    s.ep = rt_ep_new("127.0.0.1", 0, cfg);
    rt_listen(s.ep);
    struct sockaddr_in to = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK), .sin_port = htons(rt_ep_port(s.ep))};
    struct sockaddr_in srvaddr = to;
    px_t px = {0};
    pthread_t pt, st, ct[8];
    px.p = proxy_new("127.0.0.1", 0, &srvaddr, imp);
    to.sin_port = htons(proxy_port(px.p));
    pthread_create(&pt, NULL, px_thread, &px);
    pthread_create(&st, NULL, fsrv_thread, &s);
    fcli_t fc[8];
    for (int i = 0; i < flows; i++) { fc[i] = (fcli_t){.to = &to, .data = data, .size = size, .cfg = *cfg, .timeout = 120}; pthread_create(&ct[i], NULL, fcli_thread, &fc[i]); }
    for (int i = 0; i < flows; i++) pthread_join(ct[i], NULL);
    pthread_join(st, NULL);
    px.stop = 1; pthread_join(pt, NULL); proxy_free(px.p);
    double sum = 0, sq = 0;
    out->flows = flows;
    for (int i = 0; i < flows; i++) {
        out->mbps[i] = fc[i].dur > 0 ? size * 8.0 / fc[i].dur / 1e6 : 0;
        out->ok[i] = fc[i].ok && s.ok[i];
        sum += out->mbps[i]; sq += out->mbps[i] * out->mbps[i];
    }
    out->total_mbps = sum;
    out->jain = sq > 0 ? sum * sum / (flows * sq) : 0;
    rt_ep_free(s.ep);
    free(data);
    int ok = 1;
    for (int i = 0; i < flows; i++) ok &= out->ok[i];
    return ok ? 0 : -1;
}

/* ---- head-of-line blocking experiment ---- */
#define CHUNK 4096
typedef struct {
    rt_ep *ep; rt_conn *conn; int n, multi; size_t per; double timeout;
    volatile double t_start; volatile int started;
    sha256_ctx h[8]; size_t got[8]; double done_t[8]; char err[96];
    double prev_t, prev_held, acc, blocked, peak;
    int ntl; double tl_t[240]; double tl_b[8][240]; double tl_h[240]; double tl_step, tl_next;
} hsrv_t;

static void hol_deliver(hsrv_t *s, int sid, const uint8_t *d, size_t n) {
    sha256_update(&s->h[sid], d, n);
    s->got[sid] += n;
    if (s->got[sid] >= s->per && s->done_t[sid] < 0) s->done_t[sid] = rt_now() - s->t_start;
}

/* time-weighted accounting of how much data is being held back right now */
static void hol_track(hsrv_t *s) {
    double t = rt_now() - s->t_start, held = (double)rt_held_bytes(s->conn);
    if (s->prev_t > 0) {
        double dt = t - s->prev_t;
        s->acc += s->prev_held * dt;
        if (s->prev_held > 0) s->blocked += dt;
    }
    if (held > s->peak) s->peak = held;
    s->prev_t = t > 0 ? t : 1e-9; s->prev_held = held;
}

static void hol_sample(hsrv_t *s) {
    double t = rt_now() - s->t_start;
    if (t < s->tl_next) return;
    if (s->ntl == 240) {                                      /* full: keep every 2nd sample, sample half as often */
        for (int i = 0; i < 120; i++) { s->tl_t[i] = s->tl_t[2 * i]; s->tl_h[i] = s->tl_h[2 * i]; for (int k = 0; k < s->n; k++) s->tl_b[k][i] = s->tl_b[k][2 * i]; }
        s->ntl = 120; s->tl_step *= 2;
    }
    s->tl_t[s->ntl] = t;
    s->tl_h[s->ntl] = (double)rt_held_bytes(s->conn);
    for (int k = 0; k < s->n; k++) s->tl_b[k][s->ntl] = (double)s->got[k];
    s->ntl++;
    s->tl_next = t + s->tl_step;
}

static void *hsrv_thread(void *arg) {
    hsrv_t *s = arg;
    double dl = rt_now() + s->timeout;
    rt_conn *c = NULL;
    while (!c && rt_now() < dl) { rt_run_once(s->ep, 20); c = rt_accept(s->ep); }
    if (!c) { strcpy(s->err, "server: accept timeout"); return NULL; }
    s->conn = c;
    while (!s->started && rt_now() < dl) rt_run_once(s->ep, 1);
    static __thread uint8_t buf[65536];
    uint8_t hdr[3]; int hl = 0, cur = 0; size_t remain = 0;      /* ordered mode: tiny frame parser */
    for (;;) {
        int alldone = 1;
        for (int i = 0; i < s->n; i++) if (s->got[i] < s->per) alldone = 0;
        if (alldone) break;
        if (rt_now() > dl) { strcpy(s->err, "server: timeout"); break; }
        int progress = 0;
        if (s->multi) {
            for (int i = 0; i < s->n; i++) {
                int n;
                while ((n = rt_stream_recv(c, i, buf, sizeof buf)) > 0) { hol_deliver(s, i, buf, n); progress = 1; }
            }
        } else {
            int n = rt_recv(c, buf, sizeof buf);
            if (n > 0) {
                progress = 1;
                size_t o = 0;
                while (o < (size_t)n) {
                    if (remain == 0) {
                        hdr[hl++] = buf[o++];
                        if (hl == 3) { cur = hdr[0]; remain = (hdr[1] << 8) | hdr[2]; hl = 0; }
                        continue;
                    }
                    size_t take = remain < (size_t)n - o ? remain : (size_t)n - o;
                    hol_deliver(s, cur, buf + o, take);
                    o += take; remain -= take;
                }
            } else if (n < 0) break;
        }
        hol_track(s);
        hol_sample(s);
        rt_run_once(s->ep, progress ? 0 : 1);
        hol_track(s);
    }
    s->tl_next = 0; hol_sample(s);
    rt_close(c);
    double d2 = rt_now() + 3;
    while (rt_conn_state(c) != ST_CLOSED && rt_now() < d2) rt_run_once(s->ep, 5);
    return NULL;
}

int run_hol(size_t per, int n, const imp_cfg *imp, const rt_cfg *cfg0, int multi, hol_res *out) {
    memset(out, 0, sizeof *out);
    if (n > 8) n = 8;
    out->nstreams = n; out->multiplexed = multi;
    rt_cfg cfg = *cfg0; cfg.trace = 0;
    uint8_t *data[8]; char want[8][65];
    for (int i = 0; i < n; i++) {
        data[i] = malloc(per ? per : 1);
        make_data(data[i], per, 1000 + i);
        sha256_ctx h; sha256_init(&h); sha256_update(&h, data[i], per); sha256_final(&h, want[i]);
    }
    hsrv_t *s = calloc(1, sizeof *s);
    s->n = n; s->multi = multi; s->per = per; s->timeout = 120; s->tl_step = 0.025;
    for (int i = 0; i < n; i++) { sha256_init(&s->h[i]); s->done_t[i] = -1; }
    s->ep = rt_ep_new("127.0.0.1", 0, &cfg);
    rt_listen(s->ep);
    struct sockaddr_in to = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK), .sin_port = htons(rt_ep_port(s->ep))};
    px_t px = {0};
    pthread_t pt, st;
    int proxied = imp && imp_active(imp);
    if (proxied) {
        struct sockaddr_in sa = to;
        px.p = proxy_new("127.0.0.1", 0, &sa, imp);
        to.sin_port = htons(proxy_port(px.p));
        pthread_create(&pt, NULL, px_thread, &px);
    }
    cfg.streams = multi;
    rt_ep *cep = rt_ep_new("127.0.0.1", 0, &cfg);
    pthread_create(&st, NULL, hsrv_thread, s);
    rt_conn *c = rt_connect(cep, &to);
    double dl = rt_now() + 30;
    while (rt_conn_state(c) != ST_ESTABLISHED && rt_conn_state(c) != ST_CLOSED && rt_now() < dl) rt_run_once(cep, 5);
    if (rt_conn_state(c) != ST_ESTABLISHED) strcpy(out->err, "connect failed");
    else if (multi && !rt_conn_is_streams(c)) strcpy(out->err, "peer did not accept multi-stream mode");
    else {
        s->t_start = rt_now(); s->started = 1;
        double end = s->t_start + 120;
        size_t off[8] = {0};
        if (multi) {
            int left = n;
            while (left && rt_now() < end) {
                left = 0;
                for (int i = 0; i < n; i++) {
                    if (off[i] >= per) continue;
                    size_t k = per - off[i] < CHUNK ? per - off[i] : CHUNK;
                    int r = rt_stream_send(c, i, data[i] + off[i], k, off[i] + k == per);
                    if (r < 0) { strcpy(out->err, "send error"); left = 0; break; }
                    off[i] += r;
                    if (off[i] < per) left++;
                }
                rt_run_once(cep, 0);
            }
            if (per == 0) for (int i = 0; i < n; i++) rt_stream_send(c, i, "", 0, 1);
        } else {
            uint8_t fr[3 + CHUNK]; size_t fl = 0, fo = 0; int rr = 0, left = n;
            while ((left || fo < fl) && rt_now() < end) {
                if (fo >= fl) {
                    left = 0;
                    for (int k = 0; k < n; k++) if (off[k] < per) left++;
                    if (left) {
                        while (off[rr] >= per) rr = (rr + 1) % n;
                        size_t m = per - off[rr] < CHUNK ? per - off[rr] : CHUNK;
                        fr[0] = (uint8_t)rr; fr[1] = (uint8_t)(m >> 8); fr[2] = (uint8_t)m;
                        memcpy(fr + 3, data[rr] + off[rr], m);
                        off[rr] += m; fl = 3 + m; fo = 0; rr = (rr + 1) % n;
                    }
                }
                if (fo < fl) { int r = rt_send(c, fr + fo, fl - fo); if (r < 0) { strcpy(out->err, "send error"); break; } fo += r; }
                rt_run_once(cep, 0);
            }
        }
        while (!rt_flushed(c) && rt_now() < end && rt_conn_state(c) != ST_CLOSED) rt_run_once(cep, 1);
        rt_close(c);
        double d2 = rt_now() + 5;
        while (rt_conn_state(c) != ST_CLOSED && rt_now() < d2) rt_run_once(cep, 5);
    }
    s->started = 1;
    pthread_join(st, NULL);
    rt_stats_get(c, &out->cli);
    int ok = !out->err[0] && !s->err[0];
    double dur = 0;
    for (int i = 0; i < n; i++) {
        char hex[65]; sha256_final(&s->h[i], hex);
        if (strcmp(hex, want[i]) || s->got[i] != per) ok = 0;
        out->completion_s[i] = s->done_t[i];
        if (s->done_t[i] > dur) dur = s->done_t[i];
    }
    if (s->err[0] && !out->err[0]) strcpy(out->err, s->err);
    out->ok = ok; out->duration_s = dur;
    out->mbps = dur > 0 ? (double)per * n * 8 / dur / 1e6 : 0;
    out->avg_held_kb = dur > 0 ? s->acc / dur / 1000.0 : 0;
    out->peak_held_kb = s->peak / 1000.0;
    out->blocked_frac = dur > 0 ? s->blocked / dur : 0;
    out->ntl = s->ntl;
    memcpy(out->tl_t, s->tl_t, sizeof out->tl_t);
    memcpy(out->tl_b, s->tl_b, sizeof out->tl_b);
    memcpy(out->tl_h, s->tl_h, sizeof out->tl_h);
    if (proxied) { px.stop = 1; pthread_join(pt, NULL); proxy_free(px.p); }
    rt_conn_free(c); rt_ep_free(cep); rt_ep_free(s->ep);
    for (int i = 0; i < n; i++) free(data[i]);
    free(s);
    return ok ? 0 : -1;
}

/* ---- native TCP baseline */
typedef struct { int lfd; size_t total; char sha[65]; } tcp_srv_t;
static void *tcp_srv_thread(void *arg) {
    tcp_srv_t *s = arg;
    int fd = accept(s->lfd, NULL, NULL);
    sha256_ctx h; sha256_init(&h);
    static __thread uint8_t buf[65536];
    ssize_t n;
    while ((n = recv(fd, buf, sizeof buf, 0)) > 0) { sha256_update(&h, buf, n); s->total += n; }
    sha256_final(&h, s->sha);
    close(fd);
    return NULL;
}

int tcp_transfer(size_t size, uint64_t seed, double *mbps) {
    uint8_t *data = malloc(size ? size : 1);
    make_data(data, size, seed);
    char want[65];
    sha256_ctx h; sha256_init(&h); sha256_update(&h, data, size); sha256_final(&h, want);
    tcp_srv_t s = {0};
    s.lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    bind(s.lfd, (struct sockaddr *)&a, sizeof a);
    listen(s.lfd, 1);
    socklen_t l = sizeof a;
    getsockname(s.lfd, (struct sockaddr *)&a, &l);
    pthread_t t;
    pthread_create(&t, NULL, tcp_srv_thread, &s);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    connect(fd, (struct sockaddr *)&a, sizeof a);
    double t0 = rt_now();
    size_t off = 0;
    while (off < size) { ssize_t n = send(fd, data + off, size - off, 0); if (n <= 0) break; off += n; }
    shutdown(fd, SHUT_WR);
    pthread_join(t, NULL);
    double dur = rt_now() - t0;
    close(fd); close(s.lfd); free(data);
    *mbps = size * 8.0 / dur / 1e6;
    return strcmp(s.sha, want) == 0 ? 0 : -1;
}
