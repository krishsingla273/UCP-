
#define _GNU_SOURCE
#include "ui.h"
#include "harness.h"
#include "ui_html.h"
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

typedef struct { char *p; size_t n, cap; } sb_t;
static void sb_add(sb_t *b, const char *fmt, ...) {
    va_list ap;
    for (;;) {
        va_start(ap, fmt);
        int w = vsnprintf(b->p + b->n, b->cap - b->n, fmt, ap);
        va_end(ap);
        if (w >= 0 && b->n + (size_t)w < b->cap) { b->n += w; return; }
        b->cap = b->cap ? b->cap * 2 : 4096;
        b->p = realloc(b->p, b->cap);
    }
}

static double qd(const char *q, const char *key, double def) {
    size_t kl = strlen(key);
    for (const char *p = q; p && *p; ) {
        if (!strncmp(p, key, kl) && p[kl] == '=') return atof(p + kl + 1);
        p = strchr(p, '&');
        if (p) p++;
    }
    return def;
}
static int qs(const char *q, const char *key, char *out, size_t cap) {
    size_t kl = strlen(key);
    for (const char *p = q; p && *p; ) {
        if (!strncmp(p, key, kl) && p[kl] == '=') {
            size_t n = strcspn(p + kl + 1, "& ");
            if (n >= cap) n = cap - 1;
            memcpy(out, p + kl + 1, n); out[n] = 0;
            return 1;
        }
        p = strchr(p, '&');
        if (p) p++;
    }
    return 0;
}

static void respond(int fd, int code, const char *ctype, const char *body, size_t n) {
    char hdr[256];
    int h = snprintf(hdr, sizeof hdr, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n",
                     code, code == 200 ? "OK" : "Error", ctype, n);
    send(fd, hdr, h, MSG_NOSIGNAL);
    size_t off = 0;
    while (off < n) { ssize_t w = send(fd, body + off, n - off, MSG_NOSIGNAL); if (w <= 0) break; off += w; }
}

static void json_dir(sb_t *b, const char *name, const imp_dir_stats *d) {
    sb_add(b, "\"%s\":{\"rx\":%llu,\"lost\":%llu,\"qdrop\":%llu,\"dup\":%llu,\"reorder\":%llu,\"corrupt\":%llu}", name,
           (unsigned long long)d->rx, (unsigned long long)d->dropped_loss, (unsigned long long)d->dropped_queue,
           (unsigned long long)d->dup, (unsigned long long)d->reordered, (unsigned long long)d->corrupted);
}

/* ---- request parameters -> transfer arguments (shared by the synchronous and the live API) */
static void parse_params(const char *q, xfer_args *a, imp_cfg *ic, char *desc, size_t dcap) {
    memset(a, 0, sizeof *a);
    rt_cfg_default(&a->cfg);
    char cc[16] = "reno", preset[32];
    imp_cfg_default(ic);
    if (qs(q, "preset", preset, sizeof preset)) imp_preset(preset, ic);
    ic->loss = qd(q, "loss", ic->loss);
    ic->reorder = qd(q, "reorder", ic->reorder);
    ic->dup = qd(q, "dup", ic->dup);
    ic->corrupt = qd(q, "corrupt", ic->corrupt);
    ic->delay_ms = qd(q, "delay", ic->delay_ms);
    ic->jitter_ms = qd(q, "jitter", ic->jitter_ms);
    ic->rate_mbps = qd(q, "rate", ic->rate_mbps);
    ic->queue = (int)qd(q, "queue", ic->queue);
    ic->seed = (uint64_t)qd(q, "seed", 1);
    qs(q, "cc", cc, sizeof cc);
    a->cfg.cc = !strcmp(cc, "cubic") ? CC_CUBIC : CC_RENO;
    a->cfg.tlp = (int)qd(q, "tlp", 1);
    a->cfg.rack = (int)qd(q, "rack", 1);
    int fec = (int)qd(q, "fec", 0);                        /* -1 = adaptive, 0 = off, N = one parity packet per N */
    if (fec < 0) a->cfg.fec_auto = 1; else a->cfg.fec = fec > 16 ? 16 : fec;
    double minrto = qd(q, "minrto", 0);                    /* ms; 0 = library default */
    if (minrto > 0) a->cfg.min_rto = minrto / 1000.0;
    double mb = qd(q, "size", 2);
    if (mb < 0.001) mb = 0.001;
    if (mb > 64) mb = 64;
    a->size = (size_t)(mb * 1e6);
    a->imp = ic;
    a->seed = (uint64_t)qd(q, "dseed", 1);
    a->timeout = 60;
    a->slow_kbps = (int)qd(q, "slow", 0);
    if (a->slow_kbps > 0) a->cfg.recv_buf = 32 * 1024;     /* small buffer so the receiver's window visibly throttles the sender */
    a->capture = 1;
    imp_describe(ic, desc, dcap);
}

static void build_result(sb_t *b, const xfer_res *r, const xfer_args *a, const char *desc) {
    sb_add(b, "{\"ok\":%s,\"sha\":\"%s\",\"want\":\"%s\",\"duration\":%.4f,\"mbps\":%.3f,\"handshake_ms\":%.3f,\"size\":%zu,"
              "\"cc\":\"%s\",\"tlp\":%d,\"rack\":%d,\"minrto\":%.0f,\"fec\":%d,\"impair\":\"%s\",\"err\":\"%s\",",
           r->ok ? "true" : "false", r->sha, r->want, r->duration_s, r->mbps, r->handshake_ms, a->size,
           a->cfg.cc == CC_CUBIC ? "cubic" : "reno", a->cfg.tlp, a->cfg.rack, a->cfg.min_rto * 1000, a->cfg.fec_auto ? -1 : a->cfg.fec, desc, r->err);
    sb_add(b, "\"cli\":{\"sent\":%llu,\"recv\":%llu,\"retx\":%llu,\"fast\":%llu,\"rto\":%llu,\"tlp\":%llu,\"rack\":%llu,\"spurious\":%llu,"
              "\"srtt_ms\":%.3f,\"rto_ms\":%.1f,\"cwnd\":%d,\"ssthresh\":%d,\"probes\":%llu,\"bad\":%llu,\"fec_sent\":%llu,\"fec_k\":%d},",
           (unsigned long long)r->cli.packets_sent, (unsigned long long)r->cli.packets_received,
           (unsigned long long)r->cli.retransmissions, (unsigned long long)r->cli.fast_retransmits,
           (unsigned long long)r->cli.timeouts, (unsigned long long)r->cli.tlp_probes, (unsigned long long)r->cli.rack_retx,
           (unsigned long long)r->cli.spurious, r->cli.srtt_ms, r->cli.rto_ms, r->cli.cwnd, r->cli.ssthresh,
           (unsigned long long)r->cli.window_probes, (unsigned long long)r->cli.checksum_failures, (unsigned long long)r->cli.fec_sent, r->cli.fec_k);
    sb_add(b, "\"srv\":{\"sent\":%llu,\"recv\":%llu,\"ooo\":%llu,\"dup\":%llu,\"bad\":%llu,\"fec_rec\":%llu},",
           (unsigned long long)r->srv.packets_sent, (unsigned long long)r->srv.packets_received,
           (unsigned long long)r->srv.ooo_packets, (unsigned long long)r->srv.dup_packets,
           (unsigned long long)r->srv.checksum_failures, (unsigned long long)r->srv.fec_recovered);
    if (r->proxied) { sb_add(b, "\"proxy\":{"); json_dir(b, "fwd", &r->pf); sb_add(b, ","); json_dir(b, "rev", &r->pr); sb_add(b, "},"); }
    else sb_add(b, "\"proxy\":null,");
    sb_add(b, "\"trace\":[");
    size_t step = r->ntrace > 700 ? r->ntrace / 700 : 1;
    int first = 1;
    for (size_t i = 0; i < r->ntrace; i += step) {
        const rt_trace_pt *t = &r->trace[i];
        sb_add(b, "%s[%.4f,%.0f,%.0f,%.0f,%.3f,%d]", first ? "" : ",", t->t, t->cwnd, t->ssthresh, t->flight, t->srtt * 1000, t->phase);
        first = 0;
    }
    sb_add(b, "],\"events\":[");
    for (size_t i = 0; i < r->nevents && i < 600; i++) sb_add(b, "%s[%.4f,%d]", i ? "," : "", r->events[i].t, r->events[i].kind);
    /* repair map: one digit per data segment = times it was retransmitted (bucketed to <= 2400 cells, worst case per bucket) */
    size_t bucket = r->nseg > 2400 ? (r->nseg + 2399) / 2400 : 1;
    sb_add(b, "],\"segments\":%zu,\"map\":\"", r->nseg);
    for (size_t i = 0; i < r->nseg; i += bucket) {
        int m = 0;
        for (size_t k = i; k < i + bucket && k < r->nseg; k++) if (r->seg_hist[k] > m) m = r->seg_hist[k];
        sb_add(b, "%c", '0' + (m > 9 ? 9 : m));
    }
    sb_add(b, "\"}");
}

static void api_run(int fd, const char *q) {      /* synchronous variant (curl-friendly) */
    xfer_args a; imp_cfg ic; char desc[256];
    parse_params(q, &a, &ic, desc, sizeof desc);
    xfer_res r;
    run_transfer(&a, &r);
    sb_t b = {0};
    build_result(&b, &r, &a, desc);
    respond(fd, 200, "application/json", b.p, b.n);
    free(b.p);
    xfer_res_free(&r);
}

/* ---- head-of-line blocking experiment (synchronous: ~5-15 s) */
static void json_hol(sb_t *b, const char *name, const hol_res *h) {
    sb_add(b, "\"%s\":{\"ok\":%s,\"duration\":%.3f,\"avg\":%.2f,\"peak\":%.2f,\"frac\":%.3f,\"err\":\"%s\",\"completion\":[", name,
           h->ok ? "true" : "false", h->duration_s, h->avg_held_kb, h->peak_held_kb, h->blocked_frac, h->err);
    for (int i = 0; i < h->nstreams; i++) sb_add(b, "%s%.3f", i ? "," : "", h->completion_s[i]);
    sb_add(b, "],\"tl\":[");
    for (int k = 0; k < h->ntl; k++) sb_add(b, "%s[%.3f,%.2f]", k ? "," : "", h->tl_t[k], h->tl_h[k] / 1000.0);
    sb_add(b, "]}");
}

static void api_hol(int fd, const char *q) {
    xfer_args a; imp_cfg ic; char desc[256];
    parse_params(q, &a, &ic, desc, sizeof desc);
    int n = (int)qd(q, "streams", 4);
    if (n < 2) n = 2;
    if (n > 8) n = 8;
    double mb = qd(q, "per", 0.4);
    if (mb < 0.05) mb = 0.05;
    if (mb > 4) mb = 4;
    size_t per = (size_t)(mb * 1e6);
    hol_res o, m;
    ic.seed = 1;
    run_hol(per, n, &ic, &a.cfg, 0, &o);
    run_hol(per, n, &ic, &a.cfg, 1, &m);
    sb_t b = {0};
    sb_add(&b, "{\"streams\":%d,\"per\":%zu,\"impair\":\"%s\",", n, per, desc);
    json_hol(&b, "ordered", &o);
    sb_add(&b, ",");
    json_hol(&b, "multi", &m);
    sb_add(&b, "}");
    respond(fd, 200, "application/json", b.p, b.n);
    free(b.p);
}

/* ---- live runs: a worker thread runs the transfer; the page polls for new trace points ---- */
typedef struct { double t, cwnd, ssthresh, flight, srtt; } livept;
static struct {
    pthread_mutex_t mu;
    int state;                     /* 0 idle, 1 running, 2 done */
    int have_thread;
    pthread_t th;
    xfer_args args; imp_cfg ic; char desc[256];
    livept *pts; size_t n, cap;
    double frac, mbps;
    char *result; size_t result_n;
} G = {.mu = PTHREAD_MUTEX_INITIALIZER};

static void on_progress(void *u, const xfer_progress *p) {
    (void)u;
    pthread_mutex_lock(&G.mu);
    if (G.n == G.cap) { G.cap = G.cap ? G.cap * 2 : 512; G.pts = realloc(G.pts, G.cap * sizeof(livept)); }
    G.pts[G.n++] = (livept){p->t, p->cwnd, p->ssthresh < 1e8 ? p->ssthresh : 0, p->flight, p->srtt_ms};
    G.frac = p->frac; G.mbps = p->mbps;
    pthread_mutex_unlock(&G.mu);
}

static void *worker(void *arg) {
    (void)arg;
    xfer_res r;
    run_transfer(&G.args, &r);
    sb_t b = {0};
    build_result(&b, &r, &G.args, G.desc);
    xfer_res_free(&r);
    pthread_mutex_lock(&G.mu);
    free(G.result);
    G.result = b.p; G.result_n = b.n; G.state = 2;
    pthread_mutex_unlock(&G.mu);
    return NULL;
}

static void api_start(int fd, const char *q) {
    pthread_mutex_lock(&G.mu);
    if (G.state == 1) { pthread_mutex_unlock(&G.mu); { const char *m = "{\"started\":false,\"busy\":true}"; respond(fd, 200, "application/json", m, strlen(m)); } return; }
    pthread_mutex_unlock(&G.mu);
    if (G.have_thread) pthread_join(G.th, NULL);
    pthread_mutex_lock(&G.mu);
    parse_params(q, &G.args, &G.ic, G.desc, sizeof G.desc);
    G.args.imp = &G.ic;
    G.args.progress = on_progress;
    G.n = 0; G.frac = 0; G.mbps = 0; G.state = 1;
    free(G.result); G.result = NULL; G.result_n = 0;
    pthread_mutex_unlock(&G.mu);
    pthread_create(&G.th, NULL, worker, NULL);
    G.have_thread = 1;
    { const char *m = "{\"started\":true}"; respond(fd, 200, "application/json", m, strlen(m)); }
}

static void api_poll(int fd, const char *q) {
    size_t from = (size_t)qd(q, "from", 0);
    sb_t b = {0};
    pthread_mutex_lock(&G.mu);
    sb_add(&b, "{\"state\":%d,\"frac\":%.4f,\"mbps\":%.2f,\"n\":%zu,\"pts\":[", G.state, G.frac, G.mbps, G.n);
    for (size_t i = from; i < G.n; i++)
        sb_add(&b, "%s[%.4f,%.0f,%.0f,%.0f,%.3f,0]", i > from ? "," : "", G.pts[i].t, G.pts[i].cwnd, G.pts[i].ssthresh, G.pts[i].flight, G.pts[i].srtt);
    sb_add(&b, "],\"result\":");
    if (G.state == 2 && G.result) sb_add(&b, "%s", G.result); else sb_add(&b, "null");
    sb_add(&b, "}");
    pthread_mutex_unlock(&G.mu);
    respond(fd, 200, "application/json", b.p, b.n);
    free(b.p);
}

static void api_presets(int fd) {
    sb_t b = {0};
    sb_add(&b, "[");
    const char *name; imp_cfg c;
    for (int i = 0; imp_preset_at(i, &name, &c) == 0; i++)
        sb_add(&b, "%s{\"name\":\"%s\",\"loss\":%g,\"dup\":%g,\"reorder\":%g,\"corrupt\":%g,\"delay\":%g,\"jitter\":%g,\"rate\":%g,\"queue\":%d}",
               i ? "," : "", name, c.loss, c.dup, c.reorder, c.corrupt, c.delay_ms, c.jitter_ms, c.rate_mbps, c.queue);
    sb_add(&b, "]");
    respond(fd, 200, "application/json", b.p, b.n);
    free(b.p);
}

int ui_serve(const char *bind_ip, int port, int open_browser) {
    signal(SIGPIPE, SIG_IGN);
    int ls = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
    a.sin_addr.s_addr = inet_addr(bind_ip);
    if (bind(ls, (struct sockaddr *)&a, sizeof a) < 0 || listen(ls, 8) < 0) {
        fprintf(stderr, "cannot listen on %s:%d (is another dashboard already running? try --port 8081)\n", bind_ip, port);
        return 1;
    }
    printf("Transport Lab dashboard:  http://%s:%d\nPress Ctrl+C to stop.\n", bind_ip, port);
    fflush(stdout);
    if (open_browser) {
        char cmd[160];
        snprintf(cmd, sizeof cmd, "(xdg-open http://%s:%d || open http://%s:%d || wslview http://%s:%d) >/dev/null 2>&1 &",
                 bind_ip, port, bind_ip, port, bind_ip, port);
        if (system(cmd)) {}
    }
    for (;;) {
        int fd = accept(ls, NULL, NULL);
        if (fd < 0) continue;
        char req[8192];
        ssize_t n = recv(fd, req, sizeof req - 1, 0);
        if (n <= 0) { close(fd); continue; }
        req[n] = 0;
        char path[2048] = "/";
        if (sscanf(req, "GET %2047s", path) != 1) { respond(fd, 400, "text/plain", "bad request", 11); close(fd); continue; }
        char *q = strchr(path, '?');
        if (q) *q++ = 0; else q = path + strlen(path);
        if (!strcmp(path, "/")) respond(fd, 200, "text/html; charset=utf-8", UI_HTML, sizeof UI_HTML - 1);
        else if (!strcmp(path, "/api/presets")) api_presets(fd);
        else if (!strcmp(path, "/api/run")) api_run(fd, q);
        else if (!strcmp(path, "/api/start")) api_start(fd, q);
        else if (!strcmp(path, "/api/hol")) api_hol(fd, q);
        else if (!strcmp(path, "/api/poll")) api_poll(fd, q);
        else if (!strcmp(path, "/api/tcp")) {
            double mbps = 0;
            size_t sz = (size_t)(qd(q, "size", 2) * 1e6);
            int rc = tcp_transfer(sz, 1, &mbps);
            char buf[96];
            int w = snprintf(buf, sizeof buf, "{\"ok\":%s,\"mbps\":%.2f}", rc == 0 ? "true" : "false", mbps);
            respond(fd, 200, "application/json", buf, w);
        } else respond(fd, 404, "text/plain", "not found", 9);
        close(fd);
    }
}
