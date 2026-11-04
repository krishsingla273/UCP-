/* impair.c - user-space UDP impairment proxy: loss, duplication, reordering, corruption,
 * delay/jitter, and a rate-limited bottleneck with a finite drop-tail queue. Seeded. */
#define _GNU_SOURCE
#include "rtp.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>

#define MAX_CLIENTS 16
#define HEAP_MAX 16384

void imp_cfg_default(imp_cfg *c) { *c = (imp_cfg){.reorder_ms = 10.0, .queue = 100, .seed = 1}; }
int imp_active(const imp_cfg *c) {
    return c->loss || c->dup || c->reorder || c->corrupt || c->delay_ms || c->jitter_ms || c->rate_mbps;
}
void imp_describe(const imp_cfg *c, char *out, size_t cap) {
    char *p = out; size_t left = cap;
    out[0] = 0;
#define ADD(name, v) do { if (v) { int n = snprintf(p, left, "%s%s=%g", p == out ? "" : ", ", name, (double)(v)); p += n; left -= n; } } while (0)
    ADD("loss", c->loss); ADD("dup", c->dup); ADD("reorder", c->reorder); ADD("corrupt", c->corrupt);
    ADD("delay_ms", c->delay_ms); ADD("jitter_ms", c->jitter_ms); ADD("rate_mbps", c->rate_mbps);
    if (c->rate_mbps) { int n = snprintf(p, left, ", queue=%dpkts", c->queue); p += n; left -= n; }
    if (!out[0]) snprintf(out, cap, "none");
}

typedef struct { uint64_t s; imp_dir_stats st; double link_free; } dir_t;
typedef struct {
    double when; uint64_t order; int fd, len; struct sockaddr_in dst; uint8_t data[1600];
} item_t;

struct proxy {
    imp_cfg cfg;
    int front;
    struct sockaddr_in target;
    struct { struct sockaddr_in addr; int fd; } cl[MAX_CLIENTS];
    int ncl;
    dir_t fwd, rev;
    item_t **heap; int hn; uint64_t order;
};

static double rnd(dir_t *d) {
    uint64_t x = d->s;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    d->s = x;
    return (double)((x * 0x2545F4914F6CDD1DULL) >> 11) * (1.0 / 9007199254740992.0);
}
static uint64_t rndu(dir_t *d) { return (uint64_t)(rnd(d) * 4294967296.0); }

static int before(item_t *a, item_t *b) { return a->when < b->when || (a->when == b->when && a->order < b->order); }
static void heap_push(proxy_t *p, item_t *it) {
    int i = p->hn++;
    p->heap[i] = it;
    while (i > 0) {
        int par = (i - 1) / 2;
        if (!before(p->heap[i], p->heap[par])) break;
        item_t *t = p->heap[i]; p->heap[i] = p->heap[par]; p->heap[par] = t; i = par;
    }
}
static item_t *heap_pop(proxy_t *p) {
    item_t *top = p->heap[0];
    p->heap[0] = p->heap[--p->hn];
    int i = 0;
    for (;;) {
        int l = 2 * i + 1, r = l + 1, m = i;
        if (l < p->hn && before(p->heap[l], p->heap[m])) m = l;
        if (r < p->hn && before(p->heap[r], p->heap[m])) m = r;
        if (m == i) break;
        item_t *t = p->heap[i]; p->heap[i] = p->heap[m]; p->heap[m] = t; i = m;
    }
    return top;
}

static void schedule(proxy_t *p, double when, const uint8_t *data, int len, int fd, const struct sockaddr_in *dst) {
    if (p->hn >= HEAP_MAX) return;
    item_t *it = malloc(sizeof *it);
    it->when = when; it->order = p->order++; it->fd = fd; it->len = len; it->dst = *dst;
    memcpy(it->data, data, len);
    heap_push(p, it);
}

static void push(proxy_t *p, dir_t *d, const uint8_t *data, int len, int fd, const struct sockaddr_in *dst) {
    const imp_cfg *c = &p->cfg;
    double now = rt_now();
    d->st.rx++;
    if (rnd(d) * 100 < c->loss) { d->st.dropped_loss++; return; }
    uint8_t buf[1600];
    memcpy(buf, data, len);
    if (len && rnd(d) * 100 < c->corrupt) {
        buf[rndu(d) % len] ^= (uint8_t)(1 + rndu(d) % 255);
        d->st.corrupted++;
    }
    int copies = 1;
    if (rnd(d) * 100 < c->dup) { copies = 2; d->st.dup++; }
    for (int k = 0; k < copies; k++) {
        double t = now;
        if (c->rate_mbps > 0) {
            double rate = c->rate_mbps * 1e6 / 8.0;
            double backlog = (d->link_free > now ? d->link_free - now : 0) * rate;
            if (backlog > c->queue * 1500.0) { d->st.dropped_queue++; continue; }   /* drop-tail */
            d->link_free = (d->link_free > now ? d->link_free : now) + len / rate;
            t = d->link_free;
        }
        double delay = c->delay_ms / 1000.0;
        if (c->jitter_ms) delay += (rnd(d) * 2 - 1) * c->jitter_ms / 1000.0;
        if (c->reorder && rnd(d) * 100 < c->reorder) { delay += c->reorder_ms / 1000.0; d->st.reordered++; }
        schedule(p, t + (delay > 0 ? delay : 0), buf, len, fd, dst);
    }
}

proxy_t *proxy_new(const char *bind_ip, int port, const struct sockaddr_in *target, const imp_cfg *cfg) {
    proxy_t *p = calloc(1, sizeof *p);
    p->cfg = *cfg; p->target = *target;
    p->fwd.s = cfg->seed * 0x9E3779B97F4A7C15ULL + 1;
    p->rev.s = (cfg->seed + 1000) * 0x9E3779B97F4A7C15ULL + 1;
    p->heap = malloc(HEAP_MAX * sizeof(item_t *));
    p->front = socket(AF_INET, SOCK_DGRAM, 0);
    int sz = 4 << 20;
    setsockopt(p->front, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
    struct sockaddr_in a = {.sin_family = AF_INET, .sin_port = htons(port)};
    a.sin_addr.s_addr = (bind_ip && *bind_ip) ? inet_addr(bind_ip) : htonl(INADDR_LOOPBACK);
    if (bind(p->front, (struct sockaddr *)&a, sizeof a) < 0) { close(p->front); free(p->heap); free(p); return NULL; }
    fcntl(p->front, F_SETFL, O_NONBLOCK);
    return p;
}

int proxy_port(proxy_t *p) {
    struct sockaddr_in a; socklen_t l = sizeof a;
    getsockname(p->front, (struct sockaddr *)&a, &l);
    return ntohs(a.sin_port);
}

void proxy_stats(proxy_t *p, imp_dir_stats *f, imp_dir_stats *r) { *f = p->fwd.st; *r = p->rev.st; }

void proxy_run(proxy_t *p, volatile int *stop) {
    static __thread uint8_t buf[2048];
    while (!*stop) {
        struct pollfd pf[1 + MAX_CLIENTS];
        pf[0] = (struct pollfd){.fd = p->front, .events = POLLIN};
        for (int i = 0; i < p->ncl; i++) pf[1 + i] = (struct pollfd){.fd = p->cl[i].fd, .events = POLLIN};
        double now = rt_now();
        int ms = 20;
        if (p->hn) { int w = (int)((p->heap[0]->when - now) * 1000.0 + 0.999); if (w < ms) ms = w < 0 ? 0 : w; }
        poll(pf, 1 + p->ncl, ms);
        for (int k = 0; k < 256; k++) {                       /* client -> target */
            struct sockaddr_in from; socklen_t fl = sizeof from;
            ssize_t n = recvfrom(p->front, buf, sizeof buf, 0, (struct sockaddr *)&from, &fl);
            if (n < 0) break;
            int ci = -1;
            for (int i = 0; i < p->ncl; i++)
                if (p->cl[i].addr.sin_port == from.sin_port && p->cl[i].addr.sin_addr.s_addr == from.sin_addr.s_addr) { ci = i; break; }
            if (ci < 0 && p->ncl < MAX_CLIENTS) {
                int fd = socket(AF_INET, SOCK_DGRAM, 0);
                int sz = 4 << 20;
                setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &sz, sizeof sz);
                fcntl(fd, F_SETFL, O_NONBLOCK);
                p->cl[p->ncl].addr = from; p->cl[p->ncl].fd = fd; ci = p->ncl++;
            }
            if (ci >= 0) push(p, &p->fwd, buf, (int)n, p->cl[ci].fd, &p->target);
        }
        for (int i = 0; i < p->ncl; i++)                       /* target -> client */
            for (int k = 0; k < 256; k++) {
                ssize_t n = recvfrom(p->cl[i].fd, buf, sizeof buf, 0, NULL, NULL);
                if (n < 0) break;
                push(p, &p->rev, buf, (int)n, p->front, &p->cl[i].addr);
            }
        now = rt_now();
        while (p->hn && p->heap[0]->when <= now) {
            item_t *it = heap_pop(p);
            sendto(it->fd, it->data, it->len, 0, (struct sockaddr *)&it->dst, sizeof it->dst);
            free(it);
        }
    }
}

void proxy_free(proxy_t *p) {
    if (!p) return;
    while (p->hn) free(heap_pop(p));
    for (int i = 0; i < p->ncl; i++) close(p->cl[i].fd);
    close(p->front);
    free(p->heap);
    free(p);
}

/* Named network scenarios shared by the CLI (--preset) and the web UI. */
static const struct { const char *name; imp_cfg c; } PRESETS[] = {
    {"clean",      {.loss = 0}},
    {"lossy",      {.loss = 5}},
    {"wifi",       {.loss = 1, .reorder = 2, .delay_ms = 3, .jitter_ms = 3}},
    {"mobile",     {.loss = 2, .delay_ms = 40, .jitter_ms = 15, .rate_mbps = 10, .queue = 60}},
    {"satellite",  {.loss = 0.5, .delay_ms = 150, .rate_mbps = 8, .queue = 100}},
    {"chaos",      {.reorder = 10, .dup = 10, .corrupt = 2}},
    {"bottleneck", {.delay_ms = 10, .rate_mbps = 20, .queue = 30}},
    {"wan",        {.loss = 3, .delay_ms = 15}},
};
const char *imp_preset_names(void) { return "clean, lossy, wifi, mobile, satellite, chaos, bottleneck, wan"; }
int imp_preset(const char *name, imp_cfg *out) {
    for (size_t k = 0; k < sizeof PRESETS / sizeof PRESETS[0]; k++)
        if (!strcmp(PRESETS[k].name, name)) {
            imp_cfg_default(out);
            imp_cfg p = PRESETS[k].c;
            out->loss = p.loss; out->dup = p.dup; out->reorder = p.reorder; out->corrupt = p.corrupt;
            out->delay_ms = p.delay_ms; out->jitter_ms = p.jitter_ms; out->rate_mbps = p.rate_mbps;
            if (p.queue) out->queue = p.queue;
            return 0;
        }
    return -1;
}
int imp_preset_at(int idx, const char **name, imp_cfg *c) {
    if (idx < 0 || idx >= (int)(sizeof PRESETS / sizeof PRESETS[0])) return -1;
    *name = PRESETS[idx].name;
    return imp_preset(PRESETS[idx].name, c);
}
