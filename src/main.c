/* main.c - CLI:  rtp <command> [options]   (run `rtp help`) */
#define _GNU_SOURCE
#include "harness.h"
#include "plot.h"
#include "rtp.h"
#include "ui.h"
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* ------------------------------------------------------------ small helpers */
static int g_color;
#define CB   (g_color ? "\033[1m" : "")
#define CD   (g_color ? "\033[2m" : "")
#define CG   (g_color ? "\033[32m" : "")
#define CR   (g_color ? "\033[31m" : "")
#define CY   (g_color ? "\033[33m" : "")
#define CC_  (g_color ? "\033[36m" : "")
#define C0   (g_color ? "\033[0m" : "")
#define U64(x) ((unsigned long long)(x))

static const char *opt(int argc, char **argv, const char *name, const char *def) {
    for (int i = 2; i < argc - 1; i++) if (!strcmp(argv[i], name)) return argv[i + 1];
    return def;
}
static int flag(int argc, char **argv, const char *name) {
    for (int i = 2; i < argc; i++) if (!strcmp(argv[i], name)) return 1;
    return 0;
}
static int has(int argc, char **argv, const char *name) { return opt(argc, argv, name, NULL) != NULL; }

static void usage(void) {
    printf("%sudp→tcp engine%s  reliable transport built from scratch on UDP (C)\n\n", CB, C0);
    printf("%sEasiest start%s\n  ./rtp ui --open           web dashboard: pick a network, run a transfer, watch it\n"
           "  ./rtp demo                full scripted demo in the terminal (+ SVG graphs and report.html)\n"
           "  ./rtp selftest            unit tests + transfers under impairment\n\n", CB, C0);
    printf("%sThree-terminal demo%s\n  ./rtp server --port 9000 --out recv.bin --once\n"
           "  ./rtp proxy  --listen 9001 --target 127.0.0.1:9000 --preset lossy\n"
           "  ./rtp client --connect 127.0.0.1:9001 --size 2000000\n\n", CB, C0);
    printf("%sCommands%s\n"
           "  server    --port P [--bind IP] [--out FILE] [--once] [--slow KB/s] [--trace CSV]\n"
           "  client    --connect HOST:PORT (--file F | --size N) [--trace CSV]\n"
           "  proxy     --listen P --target HOST:PORT [--preset NAME] [--loss %% --dup %% --reorder %% --corrupt %%\n"
           "            --delay ms --jitter ms --rate Mbps --queue pkts --seed N]\n"
           "  impair    apply|show|clear [--dev lo] [same flags]    real kernel impairment via `tc netem` (root)\n"
           "  bench     [--size N] [--losses 0,1,2,5] [--delay ms] [--netem DEV] [--csv F] [--svg F]\n"
           "  fairness  [--flows 3] [--size N] [--preset bottleneck]   several flows share one bottleneck\n"
           "  streams   [--streams 4] [--size N] [--preset wan]      head-of-line blocking: one ordered stream vs independent streams\n"
           "  plot      TRACE.csv [OUT.svg]                          cwnd graph from a --trace file\n"
           "  ui | demo | selftest | help\n\n", CB, C0);
    printf("%sCommon options%s  --cc reno|cubic   -v (log state changes & loss events)   --no-tlp --no-rack   --min-rto ms\n"
           "                  --fec N | --fec auto   (forward error correction: 1 parity packet per N, or self-tuning)\n", CB, C0);
    printf("%sPresets%s  %s\n", CB, C0, imp_preset_names());
}

static void load_cfg(int argc, char **argv, rt_cfg *cfg) {
    rt_cfg_default(cfg);
    cfg->cc = !strcmp(opt(argc, argv, "--cc", "reno"), "cubic") ? CC_CUBIC : CC_RENO;
    cfg->verbose = flag(argc, argv, "-v") || flag(argc, argv, "--verbose");
    cfg->mss = atoi(opt(argc, argv, "--mss", "1400"));
    if (flag(argc, argv, "--no-tlp")) cfg->tlp = 0;
    if (flag(argc, argv, "--no-rack")) cfg->rack = 0;
    if (has(argc, argv, "--fec")) {                      /* --fec N (parity per N packets, 1-16) or --fec auto */
        const char *f = opt(argc, argv, "--fec", "0");
        if (!strcmp(f, "auto")) cfg->fec_auto = 1; else cfg->fec = atoi(f);
    }
    if (has(argc, argv, "--tlp-min")) cfg->tlp_min = atof(opt(argc, argv, "--tlp-min", "4")) / 1000.0;
    if (has(argc, argv, "--min-rto")) cfg->min_rto = atof(opt(argc, argv, "--min-rto", "20")) / 1000.0;
}

static void load_imp(int argc, char **argv, imp_cfg *c, const char *default_preset) {
    imp_cfg_default(c);
    const char *pn = opt(argc, argv, "--preset", default_preset);
    if (pn && imp_preset(pn, c) < 0) {
        fprintf(stderr, "%sunknown preset '%s'%s. available: %s\n", CR, pn, C0, imp_preset_names());
        exit(2);
    }
    if (has(argc, argv, "--loss")) c->loss = atof(opt(argc, argv, "--loss", "0"));
    if (has(argc, argv, "--dup")) c->dup = atof(opt(argc, argv, "--dup", "0"));
    if (has(argc, argv, "--reorder")) c->reorder = atof(opt(argc, argv, "--reorder", "0"));
    if (has(argc, argv, "--corrupt")) c->corrupt = atof(opt(argc, argv, "--corrupt", "0"));
    if (has(argc, argv, "--delay")) c->delay_ms = atof(opt(argc, argv, "--delay", "0"));
    if (has(argc, argv, "--jitter")) c->jitter_ms = atof(opt(argc, argv, "--jitter", "0"));
    if (has(argc, argv, "--rate")) c->rate_mbps = atof(opt(argc, argv, "--rate", "0"));
    if (has(argc, argv, "--queue")) c->queue = atoi(opt(argc, argv, "--queue", "100"));
    if (has(argc, argv, "--seed")) c->seed = (uint64_t)atoll(opt(argc, argv, "--seed", "1"));
}

static void print_summary(const char *title, const rt_stats *s, size_t nbytes, const char *sha, double dur) {
    printf("\n%s==== %s ====%s\n", CB, title, C0);
    printf("  bytes            : %zu\n", nbytes);
    printf("  duration         : %.3f s   throughput: %s%.2f Mbps%s\n", dur, CB, nbytes * 8.0 / (dur > 1e-9 ? dur : 1e-9) / 1e6, C0);
    printf("  SHA-256          : %s\n", sha);
    printf("  packets tx/rx    : %llu / %llu\n", U64(s->packets_sent), U64(s->packets_received));
    printf("  retransmissions  : %llu  (fast: %llu, RTO: %llu, tail-loss probes: %llu, RACK: %llu, spurious: %llu)\n", U64(s->retransmissions),
           U64(s->fast_retransmits), U64(s->timeouts), U64(s->tlp_probes), U64(s->rack_retx), U64(s->spurious));
    printf("  dup/ooo packets  : %llu / %llu   checksum failures: %llu   zero-window probes: %llu\n", U64(s->dup_packets),
           U64(s->ooo_packets), U64(s->checksum_failures), U64(s->window_probes));
    printf("  cc=%s  cwnd=%d  ssthresh=%d  SRTT=%.2f ms  RTO=%.0f ms\n", s->cc, s->cwnd, s->ssthresh, s->srtt_ms, s->rto_ms);
    fflush(stdout);
}

/* "which packets needed repair" strip: one cell per group of segments, worst case in the group */
static void print_repair_map(const uint8_t *h, size_t n) {
    size_t repaired = 0, multi = 0;
    for (size_t i = 0; i < n; i++) { if (h[i]) repaired++; if (h[i] > 1) multi++; }
    if (!n || !repaired) return;
    size_t cells = n < 96 ? n : 96, per = (n + cells - 1) / cells;
    printf("\n  %spacket repair map%s  (%zu segments, 1 cell = %zu)\n  ", CB, C0, n, per);
    for (size_t i = 0; i < n; i += per) {
        int worst = 0;
        for (size_t k = i; k < i + per && k < n; k++) if (h[k] > worst) worst = h[k];
        if (worst == 0) printf("%s·%s", CD, C0);
        else if (worst == 1) printf("%so%s", CY, C0);
        else printf("%sO%s", CR, C0);
    }
    printf("\n  %s·%s delivered first time   %so%s repaired once   %sO%s repaired 2+ times   =>  %zu of %zu segments (%.1f%%) needed repair, %zu more than once\n",
           CD, C0, CY, C0, CR, C0, repaired, n, 100.0 * repaired / n, multi);
}

/* one-line live progress (only when stderr is a terminal and -v is off) */
static int g_prog_on;
static double g_prog_last;
static void progress(const char *what, double frac, double mbps, const char *extra) {
    double now = rt_now();
    if (!g_prog_on || now - g_prog_last < 0.08) return;
    g_prog_last = now;
    char bar[33];
    int fill = frac < 0 ? 0 : (int)(frac * 32 + 0.5);
    for (int i = 0; i < 32; i++) bar[i] = i < fill ? '#' : '-';
    bar[32] = 0;
    if (frac >= 0) fprintf(stderr, "\r  %s [%s%s%s] %3.0f%%  %7.1f Mbps  %s\033[K", what, CC_, bar, C0, frac * 100, mbps, extra);
    else fprintf(stderr, "\r  %s %7.1f Mbps  %s\033[K", what, mbps, extra);
    fflush(stderr);
}
static void progress_end(void) { if (g_prog_on) { fprintf(stderr, "\r\033[K"); fflush(stderr); } }

/* ------------------------------------------------------------ server */
static int cmd_server(int argc, char **argv) {
    rt_cfg cfg; load_cfg(argc, argv, &cfg);
    int port = atoi(opt(argc, argv, "--port", "9000"));
    rt_ep *ep = rt_ep_new(opt(argc, argv, "--bind", "0.0.0.0"), port, &cfg);
    if (!ep) { fprintf(stderr, "%scannot bind UDP port %d: %s%s\n(is another server already using it? try --port %d)\n", CR, port, strerror(errno), C0, port + 1); return 1; }
    rt_listen(ep);
    printf("%slistening%s on UDP port %d  (cc=%s, tail-loss-probe %s)\n", CG, C0, rt_ep_port(ep), cfg.cc == CC_CUBIC ? "cubic" : "reno", cfg.tlp ? "on" : "off");
    fflush(stdout);
    const char *out = opt(argc, argv, "--out", NULL), *trace = opt(argc, argv, "--trace", NULL);
    int once = flag(argc, argv, "--once"), slow = atoi(opt(argc, argv, "--slow", "0"));
    g_prog_on = isatty(2) && !cfg.verbose;
    if (slow) printf("%sslow receiver mode:%s reading at most %d KB/s (watch flow control in action)\n", CY, C0, slow);
    static uint8_t buf[65536];
    for (;;) {
        rt_conn *c;
        while (!(c = rt_accept(ep))) rt_run_once(ep, 50);
        struct sockaddr_in pa; rt_conn_peer(c, &pa);
        printf("accepted connection %08x from %s:%d\n", rt_conn_id(c), inet_ntoa(pa.sin_addr), ntohs(pa.sin_port));
        fflush(stdout);
        FILE *f = out ? fopen(out, "wb") : NULL;
        if (out && !f) { fprintf(stderr, "%scannot write %s: %s%s\n", CR, out, strerror(errno), C0); return 1; }
        sha256_ctx h; sha256_init(&h);
        size_t total = 0;
        double t0 = rt_now();
        for (;;) {
            size_t cap = sizeof buf;
            if (slow) {                                           /* token bucket: reader limited to `slow` KB/s */
                double allowed = slow * 1000.0 * (rt_now() - t0);
                size_t budget = allowed > (double)total ? (size_t)(allowed - (double)total) : 0;
                if (budget == 0) { rt_run_once(ep, 2); continue; }
                if (budget < cap) cap = budget;
            }
            int n = rt_recv(c, buf, cap);
            if (n > 0) {
                sha256_update(&h, buf, n); total += n; if (f) fwrite(buf, 1, n, f);
                char ex[64]; snprintf(ex, sizeof ex, "%zu KB received", total / 1000);
                progress("receiving", -1, total * 8.0 / (rt_now() - t0 + 1e-9) / 1e6, ex);
                continue;
            }
            if (n < 0) { if (n == -2) fprintf(stderr, "%sconnection error (%s)%s\n", CR, strerror(rt_conn_error(c)), C0); break; }
            rt_run_once(ep, 20);
        }
        progress_end();
        if (f) fclose(f);
        double dur = rt_now() - t0;
        char sha[65]; sha256_final(&h, sha);
        rt_close(c);
        double dl = rt_now() + 5;
        while (rt_conn_state(c) != ST_CLOSED && rt_now() < dl) rt_run_once(ep, 10);
        rt_stats st; rt_stats_get(c, &st);
        print_summary("server transfer summary", &st, total, sha, dur);
        if (trace) { rt_trace_write(c, trace); printf("  trace written    : %s\n", trace); }
        rt_conn_free(c);
        if (once) break;
    }
    rt_ep_free(ep);
    return 0;
}

/* ------------------------------------------------------------ client */
static int cmd_client(int argc, char **argv) {
    rt_cfg cfg; load_cfg(argc, argv, &cfg);
    const char *to = opt(argc, argv, "--connect", NULL);
    if (!to) { fprintf(stderr, "%smissing --connect HOST:PORT%s\n", CR, C0); return 2; }
    struct sockaddr_in sa;
    if (rt_resolve(to, &sa) < 0) { fprintf(stderr, "%scannot resolve '%s' (expected HOST:PORT)%s\n", CR, to, C0); return 1; }
    uint8_t *data; size_t len;
    const char *file = opt(argc, argv, "--file", NULL);
    if (file) {
        FILE *f = fopen(file, "rb");
        if (!f) { fprintf(stderr, "%scannot open %s: %s%s\n", CR, file, strerror(errno), C0); return 1; }
        fseek(f, 0, SEEK_END); len = ftell(f); fseek(f, 0, SEEK_SET);
        data = malloc(len ? len : 1);
        if (fread(data, 1, len, f) != len) { perror("read"); return 1; }
        fclose(f);
    } else {
        len = (size_t)atoll(opt(argc, argv, "--size", "1000000"));
        data = malloc(len ? len : 1);
        make_data(data, len, (uint64_t)atoll(opt(argc, argv, "--seed", "0")));
    }
    char sha[65]; sha256_ctx h; sha256_init(&h); sha256_update(&h, data, len); sha256_final(&h, sha);
    rt_ep *ep = rt_ep_new(NULL, 0, &cfg);
    rt_conn *c = rt_connect(ep, &sa);
    double timeout = atof(opt(argc, argv, "--timeout", "10")), dl = rt_now() + timeout;
    while (rt_conn_state(c) != ST_ESTABLISHED && rt_conn_state(c) != ST_CLOSED && rt_now() < dl) rt_run_once(ep, 5);
    if (rt_conn_state(c) != ST_ESTABLISHED) {
        int e = rt_conn_error(c);
        fprintf(stderr, "%scould not connect to %s%s: %s\nIs the server running, and is the port right? (if you use the proxy, connect to the proxy's port)\n",
                CR, to, C0, e == ECONNRESET ? "connection refused (reset by peer)" : "no answer (timed out)");
        return 1;
    }
    g_prog_on = isatty(2) && !cfg.verbose;
    double t0 = rt_now();
    size_t off = 0;
    char ex[96];
    while (off < len) {
        int n = rt_send(c, data + off, len - off);
        if (n < 0) { progress_end(); fprintf(stderr, "%ssend failed: %s%s\n", CR, strerror(rt_conn_error(c) ? rt_conn_error(c) : EPIPE), C0); return 1; }
        off += n;
        rt_run_once(ep, n > 0 ? 0 : 2);
        if (g_prog_on) { rt_stats s; rt_stats_get(c, &s); snprintf(ex, sizeof ex, "cwnd %d seg  rtt %.1fms  retx %llu", s.cwnd / cfg.mss, s.srtt_ms, U64(s.retransmissions));
            double acked = (double)rt_bytes_acked(c); progress("sending  ", len ? acked / len : 1, acked * 8 / (rt_now() - t0 + 1e-9) / 1e6, ex); }
    }
    while (!rt_flushed(c) && rt_conn_state(c) != ST_CLOSED) {
        rt_run_once(ep, 2);
        if (g_prog_on) { rt_stats s; rt_stats_get(c, &s); snprintf(ex, sizeof ex, "cwnd %d seg  rtt %.1fms  retx %llu", s.cwnd / cfg.mss, s.srtt_ms, U64(s.retransmissions));
            double acked = (double)rt_bytes_acked(c); progress("sending  ", len ? acked / len : 1, acked * 8 / (rt_now() - t0 + 1e-9) / 1e6, ex); }
        if (rt_now() - t0 > 600) break;
    }
    progress_end();
    if (rt_conn_error(c)) { fprintf(stderr, "%stransfer failed: %s%s\n", CR, strerror(rt_conn_error(c)), C0); return 1; }
    double dur = rt_now() - t0;
    rt_close(c);
    dl = rt_now() + 5;
    while (rt_conn_state(c) != ST_CLOSED && rt_now() < dl) rt_run_once(ep, 5);
    rt_stats st; rt_stats_get(c, &st);
    print_summary("client transfer summary", &st, len, sha, dur);
    { const uint8_t *hs; size_t ns = rt_segment_history(c, &hs); print_repair_map(hs, ns); }
    const char *trace = opt(argc, argv, "--trace", NULL);
    if (trace) { rt_trace_write(c, trace); printf("  trace written    : %s   (graph it: ./rtp plot %s)\n", trace, trace); }
    return 0;
}

/* ------------------------------------------------------------ proxy */
static volatile int g_stop;
static void on_sigint(int s) { (void)s; g_stop = 1; }

static void print_dir(const char *n, const imp_dir_stats *d) {
    printf("%s: seen=%llu lost=%llu queue-drops=%llu duplicated=%llu reordered=%llu corrupted=%llu\n", n, U64(d->rx),
           U64(d->dropped_loss), U64(d->dropped_queue), U64(d->dup), U64(d->reordered), U64(d->corrupted));
}

static int cmd_proxy(int argc, char **argv) {
    imp_cfg ic; load_imp(argc, argv, &ic, NULL);
    struct sockaddr_in target;
    const char *ts = opt(argc, argv, "--target", "127.0.0.1:9000");
    if (rt_resolve(ts, &target) < 0) { fprintf(stderr, "%sbad --target '%s' (expected HOST:PORT)%s\n", CR, ts, C0); return 1; }
    int lp = atoi(opt(argc, argv, "--listen", "9001"));
    proxy_t *p = proxy_new(opt(argc, argv, "--bind", "127.0.0.1"), lp, &target, &ic);
    if (!p) { fprintf(stderr, "%scannot bind UDP port %d: %s%s\n", CR, lp, strerror(errno), C0); return 1; }
    char d[256]; imp_describe(&ic, d, sizeof d);
    printf("%simpairment proxy%s  port %d -> %s\n  network: %s%s%s\n  (Ctrl+C to stop and print counters)\n", CB, C0, proxy_port(p), ts, CY, d, C0);
    fflush(stdout);
    signal(SIGINT, on_sigint);
    proxy_run(p, &g_stop);
    imp_dir_stats f, r; proxy_stats(p, &f, &r);
    printf("\n"); print_dir("client->server", &f); print_dir("server->client", &r);
    proxy_free(p);
    return 0;
}

/* ------------------------------------------------------------ tc netem (real kernel impairment) */
static int safe_dev(const char *d) { if (!*d) return 0; for (; *d; d++) if (!isalnum((unsigned char)*d) && *d != '_' && *d != '.' && *d != '-') return 0; return 1; }

static int run_cmd(const char *cmd) {
    printf("%s+ %s%s\n", CD, cmd, C0);
    int rc = system(cmd);
    if (rc != 0) fprintf(stderr, "%scommand failed.%s `tc netem` needs Linux, iproute2 and root: try `sudo ./rtp impair ...`\n", CR, C0);
    return rc;
}

static int netem_apply(const char *dev, const imp_cfg *c) {
    char cmd[512]; int n = snprintf(cmd, sizeof cmd, "tc qdisc replace dev %s root netem", dev);
    double delay = c->delay_ms;
    if (c->reorder && !delay) delay = 10;                      /* netem reordering needs a delay */
    if (delay) n += snprintf(cmd + n, sizeof cmd - n, " delay %gms%s", delay, c->jitter_ms ? "" : "");
    if (delay && c->jitter_ms) n += snprintf(cmd + n, sizeof cmd - n, " %gms", c->jitter_ms);
    if (c->loss) n += snprintf(cmd + n, sizeof cmd - n, " loss %g%%", c->loss);
    if (c->dup) n += snprintf(cmd + n, sizeof cmd - n, " duplicate %g%%", c->dup);
    if (c->corrupt) n += snprintf(cmd + n, sizeof cmd - n, " corrupt %g%%", c->corrupt);
    if (c->reorder) n += snprintf(cmd + n, sizeof cmd - n, " reorder %g%% 50%%", c->reorder);
    if (c->rate_mbps) n += snprintf(cmd + n, sizeof cmd - n, " rate %gmbit", c->rate_mbps);
    return run_cmd(cmd);
}

static int cmd_impair(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: rtp impair apply|show|clear [--dev lo] [flags]\n"); return 2; }
    const char *act = argv[2], *dev = opt(argc, argv, "--dev", "lo");
    if (!safe_dev(dev)) { fprintf(stderr, "bad --dev\n"); return 2; }
    char cmd[128];
    if (!strcmp(act, "apply")) { imp_cfg ic; load_imp(argc, argv, &ic, NULL); return netem_apply(dev, &ic) ? 1 : 0; }
    if (!strcmp(act, "show")) { snprintf(cmd, sizeof cmd, "tc qdisc show dev %s", dev); return run_cmd(cmd) ? 1 : 0; }
    if (!strcmp(act, "clear")) { snprintf(cmd, sizeof cmd, "tc qdisc del dev %s root", dev); return run_cmd(cmd) ? 1 : 0; }
    fprintf(stderr, "unknown action '%s'\n", act);
    return 2;
}

/* ------------------------------------------------------------ plot */
static int cmd_plot(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: rtp plot TRACE.csv [OUT.svg]\n"); return 2; }
    char out[512];
    if (argc > 3) snprintf(out, sizeof out, "%s", argv[3]);
    else { snprintf(out, sizeof out, "%s", argv[2]); char *dot = strrchr(out, '.'); if (dot) *dot = 0; strcat(out, ".svg"); }
    if (svg_cwnd_from_csv(argv[2], out, 1400) < 0) { fprintf(stderr, "%scannot plot %s (missing or empty trace)%s\n", CR, argv[2], C0); return 1; }
    printf("wrote %s  (open it in a browser)\n", out);
    return 0;
}

/* ------------------------------------------------------------ fairness */
static void print_fair(const fair_res *f) {
    for (int i = 0; i < f->flows; i++) printf("  flow %d : %7.2f Mbps  %s\n", i + 1, f->mbps[i], f->ok[i] ? "delivered intact" : "FAILED");
    printf("  total  : %7.2f Mbps   Jain fairness index %s%.3f%s  (1.000 = perfectly equal shares)\n", f->total_mbps, CB, f->jain, C0);
}

static int cmd_fairness(int argc, char **argv) {
    rt_cfg cfg; load_cfg(argc, argv, &cfg); cfg.trace = 0;
    imp_cfg ic; load_imp(argc, argv, &ic, "bottleneck");
    int flows = atoi(opt(argc, argv, "--flows", "3"));
    size_t size = (size_t)atoll(opt(argc, argv, "--size", "3000000"));
    char d[256]; imp_describe(&ic, d, sizeof d);
    printf("%sfairness test%s: %d simultaneous flows x %zu bytes through one bottleneck [%s]\n", CB, C0, flows, size, d);
    fair_res f;
    int rc = run_fairness(size, &ic, &cfg, flows, &f);
    print_fair(&f);
    return rc ? 1 : 0;
}

/* ------------------------------------------------------------ selftest */
static int g_pass, g_fail;
#define CHECK(cond, name) do { if (cond) { g_pass++; printf("  %s[PASS]%s %s\n", CG, C0, name); } else { g_fail++; printf("  %s[FAIL]%s %s\n", CR, C0, name); } } while (0)

static void unit_tests(void) {
    printf("%sUnit tests%s\n", CB, C0);
    rt_pkt p = {.conn_id = 0xDEADBEEF, .seq = 1000, .ack = 2000, .flags = F_ACK, .window = 65536, .nsack = 2};
    uint8_t pl[300]; make_data(pl, sizeof pl, 5);
    p.payload = pl; p.plen = sizeof pl;
    p.sack[0][0] = 10; p.sack[0][1] = 20; p.sack[1][0] = 30; p.sack[1][1] = 40;
    uint8_t wire[2048];
    size_t n = rt_pkt_encode(&p, wire);
    rt_pkt q;
    CHECK(rt_pkt_decode(wire, n, &q) == 0 && q.conn_id == p.conn_id && q.seq == 1000 && q.ack == 2000 &&
          q.window == 65536 && q.nsack == 2 && q.sack[1][1] == 40 && q.plen == 300 && !memcmp(q.payload, pl, 300),
          "codec round-trip with SACK + payload");
    rt_pkt e = {.conn_id = 1};
    CHECK(rt_pkt_encode(&e, wire) == RT_HDR_LEN, "base header is exactly 20 bytes");
    n = rt_pkt_encode(&p, wire);
    int detected = 0;
    for (size_t i = 0; i < n; i++) { uint8_t save = wire[i]; wire[i] ^= 0x5A; if (rt_pkt_decode(wire, n, &q) != 0) detected++; wire[i] = save; }
    CHECK(detected == (int)n, "every single-byte corruption is detected");
    CHECK(rt_pkt_decode(wire, 10, &q) < 0 && rt_pkt_decode(wire, n - 1, &q) < 0, "short / truncated packets rejected");
    rt_cc c; rt_cc_init(&c, CC_RENO, 1000, 2, 10000);
    for (int i = 0; i < 4; i++) rt_cc_on_ack(&c, 1000, 1, 0.01, 0);
    CHECK(c.cwnd == 6000, "Reno slow start: +1 MSS per ACK");
    c.cwnd = 20000; c.ssthresh = 10000;
    double before = c.cwnd;
    for (int i = 0; i < 20; i++) rt_cc_on_ack(&c, 1000, 1, 0.01, 0);
    CHECK(c.cwnd - before < 1100 && c.cwnd > before, "Reno congestion avoidance: ~1 MSS per RTT");
    rt_cc_on_fast_retransmit(&c, 20000, 0);
    CHECK(c.ssthresh == 10000, "Reno fast retransmit halves flight");
    rt_cc_on_recovery_exit(&c);
    CHECK(c.cwnd == 10000, "Reno fast recovery deflates to ssthresh");
    rt_cc_on_timeout(&c, 10000, 0);
    CHECK(c.cwnd == 1000 && c.ssthresh == 5000, "Reno RTO: cwnd=1 MSS, ssthresh=flight/2");
    rt_cc_init(&c, CC_CUBIC, 1000, 2, 1e18); c.cwnd = 100000;
    rt_cc_on_fast_retransmit(&c, 100000, 0);
    CHECK(c.ssthresh > 69999 && c.ssthresh < 70001, "CUBIC multiplicative decrease beta=0.7");
    char hex[65]; sha256_ctx h; sha256_init(&h); sha256_update(&h, "abc", 3); sha256_final(&h, hex);
    CHECK(!strcmp(hex, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"), "SHA-256 known-answer test");
    imp_cfg ic;
    CHECK(imp_preset("lossy", &ic) == 0 && ic.loss == 5 && imp_preset("nope", &ic) < 0, "network presets resolve");
    rt_trace_pt tp[3] = {{0, 2800, 1e9, 2800, 0, 0.1, 0, 0}, {0.1, 5600, 1e9, 5600, 0, 0.1, 0, 0}, {0.2, 9000, 4200, 9000, 0, 0.1, 0, 1}};
    CHECK(svg_cwnd("/tmp/rtp_selftest.svg", "t", tp, 3, NULL, 0, 1400) == 0 && access("/tmp/rtp_selftest.svg", R_OK) == 0, "SVG graph writer");
}

static void xfer_test(const char *name, size_t size, imp_cfg *imp, int cc, int expect_retx, int slow, uint32_t rbuf) {
    xfer_args a = {.size = size, .imp = imp, .seed = 7, .timeout = 60, .slow_kbps = slow};
    rt_cfg_default(&a.cfg); a.cfg.cc = cc; a.cfg.trace = 0;
    if (rbuf) a.cfg.recv_buf = rbuf;
    xfer_res r;
    int rc = run_transfer(&a, &r);
    char label[220];
    snprintf(label, sizeof label, "%-36s %7.2f Mbps retx=%llu (fast=%llu rto=%llu tlp=%llu) ooo=%llu dup=%llu probes=%llu %s", name, r.mbps,
             U64(r.cli.retransmissions), U64(r.cli.fast_retransmits), U64(r.cli.timeouts), U64(r.cli.tlp_probes),
             U64(r.srv.ooo_packets), U64(r.srv.dup_packets), U64(r.cli.window_probes), r.err);
    int paced = !slow || r.duration_s > 0.6 * (double)size / (slow * 1000.0);   /* sender really was held back by the receiver window */
    CHECK(rc == 0 && (!expect_retx || r.cli.retransmissions > 0) && r.srv.window_drops == 0 && paced, label);
}

static int cmd_selftest(void) {
    unit_tests();
    printf("\n%sLoopback transfers (SHA-256 verified)%s\n", CB, C0);
    imp_cfg c;
    xfer_test("clean 1 MB", 1000000, NULL, CC_RENO, 0, 0, 0);
    imp_preset("lossy", &c);      xfer_test("5% loss", 600000, &c, CC_RENO, 1, 0, 0);
    imp_preset("chaos", &c);      xfer_test("reorder + duplicate + corrupt", 600000, &c, CC_RENO, 0, 0, 0);
    imp_preset("bottleneck", &c); xfer_test("bottleneck 20 Mbps, Reno", 1500000, &c, CC_RENO, 0, 0, 0);
    xfer_test("bottleneck 20 Mbps, CUBIC", 1500000, &c, CC_CUBIC, 0, 0, 0);
    imp_preset("wifi", &c);       xfer_test("wifi-like (1% loss, jitter)", 600000, &c, CC_RENO, 0, 0, 0);
    {
        imp_preset("lossy", &c);
        xfer_args a = {.size = 600000, .imp = &c, .seed = 7, .timeout = 60, .capture = 1};
        rt_cfg_default(&a.cfg); a.cfg.rack = 0; a.cfg.tlp = 0; a.cfg.min_rto = 0.1;        /* "classic" recovery still works */
        xfer_res r; int rc = run_transfer(&a, &r);
        CHECK(rc == 0 && r.cli.rack_retx == 0 && r.cli.tlp_probes == 0, "classic recovery (RACK + TLP off) still delivers");
        xfer_res_free(&r);
        a.cfg.rack = 1; a.cfg.tlp = 1; a.cfg.min_rto = 0.02;
        rc = run_transfer(&a, &r);
        size_t rep = 0; for (size_t i = 0; i < r.nseg; i++) if (r.seg_hist[i]) rep++;
        char label[160]; snprintf(label, sizeof label, "repair map: %zu segments tracked, %zu repaired (RACK %llu, probes %llu)", r.nseg, rep, U64(r.cli.rack_retx), U64(r.cli.tlp_probes));
        CHECK(rc == 0 && r.nseg == (600000 + 1399) / 1400 && rep > 0, label);
        xfer_res_free(&r);
    }
    {
        imp_preset("lossy", &c);
        xfer_args a = {.size = 600000, .imp = &c, .seed = 7, .timeout = 60};
        rt_cfg_default(&a.cfg); a.cfg.fec = 8;
        xfer_res r; int rc = run_transfer(&a, &r);
        char label[170]; snprintf(label, sizeof label, "FEC (1 parity per 8): %llu parity sent, %llu lost packets rebuilt without retransmission",
                                  U64(r.cli.fec_sent), U64(r.srv.fec_recovered));
        CHECK(rc == 0 && r.cli.fec_sent > 0 && r.srv.fec_recovered > 0, label);
        a.cfg.fec = 0; a.cfg.fec_auto = 1; imp_cfg_default(&c); a.imp = &c; a.size = 1500000;
        rc = run_transfer(&a, &r);
        snprintf(label, sizeof label, "adaptive FEC on a clean link: delivers, ends with FEC %s (k=%d)", r.cli.fec_k ? "on" : "off", r.cli.fec_k);
        CHECK(rc == 0 && r.cli.fec_k == 0, label);
        a.cfg.fec_auto = 1; imp_preset("lossy", &c); a.size = 1500000;
        rc = run_transfer(&a, &r);
        snprintf(label, sizeof label, "adaptive FEC under 5%% loss: delivers, switched itself on (k=%d)", r.cli.fec_k);
        CHECK(rc == 0 && r.cli.fec_k > 0, label);
    }
    {
        rt_cfg cfg; rt_cfg_default(&cfg);
        imp_preset("chaos", &c);
        hol_res o, m2;
        int r1 = run_hol(120000, 4, &c, &cfg, 0, &o), r2 = run_hol(120000, 4, &c, &cfg, 1, &m2);
        char label[170]; snprintf(label, sizeof label, "4 independent streams under reorder/dup/corrupt: all intact (held back %.1f KB vs %.1f KB ordered)", m2.avg_held_kb, o.avg_held_kb);
        CHECK(r1 == 0 && r2 == 0, label);
        imp_preset("lossy", &c);
        r1 = run_hol(150000, 8, &c, &cfg, 1, &m2);
        CHECK(r1 == 0, "8 independent streams under 5% loss: every stream intact");
        rt_cfg c2 = cfg; c2.fec = 8;
        r1 = run_hol(150000, 4, &c, &c2, 1, &m2);
        CHECK(r1 == 0, "streams + FEC together under 5% loss: every stream intact");
    }
    imp_cfg_default(&c); c.loss = 50; c.delay_ms = 1;
    xfer_test("50% loss (handshake + data survive)", 20000, &c, CC_RENO, 1, 0, 0);
    xfer_test("slow receiver: flow control (16 KB buf)", 300000, NULL, CC_RENO, 0, 400, 16 * 1024);
    xfer_test("1-byte transfer", 1, NULL, CC_RENO, 0, 0, 0);
    xfer_test("zero-length transfer (FIN only)", 0, NULL, CC_RENO, 0, 0, 0);
    {
        imp_preset("bottleneck", &c);
        rt_cfg cfg; rt_cfg_default(&cfg); cfg.trace = 0;
        fair_res f;
        int rc = run_fairness(1200000, &c, &cfg, 2, &f);
        char label[160]; snprintf(label, sizeof label, "2 flows share a bottleneck (Jain %.2f, %.1f + %.1f Mbps)", f.jain, f.mbps[0], f.mbps[1]);
        CHECK(rc == 0 && f.jain > 0.5, label);
    }
    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}

/* ------------------------------------------------------------ streams: head-of-line blocking experiment */
static void hol_svg(const char *path, const char *title, const hol_res *r) {
    static const char *COL[8] = {"#0b7285", "#e8590c", "#5c940d", "#c08a00", "#495057", "#c92a2a", "#1971c2", "#8a5a2b"};
    static char names[8][16];
    double y[8][240];
    svg_series se[8];
    for (int i = 0; i < r->nstreams; i++) {
        snprintf(names[i], sizeof names[i], "stream %d", i + 1);
        for (int k = 0; k < r->ntl; k++) y[i][k] = r->tl_b[i][k] / 1000.0;
        se[i] = (svg_series){names[i], COL[i], r->ntl, r->tl_t, y[i], 0};
    }
    svg_chart(path, title, "time (s)  |  a flat stretch = that stream is frozen", "delivered to the app (KB)", se, r->nstreams, 0, NULL, NULL, 0);
}

static void hol_block_svg(const char *dir, const hol_res *o, const hol_res *m) {
    double yo[240], ym[240], xo[240], xm[240];
    for (int k = 0; k < o->ntl; k++) { xo[k] = o->tl_t[k]; yo[k] = o->tl_h[k] / 1000.0; }
    for (int k = 0; k < m->ntl; k++) { xm[k] = m->tl_t[k]; ym[k] = m->tl_h[k] / 1000.0; }
    svg_series se[2] = {{"one ordered stream", "#e8590c", o->ntl, xo, yo, 0}, {"independent streams", "#0b7285", m->ntl, xm, ym, 0}};
    char p[300]; snprintf(p, sizeof p, "%s/streams_blocking.svg", dir);
    svg_chart(p, "Data held back by head-of-line blocking", "time (s)", "received but not deliverable (KB)", se, 2, 0, NULL, NULL, 0);
}

static int by_held(const void *a, const void *b) {
    double x = ((const hol_res *)a)->avg_held_kb, y = ((const hol_res *)b)->avg_held_kb;
    return x < y ? -1 : x > y;
}

/* runs `reps` experiments per mode, returns the median run (by held-back data) of each */
static int hol_compare(int n, size_t per, const imp_cfg *ic, const rt_cfg *cfg, int reps, hol_res *ord, hol_res *mul) {
    hol_res a[9], b[9];
    int bad = 0;
    if (reps > 9) reps = 9;
    for (int k = 0; k < reps; k++) {
        imp_cfg x = *ic; x.seed = ic->seed + k;
        if (run_hol(per, n, &x, cfg, 0, &a[k])) bad++;
        if (run_hol(per, n, &x, cfg, 1, &b[k])) bad++;
    }
    qsort(a, reps, sizeof a[0], by_held);
    qsort(b, reps, sizeof b[0], by_held);
    *ord = a[reps / 2]; *mul = b[reps / 2];
    return bad;
}

static void hol_report(const hol_res *o, const hol_res *m, size_t per, const imp_cfg *ic) {
    char d[256]; imp_describe(ic, d, sizeof d);
    printf("%sHead-of-line blocking experiment%s  %d streams x %zu bytes, network [%s]\n", CB, C0, o->nstreams, per, d);
    printf("  %-44s %-30s %-24s\n", "", "ONE ordered stream (TCP-like)", "INDEPENDENT streams");
    printf("  %-44s %-30s %-24s\n", "all data intact (SHA-256 per stream)", o->ok ? "yes" : "NO", m->ok ? "yes" : "NO");
    printf("  %-44s %-30.2f %-24.2f\n", "time until every stream finished (s)", o->duration_s, m->duration_s);
    printf("  %-44s %s%-30.1f %-24.1f%s\n", "average data held back, already received (KB)", CB, o->avg_held_kb, m->avg_held_kb, C0);
    printf("  %-44s %-30.1f %-24.1f\n", "peak data held back (KB)", o->peak_held_kb, m->peak_held_kb);
    printf("  %-44s %-30.0f %-24.0f\n", "share of time something was held back (%)", o->blocked_frac * 100, m->blocked_frac * 100);
    if (m->avg_held_kb > 0 && o->avg_held_kb > 0)
        printf("  => independent streams held back %s%.1fx less data%s: a loss only blocks the stream it hit\n", CG, o->avg_held_kb / m->avg_held_kb, C0);
    printf("  (held back = bytes that arrived but could not be handed to the application yet because earlier data was missing)\n");
}

static int cmd_streams(int argc, char **argv) {
    rt_cfg cfg; load_cfg(argc, argv, &cfg);
    imp_cfg ic; load_imp(argc, argv, &ic, "wan");
    int n = atoi(opt(argc, argv, "--streams", "4")); if (n < 2) n = 2; if (n > 8) n = 8;
    size_t per = (size_t)atoll(opt(argc, argv, "--size", "600000"));
    int reps = atoi(opt(argc, argv, "--repeats", "3")); if (reps < 1) reps = 1;
    const char *dir = opt(argc, argv, "--out", ".");
    mkdir(dir, 0755);
    hol_res o, m;
    int bad = hol_compare(n, per, &ic, &cfg, reps, &o, &m);
    hol_report(&o, &m, per, &ic);
    char p1[300], p2[300];
    snprintf(p1, sizeof p1, "%s/streams_ordered.svg", dir); snprintf(p2, sizeof p2, "%s/streams_independent.svg", dir);
    hol_svg(p1, "One ordered stream: delivery of ALL streams stalls together", &o);
    hol_svg(p2, "Independent streams: each stream stalls only when it loses data", &m);
    hol_block_svg(dir, &o, &m);
    printf("  graphs in %s/: streams_ordered.svg, streams_independent.svg, streams_blocking.svg  (median of %d runs per mode)\n", dir, reps);
    return bad ? 1 : 0;
}

/* ------------------------------------------------------------ bench */
static double median(double *v, int n) {
    for (int i = 1; i < n; i++) { double x = v[i]; int j = i - 1; while (j >= 0 && v[j] > x) { v[j + 1] = v[j]; j--; } v[j + 1] = x; }
    return n % 2 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
}

static int cmd_bench(int argc, char **argv) {
    size_t size = (size_t)atoll(opt(argc, argv, "--size", "1000000"));
    double delay = atof(opt(argc, argv, "--delay", "0"));
    const char *csvp = opt(argc, argv, "--csv", "bench_loss_sweep.csv"), *svgp = opt(argc, argv, "--svg", "bench_loss_sweep.svg");
    const char *netem = opt(argc, argv, "--netem", NULL);
    rt_cfg cfg; load_cfg(argc, argv, &cfg); cfg.trace = 0;
    if (netem && !safe_dev(netem)) { fprintf(stderr, "bad --netem device\n"); return 2; }
    char losses[256]; snprintf(losses, sizeof losses, "%s", opt(argc, argv, "--losses", "0,0.5,1,2,5"));
    int reps = atoi(opt(argc, argv, "--repeats", "3")); if (reps < 1) reps = 1; if (reps > 15) reps = 15;
    FILE *csv = fopen(csvp, "w");
    if (!csv) { fprintf(stderr, "%scannot write %s%s\n", CR, csvp, C0); return 1; }
    fprintf(csv, "loss_pct,delay_ms,cc,udp_throughput_mbps,udp_ok,udp_srtt_ms,udp_retx,udp_timeouts,tcp_throughput_mbps\n");
    printf("%sloss sweep%s: size=%zu B, delay=%.1f ms, cc=%s, mode=%s\n", CB, C0, size, delay, cfg.cc == CC_CUBIC ? "cubic" : "reno",
           netem ? "tc/netem on device (BOTH protocols impaired)" : "UDP proxy (native TCP shown on clean link only)");
    double xs[32], ys[32], tx[32], ty[32]; int nx = 0, nt = 0;
    for (char *tok = strtok(losses, ","); tok && nx < 32; tok = strtok(NULL, ",")) {
        double loss = atof(tok);
        imp_cfg ic; imp_cfg_default(&ic); ic.loss = loss; ic.delay_ms = delay;
        double tcp = 0; int have_tcp = 0, allok = 1;
        double mb[16], tc_[16]; int ntc = 0;
        xfer_res r = {0};
        if (netem) { if (netem_apply(netem, &ic)) { fclose(csv); return 1; } }
        for (int k = 0; k < reps; k++) {
            ic.seed = 1 + k;
            xfer_args a = {.size = size, .imp = netem ? NULL : &ic, .seed = 1, .timeout = 120, .cfg = cfg};
            run_transfer(&a, &r);
            mb[k] = r.mbps; allok &= r.ok;
            double t1;
            if ((netem || (loss == 0 && delay == 0)) && tcp_transfer(size, 1, &t1) == 0) tc_[ntc++] = t1;
        }
        if (ntc) { tcp = median(tc_, ntc); have_tcp = 1; }
        r.mbps = median(mb, reps); r.ok = allok;
        double lo = mb[0], hi = mb[reps - 1];            /* median() sorted the array in place */
        if (netem) { char cmd[128]; snprintf(cmd, sizeof cmd, "tc qdisc del dev %s root >/dev/null 2>&1", netem); if (system(cmd)) {} }
        printf("  loss=%5.1f%%  engine %s%8.2f Mbps%s (median of %d, range %.0f-%.0f)  ok=%d  retx=%llu  RTO=%llu  tlp=%llu  srtt=%.1fms", loss, CB, r.mbps, C0, reps, lo, hi, r.ok,
               U64(r.cli.retransmissions), U64(r.cli.timeouts), U64(r.cli.tlp_probes), r.cli.srtt_ms);
        if (have_tcp) printf("  | native TCP %8.2f Mbps", tcp);
        printf("\n"); fflush(stdout);
        fprintf(csv, "%g,%g,%s,%.3f,%d,%.3f,%llu,%llu,", loss, delay, cfg.cc == CC_CUBIC ? "cubic" : "reno", r.mbps, r.ok, r.cli.srtt_ms, U64(r.cli.retransmissions), U64(r.cli.timeouts));
        if (have_tcp) fprintf(csv, "%.3f", tcp);
        fprintf(csv, "\n");
        xs[nx] = loss; ys[nx++] = r.mbps;
        if (have_tcp) { tx[nt] = loss; ty[nt++] = tcp; }
    }
    fclose(csv);
    svg_series se[2] = {{"UDP-to-TCP engine", "#0b7285", nx, xs, ys, 0}, {"native TCP", "#e8590c", nt, tx, ty, 1}};
    if (svg_chart(svgp, "Throughput vs packet loss", "packet loss (%)", "goodput (Mbps, log)", se, nt > 1 ? 2 : 1, 1, NULL, NULL, 0) == 0)
        printf("wrote %s and %s\n", csvp, svgp);
    return 0;
}

/* ------------------------------------------------------------ demo (self-contained, writes graphs + report) */
typedef struct { char name[64], note[160]; double mbps; int ok; unsigned long long retx, fast, rto, tlp, ooo, dup, bad; char svg[96]; } demo_row;

static void demo_line(const char *name, const xfer_res *r) {
    printf("  %-34s %s %s%8.2f Mbps%s  retx %-4llu (fast %llu, RTO %llu, probe %llu, rack %llu)  ooo %llu  dup %llu  bad-cksum %llu\n", name,
           r->ok ? "\033[32m✓ intact\033[0m" : "\033[31m✗ FAILED\033[0m", CB, r->mbps, C0, U64(r->cli.retransmissions), U64(r->cli.fast_retransmits),
           U64(r->cli.timeouts), U64(r->cli.tlp_probes), U64(r->cli.rack_retx), U64(r->srv.ooo_packets), U64(r->srv.dup_packets), U64(r->cli.checksum_failures + r->srv.checksum_failures));
}

static int cmd_demo(int argc, char **argv) {
    const char *dir = opt(argc, argv, "--out", "demo_out");
    int quick = flag(argc, argv, "--quick");
    mkdir(dir, 0755);
    size_t small = quick ? 400000 : 1000000, big = quick ? 2500000 : 6000000;
    rt_cfg cfg; rt_cfg_default(&cfg);
    printf("%s== udp→tcp engine: scripted demo ==%s  (every transfer is verified with SHA-256)\n\n", CB, C0);
    struct { const char *label, *preset, *file; size_t size; int cc, slow; uint32_t rbuf; int classic; } sc[] = {
        {"clean link", "clean", "clean", small * 2, CC_RENO, 0, 0, 0},
        {"5% packet loss", "lossy", "lossy", small * 2, CC_RENO, 0, 0, 0},
        {"5% loss, classic recovery (no RACK/TLP)", "lossy", "lossy_classic", small * 2, CC_RENO, 0, 0, 1},
        {"reorder + duplicate + corrupt", "chaos", "chaos", small * 2, CC_RENO, 0, 0, 0},
        {"mobile-like (2% loss, 40 ms, 10 Mbps)", "mobile", "mobile", small * 2, CC_RENO, 0, 0, 0},
        {"bottleneck 20 Mbps, Reno", "bottleneck", "bottleneck_reno", big, CC_RENO, 0, 0, 0},
        {"bottleneck 20 Mbps, CUBIC", "bottleneck", "bottleneck_cubic", big, CC_CUBIC, 0, 0, 0},
        {"slow receiver (flow control)", "clean", "slow_receiver", small, CC_RENO, 400, 16 * 1024, 0},
    };
    int N = (int)(sizeof sc / sizeof sc[0]);
    demo_row rows[16]; int nr = 0, fails = 0;
    for (int i = 0; i < N; i++) {
        imp_cfg ic; imp_preset(sc[i].preset, &ic);
        xfer_args a = {.size = sc[i].size, .imp = &ic, .seed = 11, .timeout = 90, .cfg = cfg, .slow_kbps = sc[i].slow, .capture = 1};
        a.cfg.cc = sc[i].cc;
        if (sc[i].rbuf) a.cfg.recv_buf = sc[i].rbuf;
        if (sc[i].classic) { a.cfg.rack = 0; a.cfg.tlp = 0; a.cfg.min_rto = 0.1; }      /* what a textbook stack would do */
        xfer_res r;
        run_transfer(&a, &r);
        demo_line(sc[i].label, &r);
        if (!strcmp(sc[i].file, "lossy")) print_repair_map(r.seg_hist, r.nseg);
        if (!r.ok) fails++;
        demo_row *d = &rows[nr++];
        snprintf(d->name, sizeof d->name, "%s", sc[i].label);
        d->mbps = r.mbps; d->ok = r.ok; d->retx = U64(r.cli.retransmissions); d->fast = U64(r.cli.fast_retransmits); d->rto = U64(r.cli.timeouts);
        d->tlp = U64(r.cli.tlp_probes); d->ooo = U64(r.srv.ooo_packets); d->dup = U64(r.srv.dup_packets); d->bad = U64(r.cli.checksum_failures + r.srv.checksum_failures);
        snprintf(d->note, sizeof d->note, "%s", sc[i].slow ? "receiver throttled to 400 KB/s, 16 KB buffer: sender obeys the advertised window" : "");
        char path[256], title[128];
        snprintf(d->svg, sizeof d->svg, "cwnd_%s.svg", sc[i].file);
        snprintf(path, sizeof path, "%s/%s", dir, d->svg);
        snprintf(title, sizeof title, "cwnd: %s", sc[i].label);
        svg_cwnd(path, title, r.trace, r.ntrace, r.events, r.nevents, cfg.mss);
        xfer_res_free(&r);
    }
    printf("\n%sFairness%s (3 flows, one 20 Mbps bottleneck)\n", CB, C0);
    imp_cfg bn; imp_preset("bottleneck", &bn);
    rt_cfg fcfg = cfg; fcfg.trace = 0;
    fair_res fr; int frc = run_fairness(quick ? 1500000 : 3000000, &bn, &fcfg, 3, &fr);
    print_fair(&fr);
    if (frc) fails++;
    printf("\n%sForward error correction%s (1%% loss, 40 ms round trip: every retransmission costs a full round trip)\n", CB, C0);
    double fecm[3] = {0}; const char *fecn[3] = {"no FEC", "FEC 1-in-8 (fixed)", "FEC adaptive"}; unsigned long long fecrec[3] = {0};
    for (int k = 0; k < 3; k++) {
        imp_cfg ic; imp_cfg_default(&ic); ic.loss = 1; ic.delay_ms = 20;
        double mb[3];
        for (int rep = 0; rep < 3; rep++) {
            ic.seed = 1 + rep;
            xfer_args a = {.size = small * 2, .imp = &ic, .seed = 1, .timeout = 90, .cfg = cfg};
            a.cfg.trace = 0; a.cfg.fec = k == 1 ? 8 : 0; a.cfg.fec_auto = k == 2;
            xfer_res r; run_transfer(&a, &r);
            mb[rep] = r.mbps; fecrec[k] = U64(r.srv.fec_recovered);
            if (!r.ok) fails++;
        }
        fecm[k] = median(mb, 3);
        printf("  %-22s %s%8.2f Mbps%s  (median of 3)  packets rebuilt by the receiver: %llu\n", fecn[k], CB, fecm[k], C0, fecrec[k]);
    }
    printf("\n");
    hol_res ho, hm; imp_cfg hic; imp_preset("wan", &hic);
    if (hol_compare(4, quick ? 250000 : 400000, &hic, &cfg, quick ? 1 : 3, &ho, &hm)) fails++;
    hol_report(&ho, &hm, quick ? 250000 : 400000, &hic);
    { char hp[300]; snprintf(hp, sizeof hp, "%s/streams_ordered.svg", dir); hol_svg(hp, "One ordered stream: delivery of ALL streams stalls together", &ho);
      snprintf(hp, sizeof hp, "%s/streams_independent.svg", dir); hol_svg(hp, "Independent streams: each stream stalls only when it loses data", &hm);
      hol_block_svg(dir, &ho, &hm); }
    printf("\n%sLoss sweep vs native TCP%s\n", CB, C0);
    double losses[] = {0, 0.5, 1, 2, 5}, xs[5], ys[5], tcp = 0; int nl = 5;
    for (int i = 0; i < nl; i++) {
        imp_cfg ic; imp_cfg_default(&ic); ic.loss = losses[i];
        double mb[5]; int allok = 1;
        for (int k = 0; k < 5; k++) {
            ic.seed = 1 + k;
            xfer_args a = {.size = small, .imp = &ic, .seed = 1, .timeout = 90, .cfg = cfg};
            a.cfg.trace = 0;
            xfer_res r; run_transfer(&a, &r);
            mb[k] = r.mbps; allok &= r.ok;
        }
        if (i == 0) tcp_transfer(small, 1, &tcp);
        xs[i] = losses[i]; ys[i] = median(mb, 5);
        printf("  loss %4.1f%%  %s%8.2f Mbps%s  (median of 5, range %.0f-%.0f)  %s\n", losses[i], CB, ys[i], C0, mb[0], mb[4], allok ? "intact" : "FAILED");
        if (!allok) fails++;
    }
    printf("  native kernel TCP, clean link: %.2f Mbps\n", tcp);
    double tx[2] = {0, 5}, ty[2] = {tcp, tcp};
    svg_series se[2] = {{"UDP-to-TCP engine", "#0b7285", nl, xs, ys, 0}, {"native TCP (clean link)", "#e8590c", 2, tx, ty, 1}};
    char p2[256]; snprintf(p2, sizeof p2, "%s/throughput_vs_loss.svg", dir);
    svg_chart(p2, "Throughput vs packet loss", "packet loss (%)", "goodput (Mbps, log)", se, 2, 1, NULL, NULL, 0);
    /* HTML report */
    snprintf(p2, sizeof p2, "%s/report.html", dir);
    FILE *f = fopen(p2, "w");
    if (f) {
        fprintf(f, "<!doctype html><meta charset=utf-8><title>udp→tcp demo report</title><style>body{font:15px system-ui;background:#f6f3ea;color:#1d2a2e;max-width:980px;margin:30px auto;padding:0 20px}"
                   "h1,h2{font-family:ui-monospace,Menlo,monospace}table{border-collapse:collapse;width:100%%;font:13px ui-monospace,Menlo,monospace}td,th{border-bottom:1px solid #d9d3c3;padding:6px 8px;text-align:right}"
                   "td:first-child,th:first-child{text-align:left}img{width:100%%;border:1px solid #d9d3c3;margin:8px 0 22px}.ok{color:#2b8a3e}.bad{color:#c92a2a}</style>"
                   "<h1>udp→tcp engine · demo report</h1><table><tr><th>scenario<th>result<th>Mbps<th>retx<th>fast<th>RTO<th>probes<th>out-of-order<th>dups<th>bad checksums</tr>");
        for (int i = 0; i < nr; i++)
            fprintf(f, "<tr><td>%s<td class=%s>%s<td>%.1f<td>%llu<td>%llu<td>%llu<td>%llu<td>%llu<td>%llu<td>%llu</tr>", rows[i].name, rows[i].ok ? "ok" : "bad",
                    rows[i].ok ? "✓ intact" : "✗ failed", rows[i].mbps, rows[i].retx, rows[i].fast, rows[i].rto, rows[i].tlp, rows[i].ooo, rows[i].dup, rows[i].bad);
        fprintf(f, "</table><p>Fairness (3 flows, 20 Mbps bottleneck): %.1f / %.1f / %.1f Mbps, Jain index %.3f.</p><h2>Throughput vs loss</h2><img src=throughput_vs_loss.svg>", fr.mbps[0], fr.mbps[1], fr.mbps[2], fr.jain);
        fprintf(f, "<h2>Forward error correction (1%% loss, 40 ms RTT)</h2><p>no FEC %.1f Mbps &middot; FEC 1-in-8 %.1f Mbps &middot; adaptive %.1f Mbps</p>", fecm[0], fecm[1], fecm[2]);
        fprintf(f, "<h2>Head-of-line blocking: one ordered stream vs independent streams</h2><p>Average data held back: %.1f KB (ordered) vs %.1f KB (independent); peak %.1f vs %.1f KB.</p><img src=streams_blocking.svg><img src=streams_ordered.svg><img src=streams_independent.svg>",
                ho.avg_held_kb, hm.avg_held_kb, ho.peak_held_kb, hm.peak_held_kb);
        for (int i = 0; i < nr; i++) fprintf(f, "<h2>%s</h2><img src=%s>", rows[i].name, rows[i].svg);
        fclose(f);
    }
    printf("\n%s%s%s  graphs + report written to %s/   (open %s/report.html in a browser)\n", fails ? CR : CG, fails ? "SOME CHECKS FAILED" : "ALL TRANSFERS DELIVERED INTACT", C0, dir, dir);
    return fails ? 1 : 0;
}

/* ------------------------------------------------------------ main */
int main(int argc, char **argv) {
    g_color = isatty(1) && !getenv("NO_COLOR");
    if (argc < 2 || !strcmp(argv[1], "help") || !strcmp(argv[1], "--help") || !strcmp(argv[1], "-h")) { usage(); return argc < 2 ? 2 : 0; }
    if (!strcmp(argv[1], "server")) return cmd_server(argc, argv);
    if (!strcmp(argv[1], "client")) return cmd_client(argc, argv);
    if (!strcmp(argv[1], "proxy")) return cmd_proxy(argc, argv);
    if (!strcmp(argv[1], "impair")) return cmd_impair(argc, argv);
    if (!strcmp(argv[1], "plot")) return cmd_plot(argc, argv);
    if (!strcmp(argv[1], "fairness")) return cmd_fairness(argc, argv);
    if (!strcmp(argv[1], "streams")) return cmd_streams(argc, argv);
    if (!strcmp(argv[1], "selftest")) return cmd_selftest();
    if (!strcmp(argv[1], "bench")) return cmd_bench(argc, argv);
    if (!strcmp(argv[1], "demo")) return cmd_demo(argc, argv);
    if (!strcmp(argv[1], "ui")) return ui_serve(opt(argc, argv, "--bind", "127.0.0.1"), atoi(opt(argc, argv, "--port", "8080")), flag(argc, argv, "--open"));
    fprintf(stderr, "%sunknown command '%s'%s\n\n", CR, argv[1], C0);
    usage();
    return 2;
}
