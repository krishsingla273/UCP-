/* plot.c - dependency-free SVG charts (cwnd traces, loss sweeps). Style matches the web UI. */
#include "plot.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define W 920
#define H 440
#define ML 62
#define MR 24
#define MT 46
#define MB 52

static double nice_step(double range, int ticks) {
    double raw = range / ticks, mag = pow(10, floor(log10(raw))), f = raw / mag;
    return (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * mag;
}

int svg_chart(const char *path, const char *title, const char *xl, const char *yl, const svg_series *s, int ns,
              int logy, const double *vx, const int *vkind, int nv) {
    double x0 = 1e300, x1 = -1e300, y0 = 1e300, y1 = -1e300;
    for (int k = 0; k < ns; k++)
        for (int i = 0; i < s[k].n; i++) {
            double y = s[k].y[i];
            if (logy && y <= 0) continue;
            if (s[k].x[i] < x0) x0 = s[k].x[i];
            if (s[k].x[i] > x1) x1 = s[k].x[i];
            if (y < y0) y0 = y;
            if (y > y1) y1 = y;
        }
    if (x0 > x1) { x0 = 0; x1 = 1; y0 = 0; y1 = 1; }
    if (x1 == x0) x1 = x0 + 1;
    if (logy) { y0 = pow(10, floor(log10(y0))); y1 = pow(10, ceil(log10(y1))); if (y1 <= y0) y1 = y0 * 10; }
    else { y0 = 0; if (y1 <= 0) y1 = 1; y1 *= 1.08; }
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    double pw = W - ML - MR, ph = H - MT - MB;
#define PX(v) (ML + ((v) - x0) / (x1 - x0) * pw)
#define PY(v) (MT + ph - (logy ? (log10(v) - log10(y0)) / (log10(y1) - log10(y0)) : ((v) - y0) / (y1 - y0)) * ph)
    fprintf(f, "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 %d %d' font-family='ui-monospace,Menlo,Consolas,monospace' font-size='12'>\n", W, H);
    fprintf(f, "<rect width='%d' height='%d' fill='#f6f3ea'/>\n<text x='%d' y='26' font-size='15' font-weight='bold' fill='#1d2a2e'>%s</text>\n", W, H, ML, title);
    double xs = nice_step(x1 - x0, 8);
    for (double v = ceil(x0 / xs) * xs; v <= x1 + 1e-9; v += xs) {
        fprintf(f, "<line x1='%.1f' y1='%d' x2='%.1f' y2='%.0f' stroke='#d9d3c3'/><text x='%.1f' y='%d' fill='#5b676a' text-anchor='middle'>%g</text>\n", PX(v), MT, PX(v), MT + ph, PX(v), H - MB + 18, v);
    }
    if (logy) {
        for (double v = y0; v <= y1 * 1.0001; v *= 10)
            fprintf(f, "<line x1='%d' y1='%.1f' x2='%.0f' y2='%.1f' stroke='#d9d3c3'/><text x='%d' y='%.1f' fill='#5b676a' text-anchor='end'>%g</text>\n", ML, PY(v), ML + pw, PY(v), ML - 8, PY(v) + 4, v);
    } else {
        double ys = nice_step(y1 - y0, 6);
        for (double v = 0; v <= y1; v += ys)
            fprintf(f, "<line x1='%d' y1='%.1f' x2='%.0f' y2='%.1f' stroke='#d9d3c3'/><text x='%d' y='%.1f' fill='#5b676a' text-anchor='end'>%g</text>\n", ML, PY(v), ML + pw, PY(v), ML - 8, PY(v) + 4, v);
    }
    fprintf(f, "<rect x='%d' y='%d' width='%.0f' height='%.0f' fill='none' stroke='#1d2a2e'/>\n", ML, MT, pw, ph);
    for (int i = 0; i < nv; i++) {
        if (vx[i] < x0 || vx[i] > x1) continue;
        const char *col = vkind[i] == 1 ? "#c92a2a" : vkind[i] == 2 ? "#7a8a8e" : "#e8590c";
        fprintf(f, "<line x1='%.1f' y1='%d' x2='%.1f' y2='%.0f' stroke='%s' stroke-width='1' opacity='.35'/>\n", PX(vx[i]), MT, PX(vx[i]), MT + ph, col);
    }
    int lgap = (int)(pw / (ns > 0 ? ns : 1)); if (lgap > 190) lgap = 190;
    for (int k = 0; k < ns; k++) {
        fprintf(f, "<polyline fill='none' stroke='%s' stroke-width='1.8'%s points='", s[k].color, s[k].dashed ? " stroke-dasharray='6 4'" : "");
        for (int i = 0; i < s[k].n; i++) {
            if (logy && s[k].y[i] <= 0) continue;
            fprintf(f, "%.1f,%.1f ", PX(s[k].x[i]), PY(s[k].y[i]));
        }
        fprintf(f, "'/>\n");
        if (s[k].n <= 12)
            for (int i = 0; i < s[k].n; i++) if (!(logy && s[k].y[i] <= 0))
                fprintf(f, "<circle cx='%.1f' cy='%.1f' r='3.5' fill='%s'/>\n", PX(s[k].x[i]), PY(s[k].y[i]), s[k].color);
        fprintf(f, "<rect x='%d' y='%d' width='14' height='3' fill='%s'/><text x='%d' y='%d' fill='#1d2a2e'>%s</text>\n", ML + 10 + k * lgap, MT + 12 + 0, s[k].color, ML + 30 + k * lgap, MT + 17, s[k].name);
    }
    fprintf(f, "<text x='%.0f' y='%d' fill='#1d2a2e' text-anchor='middle'>%s</text>\n", ML + pw / 2, H - 12, xl);
    fprintf(f, "<text transform='translate(16 %.0f) rotate(-90)' fill='#1d2a2e' text-anchor='middle'>%s</text>\n</svg>\n", MT + ph / 2, yl);
    fclose(f);
    return 0;
}

int svg_cwnd(const char *path, const char *title, const rt_trace_pt *t, size_t n, const rt_event *ev, size_t nev, int mss) {
    if (!n) return -1;
    size_t step = n > 2500 ? n / 2500 : 1, m = 0;
    double *x = malloc(n * sizeof(double) * 4), *c = x + n, *ss = c + n, *fl = ss + n;
    for (size_t i = 0; i < n; i += step, m++) {
        x[m] = t[i].t; c[m] = t[i].cwnd / mss;
        ss[m] = t[i].ssthresh > 0 && t[i].ssthresh < 1e8 ? t[i].ssthresh / mss : NAN;
        fl[m] = t[i].flight / mss;
    }
    /* ssthresh: drop NaN points by compacting */
    double *sx = malloc(m * sizeof(double) * 2), *sy = sx + m; int sm = 0;
    for (size_t i = 0; i < m; i++) if (!isnan(ss[i])) { sx[sm] = x[i]; sy[sm] = ss[i]; sm++; }
    svg_series se[3] = {{"cwnd (MSS)", "#0b7285", (int)m, x, c, 0}, {"ssthresh", "#e8590c", sm, sx, sy, 1}, {"in flight", "#7a8a8e", (int)m, x, fl, 0}};
    double *vx = malloc((nev + 1) * sizeof(double)); int *vk = malloc((nev + 1) * sizeof(int));
    for (size_t i = 0; i < nev; i++) { vx[i] = ev[i].t; vk[i] = ev[i].kind; }
    int r = svg_chart(path, title, "time (s)   |  orange = fast retransmit, red = timeout, grey = tail-loss probe", "segments", se, 3, 0, vx, vk, (int)nev);
    free(x); free(sx); free(vx); free(vk);
    return r;
}

int svg_cwnd_from_csv(const char *csv, const char *svg, int mss) {
    FILE *f = fopen(csv, "r");
    if (!f) return -1;
    char line[512];
    size_t cap = 4096, n = 0;
    rt_trace_pt *t = malloc(cap * sizeof *t);
    if (!fgets(line, sizeof line, f)) { fclose(f); free(t); return -1; }
    while (fgets(line, sizeof line, f)) {
        rt_trace_pt p = {0};
        if (sscanf(line, "%lf,%lf,%lf,%lf,%lf,%lf,%lf", &p.t, &p.cwnd, &p.ssthresh, &p.flight, &p.srtt, &p.rto, &p.rwnd) < 7) continue;
        if (n == cap) { cap *= 2; t = realloc(t, cap * sizeof *t); }
        t[n++] = p;
    }
    fclose(f);
    int r = svg_cwnd(svg, "Congestion window", t, n, NULL, 0, mss);
    free(t);
    return r;
}
