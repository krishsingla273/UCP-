#ifndef PLOT_H
#define PLOT_H
#include "rtp.h"
typedef struct { const char *name, *color; int n; const double *x, *y; int dashed; } svg_series;
/* Generic line chart -> SVG. vx/vkind: optional vertical event markers (0 fast rexmit, 1 RTO, 2 TLP). */
int svg_chart(const char *path, const char *title, const char *xl, const char *yl, const svg_series *s, int ns,
              int logy, const double *vx, const int *vkind, int nv);
int svg_cwnd(const char *path, const char *title, const rt_trace_pt *t, size_t n, const rt_event *ev, size_t nev, int mss);
int svg_cwnd_from_csv(const char *csv, const char *svg, int mss);
#endif
