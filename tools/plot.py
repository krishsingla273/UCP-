#!/usr/bin/env python3
"""Plot cwnd traces (trace_*.csv) and the loss sweep (bench_loss_sweep.csv). Needs matplotlib.
Usage: python3 tools/plot.py sample_output"""
import csv, glob, os, sys
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt

d = sys.argv[1] if len(sys.argv) > 1 else "sample_output"
MSS = 1400
for p in sorted(glob.glob(os.path.join(d, "trace_*.csv"))):
    rows = list(csv.DictReader(open(p)))
    if not rows: continue
    t = [float(r["t"]) for r in rows]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(t, [float(r["cwnd"]) / MSS for r in rows], label="cwnd (MSS)")
    ax.plot(t, [float(r["ssthresh"]) / MSS if float(r["ssthresh"]) else float("nan") for r in rows], "--", label="ssthresh (MSS)")
    ax.plot(t, [float(r["flight"]) / MSS for r in rows], ":", label="in flight (MSS)", alpha=.7)
    ax.set_xlabel("time (s)"); ax.set_ylabel("segments"); ax.grid(alpha=.3); ax.legend()
    ax.set_title("cwnd - " + os.path.basename(p)[6:-4])
    fig.tight_layout(); out = p[:-4].replace("trace_", "cwnd_") + ".png"; fig.savefig(out, dpi=130); plt.close(fig); print("wrote", out)
b = os.path.join(d, "bench_loss_sweep.csv")
if os.path.exists(b):
    rows = list(csv.DictReader(open(b)))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot([float(r["loss_pct"]) for r in rows], [float(r["udp_throughput_mbps"]) for r in rows], "o-", label="UDP-to-TCP engine (C)")
    tcp = [float(r["tcp_throughput_mbps"]) for r in rows if r["tcp_throughput_mbps"]]
    if tcp: ax.axhline(tcp[0], color="gray", ls="--", label=f"native TCP, clean link ({tcp[0]:.0f} Mbps)")
    ax.set_yscale("log"); ax.set_xlabel("packet loss (%)"); ax.set_ylabel("goodput (Mbps)"); ax.grid(alpha=.3, which="both"); ax.legend()
    ax.set_title("Throughput vs loss"); fig.tight_layout(); fig.savefig(os.path.join(d, "throughput_vs_loss.png"), dpi=130); print("wrote throughput_vs_loss.png")
