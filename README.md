# UCP-

A small C project that explores a reliable, TCP-like transport implemented in user space over UDP. It includes a command-line client and server, an impairment proxy, transfer benchmarks, congestion-control experiments, and a browser-based dashboard.

> **Experimental project:** this is a learning and testing tool, not a drop-in replacement for TCP or a production transport. Its protocol is custom and is not wire-compatible with TCP.

## What it includes

- Reliable UDP transfers with checksums, retransmission, flow control, and Reno or CUBIC congestion control.
- Loss recovery experiments including RACK-style detection, tail-loss probes, and optional forward error correction (FEC).
- A configurable proxy for simulating loss, delay, jitter, duplication, reordering, corruption, and bandwidth limits.
- Independent logical streams for exploring head-of-line blocking.
- Transfer, fairness, and loss-sweep benchmarks, plus SVG graphs and an HTML demo report.
- A local web dashboard for running experiments and viewing results.

## Requirements

- A Unix-like system with GCC (or a compatible C compiler), `make`, POSIX sockets, and pthreads.
- Python 3 and Matplotlib only if you want to use the optional `tools/plot.py` script.
- Linux, `tc`/iproute2, and root privileges only for the `impair` command that configures kernel `netem`.

## Build

From the repository root:

```sh
make
```

This builds the `rtp` executable. Run `./rtp help` to see all commands and options.

## Quick start

Run the built-in tests and scripted demonstration:

```sh
./rtp selftest
./rtp demo
```

The demo writes graphs and an HTML report to `demo_out/` by default. Open `demo_out/report.html` in a browser to view it. For a shorter run, use `./rtp demo --quick`.

Start the local dashboard with:

```sh
./rtp ui --open
```

The dashboard binds to `127.0.0.1:8080` by default. Use `--port` to choose another port; `--bind` changes the bind address.

## Run a client/server transfer

Open three terminals in the repository directory. Start a receiver:

```sh
./rtp server --port 9000 --out received.bin --once
```

Optionally put the impairment proxy in front of it (the `lossy` preset simulates packet loss):

```sh
./rtp proxy --listen 9001 --target 127.0.0.1:9000 --preset lossy
```

Send 2 MB of generated test data through the proxy:

```sh
./rtp client --connect 127.0.0.1:9001 --size 2000000
```

You can send a file instead with `--file PATH`. The client and server print transfer statistics and a SHA-256 digest so you can verify the received data. Stop the proxy with Ctrl+C to display its counters.

For a direct transfer, connect the client to `127.0.0.1:9000` and omit the proxy.

## Useful commands

```sh
./rtp fairness --flows 3 --preset bottleneck
./rtp streams --streams 4 --preset wan
./rtp bench --losses 0,1,2,5
./rtp plot trace.csv
```

- `fairness` compares simultaneous flows sharing a simulated bottleneck.
- `streams` compares ordered delivery with independent streams.
- `bench` runs a packet-loss sweep and writes CSV and SVG output.
- `plot` turns a CSV trace produced with `--trace` into an SVG congestion-window graph.

Most commands accept `--cc reno` or `--cc cubic`. Use `./rtp help` for the complete command and option list.

## Kernel network impairment (Linux)

The `impair` command uses `tc netem` to change the selected interface's qdisc. It requires Linux, iproute2, and root privileges, and may affect other traffic on that interface. For example:

```sh
sudo ./rtp impair apply --dev lo --loss 2 --delay 20
sudo ./rtp impair show --dev lo
sudo ./rtp impair clear --dev lo
```

Use the user-space `proxy` command for isolated experiments that do not require changing a system interface.

## Make targets

- `make` or `make all` — build `rtp`.
- `make test` — build and run `./rtp selftest`.
- `make clean` — remove the executable and generated build files.

## Repository layout

- `src/` — transport, congestion control, proxy, benchmark harness, plotting, and CLI implementation.
- `ui/` — dashboard page embedded into the executable at build time.
- `tools/plot.py` — optional Matplotlib-based plotter for generated CSV files.
- `sample_output/` — example graphs and report output.

## License

No license is currently specified. Unless a license is added to this repository, the code is not granted for reuse, modification, or redistribution under an open-source license.
