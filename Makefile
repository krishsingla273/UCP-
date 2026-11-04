CC      ?= gcc
CFLAGS  ?= -O2 -Wall -Wextra -std=gnu11
LDFLAGS ?= -pthread -lm
SRC     := src/codec.c src/cc.c src/conn.c src/impair.c src/sha256.c src/harness.c src/plot.c src/ui.c src/main.c

all: rtp

# the dashboard page is embedded into the binary as a C string
src/ui_html.h: ui/index.html
	( echo 'static const char UI_HTML[] ='; sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/^/"/' -e 's/$$/\\n"/' ui/index.html; echo ';' ) > $@

rtp: $(SRC) src/rtp.h src/harness.h src/plot.h src/ui.h src/ui_html.h
	$(CC) $(CFLAGS) -Isrc $(SRC) -o $@ $(LDFLAGS)

test: rtp
	./rtp selftest

clean:
	rm -f rtp *.o src/ui_html.h

.PHONY: all test clean
