#include "rtp.h"
#include <string.h>

static void put16(uint8_t *p, uint32_t v) { p[0] = v >> 8; p[1] = v; }
static void put32(uint8_t *p, uint32_t v) { p[0] = v >> 24; p[1] = v >> 16; p[2] = v >> 8; p[3] = v; }
static uint32_t get16(const uint8_t *p) { return (p[0] << 8) | p[1]; }
static uint32_t get32(const uint8_t *p) { return ((uint32_t)p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]; }

/* 16-bit one's complement checksum (RFC 1071). Verifies as 0 over a valid packet. */
uint16_t rt_cksum(const uint8_t *d, size_t n) {
    uint32_t s = 0;
    size_t i;
    for (i = 0; i + 1 < n; i += 2) s += (d[i] << 8) | d[i + 1];
    if (n & 1) s += d[n - 1] << 8;
    while (s >> 16) s = (s & 0xFFFF) + (s >> 16);
    return (uint16_t)~s;
}

/* Header: id(4) seq(4) ack(4) flags(1) hdrlen-words(1) window>>8 (2) plen(2) cksum(2) [+SACK] */
size_t rt_pkt_encode(const rt_pkt *p, uint8_t *out) {
    int ns = p->nsack > RT_MAX_SACK ? RT_MAX_SACK : p->nsack;
    size_t hl = RT_HDR_LEN + 8 * (size_t)ns;
    uint32_t w = p->window >> 8;
    put32(out, p->conn_id);
    put32(out + 4, p->seq);
    put32(out + 8, p->ack);
    out[12] = p->flags | (ns ? F_SACK : 0);
    out[13] = (uint8_t)(hl / 4);
    put16(out + 14, w > 0xFFFF ? 0xFFFF : w);
    put16(out + 16, p->plen);
    put16(out + 18, 0);
    for (int i = 0; i < ns; i++) {
        put32(out + RT_HDR_LEN + 8 * i, p->sack[i][0]);
        put32(out + RT_HDR_LEN + 8 * i + 4, p->sack[i][1]);
    }
    if (p->plen) memcpy(out + hl, p->payload, p->plen);
    put16(out + 18, rt_cksum(out, hl + p->plen));
    return hl + p->plen;
}

int rt_pkt_decode(const uint8_t *b, size_t len, rt_pkt *p) {
    if (len < RT_HDR_LEN) return -1;
    size_t hl = (size_t)b[13] * 4;
    uint16_t plen = (uint16_t)get16(b + 16);
    if (hl < RT_HDR_LEN || hl > RT_HDR_LEN + 8 * RT_MAX_SACK || ((hl - RT_HDR_LEN) % 8)) return -2;
    if (len != hl + plen) return -3;
    if (rt_cksum(b, len) != 0) return -4;
    p->conn_id = get32(b);
    p->seq = get32(b + 4);
    p->ack = get32(b + 8);
    p->flags = b[12] & ~F_SACK;
    p->window = get16(b + 14) << 8;
    p->plen = plen;
    p->payload = b + hl;
    p->nsack = 0;
    if (b[12] & F_SACK) {
        for (size_t off = RT_HDR_LEN; off < hl; off += 8) {
            uint32_t s = get32(b + off), e = get32(b + off + 4);
            if ((int32_t)(e - s) <= 0) return -5;
            p->sack[p->nsack][0] = s;
            p->sack[p->nsack][1] = e;
            p->nsack++;
        }
    }
    return 0;
}
