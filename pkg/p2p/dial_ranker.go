package p2p

import (
	"sort"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

// custom adaptation of https://github.com/libp2p/go-libp2p/blob/703c3a4377d1ca2cf3fc93c92c883be1644f3ba6/p2p/net/swarm/dial_ranker.go
// priorize TCP connections as they seem to be more stable
const (
	TCP_SCORE     = 0
	WBS_SCORE     = 17
	QUIC_SCORE    = 18
	QUIC_V1_SCORE = 19

	PublicTCPDelay   = 250 * time.Millisecond
	PrivateTCPDelay  = 30 * time.Millisecond
	PublicQUICDelay  = 250 * time.Millisecond
	PrivateQUICDelay = 30 * time.Millisecond
	RelayDelay       = 250 * time.Millisecond

	QuicDelay = 250 * time.Millisecond
)

func CustomDialRanker(addrs []ma.Multiaddr) []network.AddrDelay {
	relay, addrs := filterAddrs(addrs, isRelayAddr)
	ip4, addrs := filterAddrs(addrs, func(a ma.Multiaddr) bool { return isProtocolAddr(a, ma.P_IP4) })
	ip6, addrs := filterAddrs(addrs, func(a ma.Multiaddr) bool { return isProtocolAddr(a, ma.P_IP6) })

	var relayOffset time.Duration = 0
	if len(ip4) > 0 || len(ip6) > 0 {
		// if there is a public direct address available delay relay dials
		relayOffset = RelayDelay
	}

	res := make([]network.AddrDelay, 0, len(addrs))
	for i := 0; i < len(addrs); i++ {
		res = append(res, network.AddrDelay{Addr: addrs[i], Delay: 0})
	}
	res = append(res, getAddrDelay(ip4, PublicTCPDelay, PublicQUICDelay, QuicDelay)...)
	res = append(res, getAddrDelay(ip6, PublicTCPDelay, PublicQUICDelay, QuicDelay)...)
	res = append(res, getAddrDelay(relay, PublicTCPDelay, PublicQUICDelay, 2*relayOffset)...)
	return res
}

func getAddrDelay(
	addrs []ma.Multiaddr,
	tcpDelay time.Duration,
	quicDelay time.Duration,
	offset time.Duration) []network.AddrDelay {

	sort.Slice(addrs, func(i, j int) bool { return score(addrs[i]) < score(addrs[j]) })

	res := make([]network.AddrDelay, 0, len(addrs))
	tcpCount := 0
	for _, a := range addrs {
		delay := offset
		switch {
		case isProtocolAddr(a, ma.P_QUIC) || isProtocolAddr(a, ma.P_QUIC_V1):
			if tcpCount >= 1 {
				delay += 2 * quicDelay
			} else if tcpCount == 1 {
				delay += quicDelay
			}
		case isProtocolAddr(a, ma.P_TCP):
			// For TCP addresses we dial a single address first and then wait for QUICDelay
			// After QUICDelay we dial rest of the QUIC addresses
			if tcpCount > 0 {
				delay += tcpDelay
			}
			tcpCount++
		}
		res = append(res, network.AddrDelay{Addr: a, Delay: delay})
	}
	return res
}

// score scores a multiaddress for dialing delay. lower is better
func score(a ma.Multiaddr) int {
	// the lower 16 bits of the result are the relavant port
	// the higher bits rank the protocol
	// low ports are ranked higher because they're more likely to
	// be listen addresses
	if _, err := a.ValueForProtocol(ma.P_WEBTRANSPORT); err == nil {
		p, _ := a.ValueForProtocol(ma.P_UDP)
		pi, _ := strconv.Atoi(p) // cannot error
		return pi + (1 << 18)
	}
	if _, err := a.ValueForProtocol(ma.P_QUIC); err == nil {
		p, _ := a.ValueForProtocol(ma.P_UDP)
		pi, _ := strconv.Atoi(p) // cannot error
		return pi + (1 << 17)
	}
	if _, err := a.ValueForProtocol(ma.P_QUIC_V1); err == nil {
		p, _ := a.ValueForProtocol(ma.P_UDP)
		pi, _ := strconv.Atoi(p) // cannot error
		return pi
	}

	if p, err := a.ValueForProtocol(ma.P_TCP); err == nil {
		pi, _ := strconv.Atoi(p) // cannot error
		return pi + (1 << 19)
	}
	return (1 << 30)
}

func isProtocolAddr(a ma.Multiaddr, p int) bool {
	found := false
	ma.ForEach(a, func(c ma.Component) bool {
		if c.Protocol().Code == p {
			found = true
			return false
		}
		return true
	})
	return found
}

// filterAddrs filters an address slice in place
func filterAddrs(addrs []ma.Multiaddr, f func(a ma.Multiaddr) bool) (filtered, rest []ma.Multiaddr) {
	j := 0
	for i := 0; i < len(addrs); i++ {
		if f(addrs[i]) {
			addrs[i], addrs[j] = addrs[j], addrs[i]
			j++
		}
	}
	return addrs[:j], addrs[j:]
}

func isRelayAddr(addr ma.Multiaddr) bool {
	_, err := addr.ValueForProtocol(ma.P_CIRCUIT)
	return err == nil
}
