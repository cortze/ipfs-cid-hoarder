package models

import (
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
)

// PeerInfo has the basic info of a Peer elected as a PR Holder
type PeerInfo struct {
	ID        peer.ID
	MultiAddr []ma.Multiaddr
	UserAgent string
	Client    string
	Version   string
	// TODO: Is there anything else to add?
}

func NewPeerInfo(peerId peer.ID, multiAddr []ma.Multiaddr, userAgent string) *PeerInfo {
	client, version := FilterClientType(userAgent)

	return &PeerInfo{
		ID:        peerId,
		MultiAddr: multiAddr,
		UserAgent: userAgent,
		Client:    client,
		Version:   version,
	}
}

func (p *PeerInfo) GetAddrInfo() peer.AddrInfo {
	addrinfo := peer.AddrInfo{
		ID:    p.ID,
		Addrs: make([]ma.Multiaddr, len(p.MultiAddr)),
	}
	addrinfo.Addrs = p.MultiAddr
	return addrinfo
}

// Aux funcs

func FilterClientType(userAgent string) (string, string) {
	userAgentLower := strings.ToLower(userAgent)
	fields := strings.Split(userAgentLower, "/")
	if strings.Contains(userAgentLower, "rust-libp2p") {
		return "rust-client", cleanVersion(getVersionIfAny(fields, 1))
	} else if strings.Contains(userAgentLower, "kubo") {
		return "kubo", cleanVersion(getVersionIfAny(fields, 1))
	} else if strings.Contains(userAgentLower, "go-ipfs") {
		return "go-ipfs", cleanVersion(getVersionIfAny(fields, 1))
	} else if strings.Contains(userAgentLower, "kubo") {
		return "kubo", cleanVersion(getVersionIfAny(fields, 1))
	} else if strings.Contains(userAgentLower, "hydra") {
		return "hydra-booster", cleanVersion(getVersionIfAny(fields, 1))
	} else if strings.Contains(userAgentLower, "ioi") {
		return "ioi", cleanVersion(getVersionIfAny(fields, 1))
	} else if strings.Contains(userAgentLower, "storm") {
		return "storm", cleanVersion(getVersionIfAny(fields, 1))
	} else if userAgentLower == "" || userAgent == "Not Defined " {
		return "NotIdentified", ""
	} else {
		log.Debugf("Could not get client from userAgent: %s", userAgent)
		return "Others", ""
	}
}

func getVersionIfAny(fields []string, index int) string {
	if index > (len(fields) - 1) {
		return "Unknown"
	} else {
		return fields[index]
	}
}

func cleanVersion(version string) string {
	cleaned := strings.Split(version, "+")[0]
	cleaned = strings.Split(cleaned, "-")[0]
	return cleaned
}

func cleanVersionAux(version string) string {
	cleaned := strings.Split(version, "+")[0]
	cleaned = strings.Split(cleaned, "-")[1]
	return cleaned
}
