package p2p

import (
	"encoding/hex"
	"strings"

	"github.com/libp2p/go-libp2p-core/crypto"
)

// Parse a Secp256k1PrivateKey from string, checking if it has the proper curve
func ParsePrivateKey(v string) (crypto.PrivKey, error) {
	v = strings.TrimPrefix(v, "0x")
	privKeyBytes, err := hex.DecodeString(v)
	if err != nil {
		log.Debugf("cannot parse private key, expected hex string: %v", err)
		return nil, err
	}
	var priv crypto.PrivKey
	priv, err = crypto.UnmarshalSecp256k1PrivateKey(privKeyBytes)
	if err != nil {
		log.Debugf("cannot parse private key, invalid private key (Secp256k1): %v", err)
		return nil, err
	}
	return priv, nil
}

// Export Private Key to a string
func PrivKeyToString(inputKey crypto.PrivKey) string {

	keyBytes, err := inputKey.Raw()

	if err != nil {
		log.Debugf("Could not Export Private Key to String")
		return ""
	}

	return hex.EncodeToString(keyBytes)
}
