package sig4byte

import (
  "encoding/hex"
)

//go:generate git clone https://github.com/ethereum-lists/4bytes.git /tmp/4bytes
//go:generate go run gen/gen.go
//go:generate rm -rf /tmp/4bytes

var Sigs map[string]string

func register(hex, sig string) {
  if Sigs == nil { sigs := make(map[string]string) }
  Sigs[hex] = sig
}

func Get(hex string) string {
  if sig, ok := Sigs[hex]; ok {
    return sig
  }
  return "UNKNOWN"
}

func GetBytes(sig []byte) string {
  return Get(hex.EncodeToString(sig))
}
