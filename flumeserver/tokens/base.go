package tokens

import (
  "encoding/json"
  "github.com/ethereum/go-ethereum/common"
  "log"
)

//go:generate git clone https://github.com/ethereum-lists/tokens.git /tmp/tokenlist
//go:generate go run gen/gen.go
//go:generate rm -rf /tmp/tokenlist

type Token struct {
  Symbol string `json:"symbol"`
  Address common.Address `json:"address"`
  Decimals json.Number `json:"decimals"`
  Name string `json:"name"`
  Type string `json:"type"`
  EnsAddress string `json:"ens_address"`
  Website string `json:"website"`
  Logo struct {
      Src string `json:"src"`
      Width json.RawMessage `json:"width"`
      Height json.RawMessage `json:"height"`
      IpfsHash string `json:"ipfs_hash"`
    } `json:"logo"`
  Support struct {
      Email string `json:"email"`
      URL string `json:"url"`
    } `json:"support"`
  Social map[string]string `json:"social"`
}

var Tokens map[uint64]map[common.Address]Token

func register(network uint64, tokenJSON []byte) {
  if len(tokenJSON) == 0 { return }
  if Tokens == nil {
    Tokens = make(map[uint64]map[common.Address]Token)
  }
  if _, ok := Tokens[network]; !ok {
    Tokens[network] = make(map[common.Address]Token)
  }
  token := Token{}
  if err := json.Unmarshal(tokenJSON, &token); err != nil {
    log.Printf("Failed to register token: (%v) `%v`", err.Error(), string(tokenJSON))
    return
  }
  Tokens[network][token.Address] = token
}
