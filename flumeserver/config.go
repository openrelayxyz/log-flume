package main

import (
	"fmt"
	"runtime"
	"path"
	"strconv"
	"errors"
	"gopkg.in/yaml.v2"
	"github.com/openrelayxyz/cardinal-types"
	"github.com/openrelayxyz/cardinal-rpc"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"io/ioutil"
	"os"
)

type Config struct {
	Port *int64 `yaml:port`
  PprofPort *int `yaml:"pprofPort"`
  MinSafeBlock *int `yaml:"minSafeBlock"`
	Network string `yaml:networkName`
	Chainid int `yaml:chainid`
  TxTopic string `yaml:"mempoolTopic"`
  KafkaRollback int64 `yaml:"kafkaRollback"`
  ReorgThreshold int64 `yaml:"reorgThreshold"`
  MempoolDb string `yaml:"mempoolDB"`
	BlocksDb string `yaml"blocksDB"`
	TxDb string `yaml:"transactionsDB"`
	LogsDb string `yaml:"logsDB"`
	MempoolSlots int `yaml:"mempoolSize"`
	Concurrency int `yaml:"concurrency"`
	LogLevel string `yaml:"loggingLevel"`
}

func LoadConfig(fname string) (*Config, error) {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	cfg := Config{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	switch cfg.Network {
	case "mainnet":
	    cfg.HomesteadBlock = 1150000
	    cfg.Eip155Block = 2675000
	    cfg.Chainid = 1
	case "classic":
	    cfg.HomesteadBlock = 1150000
	    cfg.Eip155Block = 3000000
	    cfg.Chainid = 61
	case "ropsten":
	    cfg.HomesteadBlock = 0
	    cfg.Eip155Block = 10
	    cfg.Chainid = 3
	  case "rinkeby":
	    cfg.HomesteadBlock = 1
	    cfg.Eip155Block = 3
	    cfg.Chainid = 4
	  case "goerli":
			cfg.HomesteadBlock = 0
	    cfg.Eip155Block = 0
	    cfg.Chainid = 5
		case "":
			if cfg.chainid == 0 {
				err := errors.New("Network name, eipp155Block, and homestead Block values must be set in configuration file")
				return nil, err
			} //if chainid is not zero we assume the other fields are valid
		case "sepolia":
	    cfg.chainid = 11155111
	  case "kiln":
	    cfg.chainid = 1337802
		default:
			err := errors.New("Unrecognized network name")
			return nil, err
	}

	var logLvl log.Lvl
	switch cfg.LogLevel {
		case "debug":
			logLvl = log.LvlDebug
		case "info":
			logLvl = log.LvlInfo
		case "warn":
			logLvl = log.LvlWarn
		case "error":
			logLvl = log.LvlError
		default:
			logLvl = log.LvlInfo
	}

	log.Root().SetHandler(log.LvlFilterHandler(logLvl, log.Root().GetHandler()))

	if cfg.Port == 0 {
		cfg.Port = 8000
	}

	if cfg.PprofPort == 0 {
		cnf.PprofPort = 6969
	}

	if cfg.MinSafeBlock == 0 {
		cfg.MinSafeBlock = 1000000
	}

	if cfg.kafkaRollback == 0 {
		cfg.kafkaRollback = 5000
	}
	return &cfg, nil
}
