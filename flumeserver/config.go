package main

import (
	"errors"
	"fmt"
	log "github.com/inconshreveable/log15"
	"github.com/openrelayxyz/cardinal-streams/transports"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type statsdOpts struct {
	Address  string `yaml:"address"`
	Port     string `yaml:"port"`
	Prefix   string `yaml:"prefix"`
	Interval int64  `yaml:"interval.sec"`
	Minor    bool   `yaml:"include.minor"`
}

type cloudwatchOpts struct {
	Namespace   string            `yaml:"namespace"`
	Dimensions  map[string]string `yaml:"dimensions"`
	Interval    int64             `yaml:"interval.sec"`
	Percentiles []float64         `yaml:"percentiles"`
	Minor       bool              `yaml:"include.minor"`
}

type broker struct {
	URL               string `yaml:"url"`
	DefaultTopic      string `yaml:"default.topic"`
	BlockTopic        string `yaml:"block.topic"`
	TransactionsTopic string `yaml:"transactions.topic"`
	LogsTopic         string `yaml:"logs.topic"`
	ReceiptTopic      string `yaml:"receipts.topic"`
	Rollback          int64  `yaml:"rollback"`
}

type Config struct {
	Port           int64  `yaml:"port"`
	PprofPort      int    `yaml:"pprofPort"`
	MinSafeBlock   int    `yaml:"minSafeBlock"`
	Network        string `yaml:"networkName"`
	Chainid        uint64 `yaml:"chainid"`
	HomesteadBlock uint64 `yaml:"homesteadBlock"`
	Eip155Block    uint64 `yaml:"eip155Block"`
	TxTopic        string `yaml:"mempoolTopic"`
	KafkaRollback  int64  `yaml:"kafkaRollback"`
	ReorgThreshold int64  `yaml:"reorgThreshold"`
	MempoolDb      string `yaml:"mempoolDB"`
	BlocksDb       string `yaml:"blocksDB"`
	TxDb           string `yaml:"transactionsDB"`
	LogsDb         string `yaml:"logsDB"`
	MempoolSlots   int    `yaml:"mempoolSize"`
	Concurrency    int    `yaml:"concurrency"`
	LogLevel       string `yaml:"loggingLevel"`
	Brokers        []broker
	brokers        []transports.BrokerParams
	Statsd *statsdOpts `yaml:"statsd"`
	CloudWatch *cloudwatchOpts `yaml:"cloudwatch"`
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

	if cfg.MempoolDb == "" {
		return nil, errors.New("No mempool database specified")
	}
	if cfg.BlocksDb == "" {
		return nil, errors.New("No blocks database specified")
	}
	if cfg.TxDb == "" {
		return nil, errors.New("No transactions database specified")
	}
	if cfg.LogsDb == "" {
		return nil, errors.New("No logs database specified")
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
	case "sepolia":
		cfg.Chainid = 11155111
	case "kiln":
		cfg.Chainid = 1337802
	case "":
		if cfg.Chainid == 0 {
			err := errors.New("Network name, eipp155Block, and homestead Block values must be set in configuration file")
			return nil, err
		} //if chainid is not zero we assume the other fields are valid
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
		cfg.PprofPort = 6969
	}

	if cfg.MinSafeBlock == 0 {
		cfg.MinSafeBlock = 1000000
	}

	if cfg.KafkaRollback == 0 {
		cfg.KafkaRollback = 5000
	}

	if cfg.ReorgThreshold == 0 {
		cfg.ReorgThreshold = 128
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 16
	}

	if len(cfg.Brokers) == 0 {
		return nil, errors.New("Config must specify at least one broker")
	}
	cfg.brokers = make([]transports.BrokerParams, len(cfg.Brokers))
	for i := range cfg.Brokers {
		if cfg.Brokers[i].DefaultTopic == "" {
			cfg.Brokers[i].DefaultTopic = fmt.Sprintf("cardinal-%v", cfg.Chainid)
		}
		if cfg.Brokers[i].BlockTopic == "" {
			cfg.Brokers[i].BlockTopic = fmt.Sprintf("%v-block", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].LogsTopic == "" {
			cfg.Brokers[i].LogsTopic = fmt.Sprintf("%v-logs", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].TransactionsTopic == "" {
			cfg.Brokers[i].TransactionsTopic = fmt.Sprintf("%v-tx", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].ReceiptTopic == "" {
			cfg.Brokers[i].ReceiptTopic = fmt.Sprintf("%v-receipt", cfg.Brokers[i].DefaultTopic)
		}
		if cfg.Brokers[i].Rollback == 0 {
			cfg.Brokers[i].Rollback = 5000
		}
		cfg.brokers[i] = transports.BrokerParams{
			URL:          cfg.Brokers[i].URL,
			DefaultTopic: cfg.Brokers[i].DefaultTopic,
			Topics: []string{
				cfg.Brokers[i].DefaultTopic,
				cfg.Brokers[i].BlockTopic,
				cfg.Brokers[i].LogsTopic,
				cfg.Brokers[i].TransactionsTopic,
				cfg.Brokers[i].ReceiptTopic,
			},
			Rollback: cfg.Brokers[i].Rollback,
		}
	}
	if cfg.CloudWatch != nil {
		if cfg.CloudWatch.Namespace == "" {
			cfg.CloudWatch.Namespace = "Flume"
		}
	}
	return &cfg, nil
}
