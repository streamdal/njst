package main

import (
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/batchcorp/njst/bench"
	"github.com/batchcorp/njst/httpsvc"
	"github.com/batchcorp/njst/natssvc"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/batchcorp/njst/cli"
)

var (
	VERSION = "UNSET"

	params = &cli.Params{}
)

func init() {
	kingpin.Flag("node-id", "Node ID").
		Default(natssvc.RandID(8)).
		Envar("NJST_NODE_ID").
		StringVar(&params.NodeID)

	kingpin.Flag("debug", "Enable debug output").
		Envar("NJST_DEBUG").
		BoolVar(&params.Debug)

	kingpin.Flag("nats-address", "One or more NATS address to use").
		Default("localhost:4222").
		Envar("NJST_NATS_ADDRESS").
		StringsVar(&params.NATSAddress)

	kingpin.Flag("http-address", "What address to bind local HTTP server to").
		Default(":5000").
		Envar("NJST_HTTP_ADDRESS").
		StringVar(&params.HTTPAddress)

	kingpin.Flag("nats-subject", "Name of the NATS subject that njst cluster will use for internal communication. "+
		"(all njst instances should have the same setting)").
		Default("njst-internal").
		Envar("NJST_NATS_SUBJECT").
		StringVar(&params.NATSSubject)

	kingpin.Flag("nats-use-tls", "Whether to use TLS for NATS communication").
		Default("false").
		Envar("NJST_NATS_USE_TLS").
		BoolVar(&params.NATSUseTLS)

	kingpin.Flag("nats-tls-cert", "Path to the TLS certificate for NATS communication").
		Envar("NJST_NATS_TLS_CERT").
		ExistingFileVar(&params.NATSTLSClientCert)

	kingpin.Flag("nats-tls-key", "Path to the TLS key for NATS communication").
		Envar("NJST_NATS_TLS_KEY").
		ExistingFileVar(&params.NATSTLSClientKey)

	kingpin.Flag("nats-tls-ca", "Path to the TLS CA for NATS communication").
		Envar("NJST_NATS_TLS_CA").
		ExistingFileVar(&params.NATSTLSCaCert)

	kingpin.Flag("enable-pprof", "Enable pprof (exposes /debug/pprof/*").
		Envar("NJST_ENABLE_PPROF").
		BoolVar(&params.EnablePprof)

	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()
}

func main() {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	if params.Debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	logrus.Infof("njst is starting...")

	// Create dependencies
	n, err := natssvc.New(params)
	if err != nil {
		logrus.Fatal("Unable to setup NATS service: ", err)
	}

	b, err := bench.New(params, n)
	if err != nil {
		logrus.Fatal("Unable to setup benchmark service: ", err)
	}

	h, err := httpsvc.New(params, n, b, VERSION)
	if err != nil {
		logrus.Fatal("Unable to setup HTTP service: ", err)
	}

	msgHandlers := map[string]nats.MsgHandler{
		"njst." + params.NodeID + ".create": b.CreateMsgHandler,
		"njst." + params.NodeID + ".delete": b.DeleteMsgHandler,
	}

	// Start services
	if err := n.Start(msgHandlers); err != nil {
		logrus.Fatal("Unable to start NATS service: ", err)
	}

	if err := h.Start(); err != nil {
		logrus.Fatal("Unable to start HTTP service: ", err)
	}

	// Give nats service time to start and register itself
	time.Sleep(time.Second)

	nodes, err := n.GetNodeList()
	if err != nil {
		logrus.Fatal("unable to determine cluster participants: ", err)
	}

	logrus.Infof("NodeID:                       %s", params.NodeID)
	logrus.Infof("HTTP server listening on:     %s", params.HTTPAddress)
	logrus.Infof("Nodes in cluster:             %d", len(nodes))
	logrus.Infof("Version:                      %s", VERSION)
	logrus.Info("")
	logrus.Info("njst is ready. Refer to 'docs/api.md' for API usage.")

	// Catch SIGINT, remove our heartbeat key
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			// sig is a ^C, handle it
			logrus.Debugf("Caught signal: %s", sig)

			if err := n.RemoveHeartbeat(); err != nil {
				logrus.Errorf("Unable to remove heartbeat: %s", err)
			}

			os.Exit(1)
		}
	}()

	wg.Wait()
}
