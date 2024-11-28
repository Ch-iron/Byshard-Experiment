package node

import (
	"io"
	"math/rand"
	"net/http"
	"net/url"

	"paperexperiment/config"
	"paperexperiment/log"
	"paperexperiment/message"
)

// http request header names
const (
	HTTPClientID  = "Id"
	HTTPCommandID = "Cid"
)

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)
	mux.HandleFunc("/query", n.handleQuery)
	mux.HandleFunc("/requestLeader", n.handleRequestLeader)
	mux.HandleFunc("/reportByzantine", n.handleReportByzantine)
	mux.HandleFunc("/slow", n.handleSlow)
	mux.HandleFunc("/flaky", n.handleFlaky)
	mux.HandleFunc("/crash", n.handleCrash)

	// http string should be in form of ":8080"
	ip, err := url.Parse(config.Configuration.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + ip.Port()
	n.server = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe())
}

func (n *node) handleQuery(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var query message.Query
	query.C = make(chan message.QueryReply)
	n.TxChan <- query
	reply := <-query.C
	_, err := io.WriteString(w, reply.Info)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleRequestLeader(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var reqLeader message.RequestLeader
	reqLeader.C = make(chan message.RequestLeaderReply)
	n.TxChan <- reqLeader
	reply := <-reqLeader.C
	_, err := io.WriteString(w, reply.Leader)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req message.Transaction
	defer r.Body.Close()

	n.TxChan <- req
}

func (n *node) handleReportByzantine(w http.ResponseWriter, r *http.Request) {
	// log.Debugf("[%v] received Handle Report Byzantine", n.id)
	var req message.ReportByzantine

	n.TxChan <- req
}

func (n *node) handleCrash(w http.ResponseWriter, r *http.Request) {
	n.Socket.Crash(config.GetConfig().Crash)
}

func (n *node) handleSlow(w http.ResponseWriter, r *http.Request) {
	for id := range config.GetConfig().HTTPAddrs {
		n.Socket.Slow(id, rand.Intn(config.GetConfig().Slow), 10)
	}
}

func (n *node) handleFlaky(w http.ResponseWriter, r *http.Request) {
	for id := range config.GetConfig().HTTPAddrs {
		n.Socket.Flaky(id, 0.5, 10)
	}
}
