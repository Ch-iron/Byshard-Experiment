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

// serve serves the http REST API request from clients
func (bb *blockbuilder) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", bb.handleRoot)
	mux.HandleFunc("/query", bb.handleQuery)
	mux.HandleFunc("/requestLeader", bb.handleRequestLeader)
	mux.HandleFunc("/reportByzantine", bb.handleReportByzantine)
	mux.HandleFunc("/slow", bb.handleSlow)
	mux.HandleFunc("/flaky", bb.handleFlaky)
	mux.HandleFunc("/crash", bb.handleCrash)

	// http string should be in form of ":8080"
	ip, err := url.Parse("http://127.0.0.1:8000")
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + ip.Port()
	bb.server = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Info("blockbuildeer http server starting on ", ip)
	log.Fatal(bb.server.ListenAndServe())
}

func (bb *blockbuilder) handleQuery(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var query message.Query
	query.C = make(chan message.QueryReply)
	bb.TxChan <- query
	reply := <-query.C
	_, err := io.WriteString(w, reply.Info)
	if err != nil {
		log.Error(err)
	}
}

func (bb *blockbuilder) handleRequestLeader(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var reqLeader message.RequestLeader
	reqLeader.C = make(chan message.RequestLeaderReply)
	bb.TxChan <- reqLeader
	reply := <-reqLeader.C
	_, err := io.WriteString(w, reply.Leader)
	if err != nil {
		log.Error(err)
	}
}

// transaction 받는 부분
func (bb *blockbuilder) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req message.Transaction
	defer r.Body.Close()

	bb.MessageChan <- req
}

func (bb *blockbuilder) handleReportByzantine(w http.ResponseWriter, r *http.Request) {
	// log.Debugf("[%v] received Handle Report Byzantine", n.id)
	var req message.ReportByzantine
	bb.TxChan <- req
}

func (bb *blockbuilder) handleCrash(w http.ResponseWriter, r *http.Request) {
	bb.BBSocket.Crash(config.GetConfig().Crash)
}

func (bb *blockbuilder) handleSlow(w http.ResponseWriter, r *http.Request) {
	for id := range config.GetConfig().HTTPAddrs {
		bb.BBSocket.Slow(id, rand.Intn(config.GetConfig().Slow), 10)
	}
}

func (bb *blockbuilder) handleFlaky(w http.ResponseWriter, r *http.Request) {
	for id := range config.GetConfig().HTTPAddrs {
		bb.BBSocket.Flaky(id, 0.5, 10)
	}
}
