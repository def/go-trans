package main

import (
	"bytes"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	logging "github.com/op/go-logging"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var CassandraSession *gocql.Session
var Logger = logging.MustGetLogger("go-translation")

type nodes []string

func (n *nodes) String() string {
	return fmt.Sprintf("%v", *n)
}

func (n *nodes) Set(value string) error {
	*n = append(*n, value)
	return nil
}

func PingHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "OK")
}

func TranslationListHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestId := r.Header.Get("X-Request-Id")
	values := r.URL.Query()
	siteArgs, ok := values["site"]
	if !ok {
		Logger.Info("application.%s: finished handler TranslationListHandler: total=%0.2f code=400", requestId, time.Since(start).Seconds()*1000)
		http.Error(w, "site param missing", http.StatusBadRequest)
		return
	}
	site, err := strconv.ParseInt(siteArgs[0], 10, 64)
	if err != nil {
		Logger.Info("application.%s: finished handler TranslationListHandler: total=%0.2f code=400", requestId, time.Since(start).Seconds()*1000)
		http.Error(w, "site param must be int", http.StatusBadRequest)
		return
	}

	langArgs, ok := values["lang"]

	if !ok {
		Logger.Info("application.%s: finished handler TranslationListHandler: total=%0.2f code=400", requestId, time.Since(start).Seconds()*1000)
		http.Error(w, "lang param missing", http.StatusBadRequest)
		return
	}
	tArgs, ok := values["t"]
	if !ok {
		Logger.Info("application.%s: finished handler TranslationListHandler: total=%0.2f code=400", requestId, time.Since(start).Seconds()*1000)
		http.Error(w, "t params missing", http.StatusBadRequest)
		return
	}
	iter := CassandraSession.Query(
		"SELECT name, value FROM translation WHERE lang=? and site_id=? AND name IN ?",
		langArgs[0], site, tArgs,
	).Iter()
	res := &bytes.Buffer{}
	var name, value string
	res.Write([]byte(xml.Header))
	res.Write([]byte("<translations>"))

	keys := make(map[string]bool, len(tArgs))
	for _, k := range tArgs {
		keys[k] = true
	}

	for iter.Scan(&name, &value) {
		delete(keys, name)
		res.Write([]byte(fmt.Sprintf("<translation name=\"%s\">%s</translation>", name, value)))
	}
	for k, _ := range keys {
		res.Write([]byte(fmt.Sprintf("<translation name=\"%s\">[%s:%d][%s]</translation>", k, strings.ToLower(langArgs[0]), site, k)))
	}

	res.Write([]byte("</translations>"))
	if err := iter.Close(); err != nil {
		Logger.Info("application.%s: finished handler TranslationListHandler: total=%0.2f code=503", requestId, time.Since(start).Seconds()*1000)
		Logger.Error("failed to get data from cassandra: %s", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.Write(res.Bytes())
	Logger.Info("application.%s: finished handler TranslationListHandler: total=%0.2f code=200", requestId, time.Since(start).Seconds()*1000)
}

func main() {
	format := logging.MustStringFormatter("%{time:2006-01-02 15:04:05.999} %{level} %{message}")
	logging.SetFormatter(format)
	logging.SetLevel(logging.DEBUG, "go-translation")

	var cassandraNodes nodes
	flag.Var(&cassandraNodes, "n", "list of cassandra IPs")
	keyspace := flag.String("k", "translations", "cassandra keyspace")
	port := flag.Int("p", 8080, "listen port")

	flag.Parse()
	if len(cassandraNodes) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	Logger.Info("using cassandra ips: %v", cassandraNodes)
	Logger.Info("cassandra keyspace: %s", *keyspace)
	Logger.Info("listening port: %d", *port)
	cluster := gocql.NewCluster(cassandraNodes...)
	cluster.Timeout = 100 * time.Millisecond
	cluster.NumConns = 10
	cluster.Keyspace = *keyspace

	var err error
	CassandraSession, err = cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	CassandraSession.SetConsistency(gocql.LocalOne)

	http.HandleFunc("/status", PingHandler)
	http.HandleFunc("/translationList", TranslationListHandler)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
