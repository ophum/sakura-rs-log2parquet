package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"

	sakurarslog2parquet "github.com/ophum/sakura-rs-log2parquet"
)

func main() {
	var logFile string
	flag.StringVar(&logFile, "log", "access.log", "access log path")
	flag.Parse()

	f, err := os.Open(logFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var fr io.ReadCloser = f
	if filepath.Ext(logFile) == ".gz" {
		fr, err = gzip.NewReader(f)
		if err != nil {
			panic(err)
		}
		fr.Close()

	}

	r := bufio.NewReader(fr)

	logs := []*sakurarslog2parquet.AccessLog{}
	for {
		l, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			panic(err)
		}

		accessLog, err := sakurarslog2parquet.ParseAccessLog(string(l))
		if err != nil {
			log.Println(string(l))
			log.Println(err)
			continue
		}

		logs = append(logs, accessLog)
	}
	if len(logs) > 0 {
		if err := sakurarslog2parquet.WriteAccessLog(logFile+".parquet", logs); err != nil {
			panic(err)
		}
	}
}
