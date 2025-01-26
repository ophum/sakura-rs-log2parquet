package main

import (
	"bufio"
	"errors"
	"flag"
	"io"
	"log"
	"os"

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

	r := bufio.NewReader(f)

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
