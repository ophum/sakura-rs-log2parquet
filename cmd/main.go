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
	var logFile, outFile string
	flag.StringVar(&logFile, "log", "access.log", "access log path")
	flag.StringVar(&outFile, "out", "output.parquet", "output parquet path")
	flag.Parse()

	files, err := filepath.Glob("access_log*")
	if err != nil {
		panic(err)
	}
	log.Println(files)

	accessLogs := []*sakurarslog2parquet.AccessLog{}
	for _, path := range files {
		log.Println("parse", path)
		logs, err := parseFromFile(path)
		if err != nil {
			panic(err)
		}
		accessLogs = append(accessLogs, logs...)
	}

	if len(accessLogs) > 0 {
		log.Println("write parquet")
		if err := sakurarslog2parquet.WriteAccessLog(outFile, accessLogs); err != nil {
			panic(err)
		}
	}
}

func parseFromFile(path string) ([]*sakurarslog2parquet.AccessLog, error) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var fr io.ReadCloser = f
	if filepath.Ext(path) == ".gz" {
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
			return nil, err
		}

		accessLog, err := sakurarslog2parquet.ParseAccessLog(string(l))
		if err != nil {
			log.Println(string(l))
			log.Println(err)
			continue
		}

		logs = append(logs, accessLog)
	}
	return logs, nil
}
