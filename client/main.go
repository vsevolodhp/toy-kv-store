package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type latencyLog struct {
	LatencySecs float64
	Sample      int
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	var (
		serverPort        = flag.Int("targetPort", 8080, "target server port")
		templatesFileName = flag.String("inputs", "puts.txt", "file with request templates")
	)

	flag.Parse()

	reqTemplates, err := getTemplates(*templatesFileName)
	if err != nil {
		slog.Error("unable to parse requests", "error", err)
		os.Exit(1)
	}

	baseURL := fmt.Sprintf("http://localhost:%d", *serverPort)
	transport := &http.Transport{
		MaxConnsPerHost:       10,
		MaxIdleConns:          20,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       30 * time.Second,
		ForceAttemptHTTP2:     false,
		DisableCompression:    true,
		ResponseHeaderTimeout: 15 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 0,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second}).DialContext,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}

	latencyCh := make(chan latencyLog, 10)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		f, err := os.OpenFile("latency_sample.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			slog.Error("unable to create latency_sample", "error", err)
			return
		}
		defer f.Close()
		w := csv.NewWriter(f)
		w.Write([]string{"Sample", "Latency_Secs"})
		for v := range latencyCh {
			if err = w.Write([]string{strconv.Itoa(v.Sample), fmt.Sprintf("%.4f", v.LatencySecs)}); err != nil {
				slog.Error("unable to log latency", "error", err)
			}
		}
		w.Flush()
	}()

	var wg sync.WaitGroup
	sem := make(chan struct{}, 1)

	for _, tmplt := range reqTemplates {
		wg.Add(1)
		sem <- struct{}{}

		go func(t reqTemplate) {
			defer wg.Done()
			defer func() { <-sem }()

			switch t.method {
			case http.MethodGet:
				doGET(client, baseURL, t, latencyCh)
			case http.MethodPut:
				doPUT(client, baseURL, t, latencyCh)
			case http.MethodDelete:
				doDELETE(client, baseURL, t, latencyCh)
			}
		}(tmplt)
	}
	wg.Wait()

	close(latencyCh)
	<-writerDone

	slog.Info("all reqTemplates have been processed")
}

func doGET(client *http.Client, baseURL string, tmplt reqTemplate, latencyCh chan<- latencyLog) {
	reqURL, err := url.JoinPath(baseURL, tmplt.path)
	if err != nil {
		slog.Error("unable to build request URL", "error", err)
		return
	}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		slog.Error("unable to build GET request", "lineno", tmplt.lineno, "error", err)
		return
	}
	resp, err := doWithRetry(client, req, tmplt.lineno, latencyCh)
	if err != nil {
		slog.Error("request failed", "lineno", tmplt.lineno, "error", err)
		return
	}
	defer resp.Body.Close()
	if tmplt.expected == "NOT_FOUND" {
		if resp.StatusCode != http.StatusNotFound {
			slog.Info("expected: NOT_FOUND", "got", resp.Status, "lineno", tmplt.lineno)
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		slog.Info("expected: OK", "got", resp.Status, "lineno", tmplt.lineno)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if got != tmplt.expected {
		slog.Info("unexpected result", "expected", tmplt.expected, "got", got, "lineno", tmplt.lineno)
	}
}

func doPUT(client *http.Client, baseURL string, tmplt reqTemplate, latencyCh chan<- latencyLog) {
	reqURL, err := url.JoinPath(baseURL, tmplt.path)
	if err != nil {
		slog.Error("unable to build request URL", "error", err)
		return
	}
	req, err := http.NewRequest(http.MethodPut, reqURL, strings.NewReader(tmplt.payload))
	if err != nil {
		slog.Error("unable to build PUT request", "lineno", tmplt.lineno, "error", err)
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := doWithRetry(client, req, tmplt.lineno, latencyCh)
	if err != nil {
		slog.Error("request failed", "lineno", tmplt.lineno, "error", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Info("expected: OK", "got", resp.Status, "lineno", tmplt.lineno)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if got != tmplt.payload {
		slog.Info("unexpected result", "expected", tmplt.expected, "got", got, "lineno", tmplt.lineno)
	}
}

func doDELETE(client *http.Client, baseURL string, tmplt reqTemplate, latencyCh chan<- latencyLog) {
	reqURL, err := url.JoinPath(baseURL, tmplt.path)
	if err != nil {
		slog.Error("unable to build request URL", "error", err)
		return
	}
	req, err := http.NewRequest(http.MethodDelete, reqURL, nil)
	if err != nil {
		slog.Error("unable to build DELETE request", "lineno", tmplt.lineno, "error", err)
		return
	}
	resp, err := doWithRetry(client, req, tmplt.lineno, latencyCh)
	if err != nil {
		slog.Error("request failed", "lineno", tmplt.lineno, "error", err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		slog.Info("expected: OK", "got", resp.Status, "lineno", tmplt.lineno)
	}
}

func doWithRetry(client *http.Client, req *http.Request, lineno int, latencyCh chan<- latencyLog) (*http.Response, error) {
	backoffs := []time.Duration{
		10 * time.Millisecond,
		30 * time.Millisecond,
		100 * time.Millisecond,
		300 * time.Millisecond,
		5 * time.Second,
	}
	var lastErr error
	for attempt := 0; attempt < len(backoffs)+1; attempt++ {
		start := time.Now()
		// TODO: there is an issue if PUT request is going to be retried
		// the Body field is going to be already EOF after the first try
		// either provide GetBody or build requests for each attempt
		resp, err := client.Do(req.Clone(req.Context()))
		end := time.Since(start).Seconds()
		latencyCh <- latencyLog{end, lineno}

		if err == nil && (resp.StatusCode < 500 || resp.StatusCode >= 600) {
			return resp, nil
		}
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("server returned: %s", resp.Status)
		} else {
			lastErr = err
		}
		if attempt == len(backoffs) {
			break
		}
		time.Sleep(backoffs[attempt])
	}

	return nil, lastErr
}
