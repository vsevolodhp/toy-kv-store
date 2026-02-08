package main

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	lines, err := readLines()
	if err != nil {
		slog.Error("unable to read requests", slog.Any("error", err))
		return
	}

	baseUrl := "http://localhost:8080"
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
	var wg sync.WaitGroup
	sem := make(chan struct{}, 1)

	for _, l := range lines {
		wg.Add(1)
		sem <- struct{}{}
		slog.Debug("processing request", slog.Int("lineno", l.lineno))
		switch l.method {
		case "GET":
			go func(l line) {
				defer wg.Done()
				defer func() { <-sem }()

				doGET(client, baseUrl, l)
			}(l)
		case "PUT":
			go func(l line) {
				defer wg.Done()
				defer func() { <-sem }()

				doPUT(client, baseUrl, l)
			}(l)
		case "DELETE":
			go func(l line) {
				defer wg.Done()
				defer func() { <-sem }()

				doDELETE(client, baseUrl, l)
			}(l)
		default:
			<-sem
			wg.Done()

			slog.Error("unsupported request method", slog.String("method", l.method))
		}
	}
	wg.Wait()

	slog.Info("all lines have been processed")
}

type line struct {
	method         string
	path           string
	payload        string
	expected       string
	expectNotFound bool
	lineno         int
	raw            string
}

func readLines() ([]line, error) {
	f, err := os.Open("puts.txt")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []line
	sc := bufio.NewScanner(f)
	lineno := 0
	for sc.Scan() {
		lineno++
		str := strings.TrimSpace(sc.Text())
		if str == "" || strings.HasPrefix(str, "#") {
			continue
		}
		l, err := parseLine(str)
		l.lineno = lineno
		if err != nil {
			slog.Error("unable to parse line", slog.Any("error", err), slog.Int("lineno", lineno))
			continue
		}
		out = append(out, l)
	}
	if err = sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func parseLine(raw string) (line, error) {
	fields := strings.Fields(raw)
	if len(fields) == 0 {
		return line{}, fmt.Errorf("empty line")
	}
	if len(fields) < 2 {
		return line{}, fmt.Errorf("need method and key")
	}

	method := strings.ToUpper(fields[0])
	path := "/" + fields[1]

	switch method {
	case "PUT":
		return line{method: "PUT", path: path, payload: fields[2], raw: raw}, nil

	case "GET":
		if fields[2] == "NOT_FOUND" {
			return line{method: "GET", path: path, expectNotFound: true, raw: raw}, nil
		}
		return line{method: "GET", path: path, expected: fields[2], raw: raw}, nil
	case "DELETE":
		return line{method: "DELETE", path: path, raw: raw}, nil

	default:
		return line{}, fmt.Errorf("unsupported method %q", method)
	}
}

func doGET(client *http.Client, baseUrl string, l line) {
	req, err := http.NewRequest("GET", baseUrl+l.path, nil)
	if err != nil {
		slog.Error("unable to build GET request", slog.Any("error", err), slog.Any("line", l))
		return
	}
	resp, err := doWithRetry(client, req)
	if err != nil {
		slog.Error("request failed", slog.Any("error", err))
		return
	}
	defer resp.Body.Close()
	if l.expectNotFound {
		if resp.StatusCode != http.StatusNotFound {
			slog.Error("expected NOT_FOUND", slog.String("got", resp.Status), slog.Any("line", l))
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("expected OK", slog.String("got", resp.Status), slog.Any("line", l))
		return
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if got != l.expected {
		slog.Error("unexpected response", slog.String("expected", l.expected), slog.String("got", got), slog.Any("line", l))
	}
}

func doPUT(client *http.Client, baseUrl string, l line) {
	req, err := http.NewRequest("PUT", baseUrl+l.path, strings.NewReader(l.payload))
	if err != nil {
		slog.Error("unable to build PUT request", slog.Any("error", err), slog.Any("line", l))
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := doWithRetry(client, req)
	if err != nil {
		slog.Error("PUT failed", slog.Any("error", err), slog.Any("line", l))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.Error("PUT is unsuccesful", slog.Any("error", err), slog.Any("line", l))
		return
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if got != l.payload {
		slog.Error("unexpected response", slog.String("expected", l.payload), slog.String("got", got), slog.Any("line", l))
	}
}

func doDELETE(client *http.Client, baseUrl string, l line) {
	req, err := http.NewRequest("DELETE", baseUrl+l.path, nil)
	if err != nil {
		slog.Error("unable to build DELETE request", slog.Any("error", err), slog.Any("line", l))
		return
	}
	resp, err := doWithRetry(client, req)
	if err != nil {
		slog.Error("DELETE failed", slog.Any("error", err), slog.Any("line", l))
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		slog.Error("delete is unsuccesful", slog.String("status", resp.Status), slog.Any("line", l))
	}
}

func doWithRetry(client *http.Client, req *http.Request) (*http.Response, error) {
	backoffs := []time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 100 * time.Millisecond, 300 * time.Millisecond, 5 * time.Second}
	var lastErr error
	for attempt := 0; attempt < len(backoffs)+1; attempt++ {
		resp, err := client.Do(req.Clone(req.Context()))
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
