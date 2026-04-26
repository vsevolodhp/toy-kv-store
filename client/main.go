package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	lines, err := readLines("puts.txt")
	if err != nil {
		log.Fatal("unable to read requests: %w", err)
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
	f, err := os.OpenFile("latency_sample.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("unable to create latency_sample: %w", err)
	}
	defer f.Close()
	f.WriteString("Sample,Latency_Secs\n")

	for _, l := range lines {
		wg.Add(1)
		sem <- struct{}{}
		switch l.method {
		case "GET":
			go func(l line) {
				defer wg.Done()
				defer func() { <-sem }()

				start := time.Now()
				doGET(client, baseUrl, l)
				fmt.Fprintf(f, "%d,%.4f\n", l.lineno, time.Since(start).Seconds())
			}(l)
		case "PUT":
			go func(l line) {
				defer wg.Done()
				defer func() { <-sem }()

				start := time.Now()
				doPUT(client, baseUrl, l)
				fmt.Fprintf(f, "%d,%.4f\n", l.lineno, time.Since(start).Seconds())
			}(l)
		case "DELETE":
			go func(l line) {
				defer wg.Done()
				defer func() { <-sem }()

				start := time.Now()
				doDELETE(client, baseUrl, l)
				fmt.Fprintf(f, "%d,%.4f\n", l.lineno, time.Since(start).Seconds())
			}(l)
		default:
			<-sem
			wg.Done()
		}
	}
	wg.Wait()
	log.Print("all lines have been processed")
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

func readLines(filename string) ([]line, error) {
	f, err := os.Open(filename)
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

		// ignore line breaks and commented lines
		if str == "" || strings.HasPrefix(str, "#") {
			continue
		}

		l, err := parseLine(str)
		l.lineno = lineno
		if err != nil {
			log.Printf("unable to parse line #%d: %s", lineno, err)
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
		log.Printf("unable to build GET request #%d: %v", l.lineno, err)
		return
	}
	resp, err := doWithRetry(client, req)
	if err != nil {
		log.Printf("request #%d failed: %v", l.lineno, err)
		return
	}
	defer resp.Body.Close()
	if l.expectNotFound {
		if resp.StatusCode != http.StatusNotFound {
			log.Printf("expected: NOT_FOUND, got: %q, line: %d", resp.Status, l.lineno)
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("expected: OK, got: %q, line: %d", resp.Status, l.lineno)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if got != l.expected {
		log.Printf("expected: %q, got: %q, line: %d", l.expected, got, l.lineno)
	}
}

func doPUT(client *http.Client, baseUrl string, l line) {
	req, err := http.NewRequest("PUT", baseUrl+l.path, strings.NewReader(l.payload))
	if err != nil {
		log.Printf("unable to build PUT request #%d: %v", l.lineno, err)
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := doWithRetry(client, req)
	if err != nil {
		log.Printf("request #%d failed: %v", l.lineno, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("expected: OK, got: %q, line: %d", resp.Status, l.lineno)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if got != l.payload {
		log.Printf("expected: %q, got: %q, line: %d", l.expected, got, l.lineno)
	}
}

func doDELETE(client *http.Client, baseUrl string, l line) {
	req, err := http.NewRequest("DELETE", baseUrl+l.path, nil)
	if err != nil {
		log.Printf("unable to build DELETE request #%d: %v", l.lineno, err)
		return
	}
	resp, err := doWithRetry(client, req)
	if err != nil {
		log.Printf("request #%d failed: %v", l.lineno, err)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Printf("expected: OK, got: %q, line: %d", resp.Status, l.lineno)
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
