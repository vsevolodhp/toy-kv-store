package main

import (
	"bufio"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

const baseUrl = "http://localhost:8080/"

type reqTemplate struct {
	method  string
	key     string
	payload string
}

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	slog.Info("starting kv client")

	reqTemplates, err := readRequests()
	if err != nil {
		slog.Error("unable to read requests", slog.Any("err", err))
		return
	}

	// TODO: replace with custom config
	client := http.DefaultClient
	for _, reqTemplate := range reqTemplates {
		switch reqTemplate.method {
		case "GET":
			doGet(client, baseUrl, reqTemplate.key, reqTemplate.payload)
		case "PUT":
			doPut(client, baseUrl, reqTemplate.key, reqTemplate.payload)
		case "DELETE":
			doDelete(client, baseUrl, reqTemplate.key)
		default:
			slog.Error("unsupported request method", slog.String("method", reqTemplate.method))
		}
	}
}

func readRequests() ([]reqTemplate, error) {
	f, err := os.Open("puts.txt")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []reqTemplate
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		fields := strings.Fields(line)
		out = append(out, reqTemplate{
			method:  strings.ToUpper(fields[0]),
			key:     fields[1],
			payload: fields[2],
		})
	}
	if err = sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func doGet(client *http.Client, baseUrl, key string, expected string) {
	req, err := http.NewRequest("GET", baseUrl+key, nil)
	if err != nil {
		slog.Error("unable to build GET request", slog.Any("error", err))
		return
	}
	resp, err := client.Do(req)
	if expected == "NOT_FOUND" {
		if resp.StatusCode != http.StatusNotFound {
			slog.Error("expected NOT_FOUND", slog.String("got", resp.Status))
		}
		return
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("expected OK", slog.String("got", resp.Status))
		return
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	got := string(body)
	if got == expected {
		return
	}
	slog.Error("response is not expected", slog.String("got", got))
}

func doPut(client *http.Client, baseUrl, key, payload string) {
	req, err := http.NewRequest("PUT", baseUrl+key, strings.NewReader(payload))
	if err != nil {
		slog.Error("unable to build GET request", slog.Any("error", err))
		return
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("PUT failed", slog.Any("error", err))
		return
	}
	if resp.StatusCode != http.StatusOK {
		slog.Error("PUT is unsuccesful", slog.Any("error", err))
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	got := string(body)
	if got != payload {
		slog.Error("PUT returned wrong value", slog.String("got", got), slog.String("expected", payload))
	}
}

func doDelete(client *http.Client, baseUrl, key string) {
	req, err := http.NewRequest("DELETE", baseUrl+key, nil)
	if err != nil {
		slog.Error("unable to build GET request", slog.Any("error", err))
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		slog.Error("unable to delete", slog.Any("error", err))
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		slog.Error("delete is unsuccesful", slog.String("status", resp.Status))
	}
}
