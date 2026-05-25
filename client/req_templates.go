package main

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

type reqTemplate struct {
	method   string
	path     string
	payload  string
	expected string
	raw      string
	lineno   int
}

func getTemplates(filename string) ([]reqTemplate, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open %q: %w", filename, err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	lineno := 0
	var out []reqTemplate

	for s.Scan() {
		lineno++

		str := strings.TrimSpace(s.Text())
		if str == "" || strings.HasPrefix(str, "#") {
			continue
		}

		template, err := parse(str)
		if err != nil {
			return nil, fmt.Errorf("unable to build template using line %d: %w", lineno, err)
		}
		template.lineno = lineno
		out = append(out, template)
	}

	if err = s.Err(); err != nil {
		return nil, fmt.Errorf("failed to read req templates: %w", err)
	}
	return out, nil
}

// assumes format: HTTP_METHOD key value (PUT abcyx 1341)
func parse(str string) (reqTemplate, error) {
	fields := strings.Fields(str)
	if len(fields) == 0 {
		return reqTemplate{}, errors.New("invalid str: empty lines are not supported")
	}
	if len(fields) < 2 {
		return reqTemplate{}, errors.New("invalid str: must have method and key defined")
	}

	method := strings.ToUpper(fields[0])
	path := "/" + fields[1]

	switch method {
	case http.MethodGet:
		if len(fields) < 3 {
			return reqTemplate{}, fmt.Errorf("invalid GET: missing expected response value, raw: %s", str)
		}
		return reqTemplate{
			method:   method,
			path:     path,
			expected: fields[2],
			raw:      str,
		}, nil

	case http.MethodPut:
		if len(fields) < 3 {
			return reqTemplate{}, fmt.Errorf("invalid PUT: missing payload value, raw: %s", str)
		}
		return reqTemplate{
			method:  method,
			path:    path,
			payload: fields[2],
			raw:     str,
		}, nil

	case http.MethodDelete:
		return reqTemplate{
			method: method,
			path:   path,
			raw:    str,
		}, nil

	default:
		return reqTemplate{}, fmt.Errorf("unsupported method %q, raw: %s", method, str)
	}
}
