package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"github.com/patrickbucher/meow"
	"github.com/valkey-io/valkey-go"
)

func main() {
	valkeyURL, ok := os.LookupEnv("VALKEY_URL")
	if !ok {
		fmt.Fprintln(os.Stderr, "environment variable VALKEY_URL must be set")
		os.Exit(1)
	}
	println(valkeyURL)
	options := valkey.ClientOption{
		InitAddress: []string{"valkey.frickelcloud.ch:6379"},
		SelectDB:    27,
	}
	client, err := valkey.NewClient(options)
	if err != nil {
		log.Fatalf("connect to Valkey: %v", err)
	}
	defer client.Close()

	addr := flag.String("addr", "localhost", "listen to address")
	port := flag.Uint("port", 8000, "listen on port")
	flag.Parse()

	log.SetOutput(os.Stderr)

	http.HandleFunc("/endpoints/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getEndpoint(w, r, client)
		case http.MethodPost:
			postEndpoint(w, r, client)
		default:
			log.Printf("request from %s rejected: method %s not allowed",
				r.RemoteAddr, r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	http.HandleFunc("/endpoints", func(w http.ResponseWriter, r *http.Request) {
		getEndpoints(w, r, client)
	})

	listenTo := fmt.Sprintf("%s:%d", *addr, *port)
	log.Printf("listen to %s", listenTo)
	http.ListenAndServe(listenTo, nil)
}

func getEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)
	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		log.Printf("extract endpoint identifier of %s: %v", r.URL, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	key := fmt.Sprintf("endpoint:%s", identifier)

	kvs, err := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil || len(kvs) == 0 {
		log.Printf(`no such endpoint "%s"`, identifier)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	payload := meow.EndpointPayload{
		Identifier:   kvs["identifier"],
		URL:          kvs["url"],
		Method:       kvs["method"],
		StatusOnline: parseUint16(kvs["status_online"]),
		Frequency:    kvs["frequency"],
		FailAfter:    parseUint8(kvs["fail_after"]),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("serialize payload: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func postEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	log.Printf("POST %s from %s", r.URL, r.RemoteAddr)
	buf := bytes.NewBufferString("")
	io.Copy(buf, r.Body)
	defer r.Body.Close()

	endpoint, err := meow.EndpointFromJSON(buf.String())
	if err != nil {
		log.Printf("parse JSON body: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	identifierPathParam, err := extractEndpointIdentifier(r.URL.String())
	if err == nil && identifierPathParam != endpoint.Identifier {
		log.Printf("identifier mismatch: (resource: %s, body: %s)",
			identifierPathParam, endpoint.Identifier)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	key := fmt.Sprintf("endpoint:%s", endpoint.Identifier)

	exists, err := vk.Do(ctx, vk.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		log.Printf("check existence: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	cmd := vk.B().Hset().Key(key).
		FieldValue().
		FieldValue("identifier", endpoint.Identifier).
		FieldValue("url", endpoint.URL.String()).
		FieldValue("method", endpoint.Method).
		FieldValue("status_online", strconv.Itoa(int(endpoint.StatusOnline))).
		FieldValue("frequency", endpoint.Frequency.String()).
		FieldValue("fail_after", strconv.Itoa(int(endpoint.FailAfter))).
		Build()

	if err := vk.Do(ctx, cmd).Error(); err != nil {
		log.Printf("store endpoint: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if exists > 0 {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
}

func getEndpoints(w http.ResponseWriter, r *http.Request, vk valkey.Client) {
	if r.Method != http.MethodGet {
		log.Printf("request from %s rejected: method %s not allowed",
			r.RemoteAddr, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	log.Printf("GET %s from %s", r.URL, r.RemoteAddr)

	ctx := context.Background()

	keys, err := vk.Do(ctx, vk.B().Keys().Pattern("endpoint:*").Build()).AsStrSlice()
	if err != nil {
		log.Printf("get keys for endpoint:*: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	payloads := make([]meow.EndpointPayload, 0)

	for _, key := range keys {
		kvs, err := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
		if err != nil {
			log.Printf("hgetall %s: %v", key, err)
			continue
		}

		payload := meow.EndpointPayload{
			Identifier:   kvs["identifier"],
			URL:          kvs["url"],
			Method:       kvs["method"],
			StatusOnline: parseUint16(kvs["status_online"]),
			Frequency:    kvs["frequency"],
			FailAfter:    parseUint8(kvs["fail_after"]),
		}
		payloads = append(payloads, payload)
	}

	data, err := json.Marshal(payloads)
	if err != nil {
		log.Printf("serialize payloads: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

const endpointIdentifierPatternRaw = "^/endpoints/([a-z][-a-z0-9]+)$"

var endpointIdentifierPattern = regexp.MustCompile(endpointIdentifierPatternRaw)

func extractEndpointIdentifier(endpoint string) (string, error) {
	matches := endpointIdentifierPattern.FindStringSubmatch(endpoint)
	if len(matches) == 0 {
		return "", fmt.Errorf(`endpoint "%s" does not match pattern "%s"`,
			endpoint, endpointIdentifierPatternRaw)
	}
	return matches[1], nil
}

func parseUint16(s string) uint16 {
	val, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0
	}
	return uint16(val)
}

func parseUint8(s string) uint8 {
	val, err := strconv.ParseUint(s, 10, 8)
	if err != nil {
		return 0
	}
	return uint8(val)
}
