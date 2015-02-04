package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/segmentio/analytics-go"
	"log"
	"net/http"
	"os"
	"time"
)

type ConfigEnv struct {
	SegmentWriteKey string `json:"segment_write_key"`
}

type Config struct {
	Envs map[string]ConfigEnv `json:"environments"`
}

type CIOWebhook struct {
	EventType string                 `json:"event_type"`
	EventID   string                 `json:"event_id"`
	Timestamp int                    `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

func loadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("loadConfig: %v", err)
	}

	decoder := json.NewDecoder(f)
	config := &Config{}

	if err := decoder.Decode(config); err != nil {
		return nil, fmt.Errorf("loadConfig: %v", err)
	}

	return config, nil
}

func main() {

	configPath := flag.String("config", "./config.json", "Path to the config file")
	flag.Parse()

	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {

		query := r.URL.Query()

		env := query.Get("env")
		envConfig, ok := config.Envs[env]
		if !ok {
			msg := fmt.Sprintf("Environment %#v does not exist", env)
			log.Print(msg)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		buf := make([]byte, r.ContentLength)
		r.Body.Read(buf)

		var webhook *CIOWebhook
		err := json.Unmarshal(buf, &webhook)

		if err != nil {
			log.Println(err, r)
			w.WriteHeader(http.StatusNotAcceptable)
			w.Write([]byte("bad request"))
			return
		}

		delete(webhook.Data, "variables")

		customerID := webhook.Data["customer_id"].(string)

		segment := analytics.New(envConfig.SegmentWriteKey)
		segment.Track(map[string]interface{}{
			"userId":     customerID,
			"event":      fmt.Sprintf("customerio:%v", webhook.EventType),
			"properties": webhook.Data,
			"context": map[string]interface{}{
				"event_id": webhook.EventID,
			},
			"timestamp": time.Unix(int64(webhook.Timestamp), 0).Format(time.RFC3339),
		})

		log.Println("ok", r)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))

	})

	log.Print("Listening on :8080 for incoming webhooks to forward to segment.com")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
