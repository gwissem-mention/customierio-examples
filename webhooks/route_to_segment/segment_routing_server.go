package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/analytics-go"
)

type ConfigEnv struct {
	SegmentWriteKey string `json:"segment_write_key"`
}

type Config struct {
	Envs map[string]ConfigEnv `json:"environments"`
}

type Webhook struct {
	EventSourceNil  *string                `json:"event_source"`
	EventType       string                 `json:"event_type"`
	EventID         string                 `json:"event_id"`
	TimestampNil    *int                   `json:"timestamp"`
	TimestampIsoNil *string                `json:"timestamp_iso"`
	Data            map[string]interface{} `json:"data"`
}

type Action interface {
	Unmarshal(data []byte) error
	Send(client *analytics.Client) error
}

type Identify struct {
	identify *analytics.Identify
}

type Track struct {
	track *analytics.Track
}

func (w *Webhook) EventSource() string {
	if s := w.EventSourceNil; s != nil {
		return *s
	}
	return "customerio"
}

func (w *Webhook) TimestampRFC3339() string {
	if ts := w.TimestampNil; ts != nil {
		return time.Unix(int64(*ts), 0).Format(time.RFC3339)
	}
	if ts := w.TimestampIsoNil; ts != nil {
		return *ts
	}
	return time.Now().Format(time.RFC3339)
}

func (i *Identify) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &i.identify)
}
func (i *Identify) Send(client *analytics.Client) error {
	return client.Identify(i.identify)
}

func (i *Track) Unmarshal(data []byte) error {
	return json.Unmarshal(data, &i.track)
}
func (i *Track) Send(client *analytics.Client) error {
	return client.Track(i.track)
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

func handle(action Action, config *Config, w http.ResponseWriter, r *http.Request) {

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

	log.Println(string(buf))

	if err := action.Unmarshal(buf); err != nil {
		log.Println(err, r)
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte("bad request"))
		return
	}

	segment := analytics.New(envConfig.SegmentWriteKey)

	if err := action.Send(segment); err != nil {
		msg := fmt.Sprintf("action.Send failed: %s", err)
		log.Print(err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	log.Println("ok", r)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
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

		log.Println(string(buf))

		var webhook *Webhook
		err := json.Unmarshal(buf, &webhook)

		if err != nil {
			log.Println(err, r)
			w.WriteHeader(http.StatusNotAcceptable)
			w.Write([]byte("bad request"))
			return
		}

		delete(webhook.Data, "variables")

		if webhook.Data["customer_id"] == nil {
			msg := "data.customer_id is nil"
			log.Print(err)
			http.Error(w, msg, http.StatusNotAcceptable)
			return
		}
		customerID := webhook.Data["customer_id"].(string)

		var eventType, webhookEventType string
        webhookEventType = webhook.EventType
		eventType = webhookEventType
         
		if webhookEventType == "customer_unsubscribed" {
			eventType = "Email - unsubscribed"
		} else if webhookEventType == "email_converted" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		} else if webhookEventType == "email_drafted" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		} else if webhookEventType == "email_dropped" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		} else if webhookEventType == "email_delivered" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok"))
			return
		} else if webhookEventType == "email_bounced" {
			eventType = "Email - email failed"
		} else if webhookEventType == "email_failed" {
			eventType = "Email - email failed"
		} else if webhookEventType == "email_spammed" {
			eventType = "Email - email failed"
		} else if webhookEventType == "email_sent" {
			eventType = "Email - email sent"
		} else if webhookEventType == "email_opened" {
			eventType = "Email - opened email"
		} else if webhookEventType == "email_clicked" {
			eventType = "Email - clicked email"
		} 

		segment := analytics.New(envConfig.SegmentWriteKey)

		err = segment.Track(&analytics.Track{
			UserId:     customerID,
			Event:      eventType,
			Properties: webhook.Data,
			Context: map[string]interface{}{
				"event_id": webhook.EventID,
			},
			Message: analytics.Message{
				Timestamp: webhook.TimestampRFC3339(),
			},
		})

		if err != nil {
			msg := fmt.Sprintf("segment.Track failed: %s", err)
			log.Print(err)
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}

		log.Println("ok", r)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	http.HandleFunc("/webhook/identify", func(w http.ResponseWriter, r *http.Request) {
		handle(&Identify{}, config, w, r)
	})

	http.HandleFunc("/webhook/track", func(w http.ResponseWriter, r *http.Request) {
		handle(&Track{}, config, w, r)
	})

	log.Print("Listening on :8080 for incoming webhooks to forward to segment.com")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
