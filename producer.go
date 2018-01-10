// Package kafkalog defines Send method to stream your logs to kafka.
// Must have "KAFKA_BROKERS" env var formated "127.0.0.1:9092,127.0.0.1:9092"
// Implement with the following code:
// old := os.Stdout
// r, w, err := os.Pipe()
// if err != nil {
// 	panic("Error running os.Pipe()")
// }
// os.Stdout = w
// lw := kafkalog.NewAsyncProducer("loggerID")
// defer func() {
// 	w.Close()
// 	os.Stdout = old
// 	lw.Close()
// }()
// go func() {
// 	sc := bufio.NewScanner(r)
// 	for sc.Scan() {
// 		l := sc.Text()
// 		lw.Send(l)
// 	}
// }()
package kafkalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	topic = "logs"
	// AP implements sarama.AsyncProducer.
	AP = AsyncProducer{}
)

// IAsyncProducer exposes writing msg to kafka stream.
type IAsyncProducer interface {
	Send(msg string)
	Close()
}

func conf() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.NoResponse
	conf.Producer.Return.Successes = false
	conf.Producer.Return.Errors = true
	// do not compress short strings.
	// conf.Producer.Compression = sarama.CompressionLZ4
	conf.Producer.Compression = sarama.CompressionGZIP
	conf.ChannelBufferSize = 256    // default value is 256
	conf.Version = sarama.V0_11_0_0 // kafka version
	return conf
}

// AsyncProducer implements sarama.AsyncProducer
type AsyncProducer struct {
	loggerID string
	conf     *sarama.Config
	brokers  []string
	prodr    sarama.AsyncProducer
}

// NewAsyncProducer return AsyncProducer as IAsyncProducer.
func NewAsyncProducer(loggerID string) IAsyncProducer {
	ap := AsyncProducer{
		loggerID: loggerID,
		conf:     conf(),
	}
	var err error
	if ap.brokers, err = getBrokersFromEnv(); err != nil {
		panic(err.Error())
	}
	if err := GetSaramaProducer(&ap); err != nil {
		panic(err.Error())
	}
	ap.init()
	return &ap
}

func getBrokersFromEnv() ([]string, error) {
	brokersStr := os.Getenv("KAFKA_BROKERS")
	if len(brokersStr) == 0 {
		return nil, errors.New("NO_KAFKA_BROKERS_ARG_IN_ENV")
	}
	return strings.Split(brokersStr, ","), nil
}

// GetSaramaProducer factores out sarama.NewAsyncProducer() for easy testing.
var GetSaramaProducer = func(p *AsyncProducer) error {
	var err error
	// p.prodr, err = sarama.NewAsyncProducer(p.brokers, p.conf)
	p.prodr, err = sarama.NewAsyncProducer(p.brokers, p.conf)
	if err != nil {
		return err
	}
	return nil
}

func (p *AsyncProducer) init() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, os.Kill)

	go func() {
		<-c
		if err := p.prodr.Close(); err != nil {
			log.Fatal("Error closing kafka AsyncProducer", err)
		}

		log.Println("kafka AsyncProducer producer closed")
		os.Exit(1)
	}()
	go func() {
		for err := range p.prodr.Errors() {
			log.Println("Failed to write message to topic: ", err)
		}
	}()
}

// Send sends kafka message to a topic.
func (p *AsyncProducer) Send(msg string) {
	m := fmt.Sprintf("[%s] [%s] %s",
		time.Now().Format(time.RFC3339), p.loggerID, msg)
	msgBytes, err := json.Marshal(m)
	if err != nil {
		log.Println("Failed json encoding kafka log msg: ", err)
		return
	}
	msgLog := sarama.ProducerMessage{
		Topic: topic,
		// Key:       sarama.StringEncoder(),
		Timestamp: time.Now(),
		Value:     sarama.ByteEncoder(msgBytes),
	}
	p.prodr.Input() <- &msgLog
}

// Close closes the producer.
func (p *AsyncProducer) Close() {
	if err := p.prodr.Close(); err != nil {
		log.Fatal("Error closing kafka AsyncProducer", err)
	}
}
