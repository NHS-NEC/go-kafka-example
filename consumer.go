package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"metadata.broker.list": "dory-01.srvs.cloudkafka.com:9094,dory-02.srvs.cloudkafka.com:9094,dory-03.srvs.cloudkafka.com:9094",
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      "SCRAM-SHA-256",
		"sasl.username":        "twww7nib",
		"sasl.password":        "0HJaKQnDyy1AUzvDe3RDBcGmXfPeoxm5",
		"group.id":             os.Getenv("CLOUDKARAFKA_GROUPID"),
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},
		//"debug":                           "generic,broker,security",
	}
	topic := os.Getenv("CLOUDKARAFKA_TOPIC_PREFIX") + ".test"

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Consumer %v\n", c)
	err = c.Subscribe(topic, nil)
	run := true
	counter := 0
	commitAfter := 1000
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s: %s\n", e.TopicPartition, string(e.Value))
				counter++
				if counter > commitAfter {
					c.Commit()
					counter = 0
				}

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	c.Close()
}
