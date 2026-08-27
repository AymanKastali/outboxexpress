// Package kafkatest gives an integration test a real Apache Kafka 4.3.1.
//
// One container per test binary, with a topic per test rather than a shared one:
// a topic is cheap, and reading only your own records is stronger isolation than
// any amount of offset arithmetic on a shared topic.
//
// It does not use testcontainers' own Kafka module. That module is written for
// confluentinc/confluent-local — its starter script sources Confluent's
// /etc/confluent/docker/bash-config and calls Confluent's configure and launch
// scripts, none of which exist in apache/kafka, and its version check silently
// declines to validate any non-Confluent image rather than rejecting it. Spec
// §17 pins Apache Kafka 4.3.1, and a suite that runs a different distribution
// from the one deploy/ ships is testing something else.
//
// This file carries no build tag, for the reason pgtest's doc comment gives: a
// package whose every file is excluded by a constraint makes `go build ./...`
// fail, so the tag goes on the tests that import this package, never here.
package kafkatest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	platformkafka "github.com/AymanKastali/outboxexpress/internal/platform/kafka"
)

const (
	image = "apache/kafka:4.3.1"

	// advertisedListenersPath is where the post-start hook drops one line of
	// shell. The container's entrypoint waits for that file before starting the
	// broker, because a broker has to advertise a listener the client can
	// actually reach and the host port is not known until the container is
	// already running. This is the same technique testcontainers' own Kafka
	// module uses, written for the Apache image's entrypoint instead of
	// Confluent's.
	advertisedListenersPath = "/tmp/oe-advertised-listeners"

	// readTimeout bounds Records. It is a deadline, not a sleep: Records polls
	// and fails with what it actually saw (spec §15).
	readTimeout = 15 * time.Second
)

// The container is started once and never terminated explicitly: testcontainers'
// reaper removes it when the test binary exits, which is the only moment at
// which terminating it is safe. The admin client is amortised the same way — a
// fresh kgo.NewClient per topic pays a TCP connect, an ApiVersions round trip
// and a metadata fetch to issue one CreateTopics.
var (
	instance = sync.OnceValues(start)
	adminer  = sync.OnceValues(func() (*kadm.Client, error) {
		broker, err := instance()
		if err != nil {
			return nil, err
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(broker))
		if err != nil {
			return nil, fmt.Errorf("kafkatest: admin client: %w", err)
		}
		return kadm.NewClient(client), nil
	})
)

// Broker returns the bootstrap address of a running broker.
func Broker(t *testing.T) string {
	t.Helper()
	broker, err := instance()
	if err != nil {
		t.Fatalf("kafkatest: %v", err)
	}
	return broker
}

// Producer returns a client built the way production builds one.
//
// It goes through platformkafka.NewProducer for the reason pgtest.Accounts goes
// through platformpg.NewPool: a suite that constructs its clients differently
// from the process under test leaves the constructor — and every durability
// option in it — unexercised by every integration test that needs a client.
func Producer(t *testing.T) *kgo.Client {
	t.Helper()
	client, err := platformkafka.NewProducer(
		platformkafka.DefaultProducerConfig([]string{Broker(t)}))
	if err != nil {
		t.Fatalf("kafkatest: producer: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// Topic creates a topic named after the calling test and returns its name.
//
// Auto-creation is disabled on the broker (spec §12.1 needs
// UNKNOWN_TOPIC_OR_PARTITION to be a real, reachable error), so every test that
// wants a topic has to ask for one — which is the point.
func Topic(t *testing.T, partitions int32) string {
	t.Helper()
	name := topicName(t)

	admin, err := adminer()
	if err != nil {
		t.Fatalf("kafkatest: %v", err)
	}

	resp, err := admin.CreateTopic(context.Background(), partitions, 1, nil, name)
	if err != nil {
		t.Fatalf("kafkatest: create topic %s: %v", name, err)
	}
	if resp.Err != nil {
		t.Fatalf("kafkatest: create topic %s: %v", name, resp.Err)
	}
	return name
}

// Records consumes want records from the start of topic and returns them in the
// order the consumer saw them.
//
// It exists because every test that produces has to read back, and a poll loop
// hand-rolled per test is where a suite starts sleeping. It fails with the count
// it actually got, so a test that under-produces says so instead of timing out
// anonymously.
func Records(t *testing.T, topic string, want int) []*kgo.Record {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(Broker(t)),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kafkatest: consumer: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), readTimeout)
	defer cancel()

	var got []*kgo.Record
	for len(got) < want {
		fetches := client.PollRecords(ctx, want-len(got))
		if err := fetches.Err0(); err != nil {
			t.Fatalf("kafkatest: read %s: got %d of %d records: %v",
				topic, len(got), want, err)
		}
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, r) })
	}
	return got
}

// topicName derives a legal topic name from the test's name. Kafka accepts
// [a-zA-Z0-9._-] only, and t.Name() contains a slash for every subtest.
func topicName(t *testing.T) string {
	return "test." + strings.Map(func(r rune) rune {
		if r == '.' || r == '_' || r == '-' ||
			r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, t.Name())
}

func start() (string, error) {
	ctx := context.Background()

	container, err := testcontainers.Run(ctx, image,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":       "1",
			"KAFKA_PROCESS_ROLES": "broker,controller",

			// Three listeners: PLAINTEXT is the one the host reaches through the
			// mapped port, BROKER is what the broker advertises to itself, and
			// CONTROLLER is KRaft's own. A single listener cannot do this,
			// because the address the host must use and the address the broker
			// must use are different.
			"KAFKA_LISTENERS":                      "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":     "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":      "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":       "1@localhost:9093",

			// Single broker: nothing can be replicated further than one.
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",

			// Spec §12.1 classifies UNKNOWN_TOPIC_OR_PARTITION as permanent
			// "with auto-creation disabled". With auto-creation on, that branch
			// is unreachable and the classification is untestable.
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false",

			// A consumer group in a test should not wait out a rebalance window
			// that exists to batch real deployments.
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
		}),
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c",
			"while [ ! -f "+advertisedListenersPath+" ]; do sleep 0.1; done; "+
				". "+advertisedListenersPath+"; exec /etc/kafka/docker/run"),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{advertiseMappedPort},
		}),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Kafka Server started").WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		return "", fmt.Errorf("kafkatest: start %s: %w", image, err)
	}
	return brokerAddress(ctx, container)
}

// advertiseMappedPort writes the advertised-listener line the entrypoint is
// waiting for, now that the mapped port exists.
func advertiseMappedPort(ctx context.Context, container testcontainers.Container) error {
	address, err := brokerAddress(ctx, container)
	if err != nil {
		return err
	}
	line := fmt.Sprintf(
		"export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s,BROKER://localhost:9094\n",
		address)
	if err := container.CopyToContainer(ctx, []byte(line), advertisedListenersPath, 0o755); err != nil {
		return fmt.Errorf("kafkatest: advertise %s: %w", address, err)
	}
	return nil
}

func brokerAddress(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("kafkatest: host: %w", err)
	}
	port, err := container.MappedPort(ctx, "9092/tcp")
	if err != nil {
		return "", fmt.Errorf("kafkatest: mapped port: %w", err)
	}
	return fmt.Sprintf("%s:%s", host, port.Port()), nil
}
