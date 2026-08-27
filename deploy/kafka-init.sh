#!/usr/bin/env bash
# Creates the topics of spec §9.2. Run as its own one-shot service rather than
# leaving the broker to auto-create them, for two reasons: auto-creation is off
# (§12.1 needs an unknown topic to be a real error), and a topic created on first
# produce gets the broker's default partition count, which would silently discard
# the partition-key ordering §9 buys.
set -euo pipefail

# The internal listener: the advertised PLAINTEXT address points at the host.
BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9094}"
TOPICS=/opt/kafka/bin/kafka-topics.sh

echo "kafka-init: waiting for ${BOOTSTRAP}"
until "$TOPICS" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
    sleep 1
done

# --if-not-exists so that `docker compose up` twice is not an error.
create() {
    local name=$1 partitions=$2 retention_ms=$3
    echo "kafka-init: ${name} (${partitions} partitions, retention ${retention_ms}ms)"
    "$TOPICS" --bootstrap-server "$BOOTSTRAP" \
        --create --if-not-exists \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config cleanup.policy=delete \
        --config "retention.ms=${retention_ms}"
}

# 3 partitions to prove that keying by aggregate_id is what buys per-entity
# ordering rather than a single partition doing it by accident (§9.2).
create accounts.user.v1 3 604800000

# The dead-letter topic of §9.2. Nothing produces to it until Plan 3; it is
# created here because topic layout is deploy configuration, not consumer code.
create accounts.user.v1.DLT 1 2592000000

echo "kafka-init: done"
"$TOPICS" --bootstrap-server "$BOOTSTRAP" --list
