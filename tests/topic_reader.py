"""Reading a topic back from the start, for tests that assert what was published."""

from uuid import uuid4

from confluent_kafka import Consumer, Message

READ_TIMEOUT = 30.0
# Long enough that a message in flight would have landed, short enough that asserting
# an empty topic does not cost half a minute.
QUIET_TIMEOUT = 5.0


def read_events(
    broker: str, topic: str, *, expected: int, timeout: float = READ_TIMEOUT
) -> list[Message]:
    """Read up to ``expected`` messages from the start of ``topic``.

    Returns early with fewer when the topic goes quiet, so it serves both "these
    arrived" and "nothing arrived" -- the latter with a short ``timeout``. A throwaway
    group id per call is what puts every read back at offset zero.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": f"test-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        messages: list[Message] = []
        while len(messages) < expected:
            message = consumer.poll(timeout)
            if message is None:
                break
            assert message.error() is None, message.error()
            messages.append(message)
        return messages
    finally:
        consumer.close()
