"""A single-node Apache Kafka broker for the suite.

testcontainers-python ships only a Confluent-image container, and upstream
Testcontainers has since deprecated that shape in favour of one container for
``apache/kafka`` and ``apache/kafka-native``. This is their current recipe, ported.
Delete this file the day testcontainers-python ships its own.
"""

import re
import tarfile
import time
from io import BytesIO
from typing import Self

from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy


class ApacheKafkaContainer(DockerContainer):
    """A single-node KRaft broker, ready once ``start()`` returns."""

    START_SCRIPT = "/tmp/testcontainers_start.sh"
    CLUSTER_ID = "4L6g3nShT-eMCtK--X86sw"
    CLIENT_PORT = 9092
    BROKER_PORT = 9093
    CONTROLLER_PORT = 9094

    def __init__(self, image: str = "apache/kafka-native:4.3.1") -> None:
        super().__init__(image)
        self.with_exposed_ports(self.CLIENT_PORT)
        # The image waits for a script that cannot be written until the host port is
        # known -- see start(). Until then it must not launch.
        self.with_command(
            f'sh -c "while [ ! -f {self.START_SCRIPT} ]; do sleep 0.1; done; '
            f'sh {self.START_SCRIPT}"'
        )
        listeners = (
            f"PLAINTEXT://0.0.0.0:{self.CLIENT_PORT},"
            f"BROKER://0.0.0.0:{self.BROKER_PORT},"
            f"CONTROLLER://0.0.0.0:{self.CONTROLLER_PORT}"
        )
        for name, value in {
            "CLUSTER_ID": self.CLUSTER_ID,
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": listeners,
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
            ),
            "KAFKA_INTER_BROKER_LISTENER_NAME": "BROKER",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@localhost:{self.CONTROLLER_PORT}",
            # One node, so every internal topic is RF=1. Spec 9.3 states this gap.
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        }.items():
            self.with_env(name, value)

    def bootstrap_server(self) -> str:
        host = self.get_container_host_ip()
        return f"{host}:{self.get_exposed_port(self.CLIENT_PORT)}"

    def start(self) -> Self:
        super().start()
        # A client reaching the broker on a mapped port must be advertised that port,
        # and Docker only assigns it once the container is up -- hence the wait loop in
        # the command above and this script written into the running container.
        script = (
            "#!/bin/bash\n"
            f"export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{self.bootstrap_server()},"
            f"BROKER://$(hostname -i):{self.BROKER_PORT}\n"
            "/etc/kafka/docker/run\n"
        ).encode()
        self._put_file(script, self.START_SCRIPT)
        # "Kafka Server started" is the last line the broker logs, after "Awaiting
        # socket connections". Earlier markers -- the RECOVERY to RUNNING transition, for
        # one -- are printed before the listener accepts connections, so a client handed
        # bootstrap_server() at that point can be refused.
        strategy = LogMessageWaitStrategy(re.compile(r".*Kafka Server started.*"))
        strategy.with_startup_timeout(90)
        strategy.wait_until_ready(self)
        return self

    def _put_file(self, content: bytes, path: str) -> None:
        with BytesIO() as archive, tarfile.TarFile(fileobj=archive, mode="w") as tar:
            entry = tarfile.TarInfo(name=path)
            entry.size = len(content)
            entry.mtime = int(time.time())
            tar.addfile(entry, BytesIO(content))
            archive.seek(0)
            # docker-py ships no type information, so put_archive's signature is only
            # partially known to pyright. The boundary is one call wide.
            self.get_wrapped_container().put_archive(  # pyright: ignore[reportUnknownMemberType]
                "/", archive
            )
