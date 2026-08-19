"""Failure-injection seams (spec 9.1).

A SIGKILL cannot reliably land between a Kafka ack and a Postgres commit -- the window
is microseconds wide. These no-ops give tests a place to raise instead, which turns
"crash at exactly the worst moment" into an assertion.

``loop`` calls these through the module (``hooks.after_claim()``), never by importing
the names: monkeypatching rebinds the module attribute, and an imported name would
still point at the original function.
"""


def after_claim() -> None:
    """The rows are leased; nothing is published yet."""


def after_publish_before_mark() -> None:
    """Kafka has the events; Postgres still says publishing. Spec 6.2's open window."""


def after_mark() -> None:
    """The rows are retired."""
