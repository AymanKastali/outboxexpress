"""Reading and nudging outbox row state, for the relay's tests.

Plain functions rather than fixtures, so they go in a sibling module and are imported
normally -- see spec 9.5.
"""

from sqlalchemy import Engine, text


def statuses(engine: Engine) -> list[str]:
    """Every row's status, in the order the claim would take them."""
    with engine.connect() as connection:
        return list(
            connection.scalars(text("SELECT status FROM outbox ORDER BY next_attempt_at, id"))
        )


def expire_leases(engine: Engine) -> None:
    """Age every held lease past its expiry, as if the relay holding it had stalled.

    Sleeping through a real lease would cost a second per test and be flaky at the
    boundary. Moving the clock instead is what ``test_a_row_not_yet_due_is_left_alone``
    already does to ``next_attempt_at``; this is the same move on ``locked_until``.
    """
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE outbox SET locked_until = now() - interval '1 second' "
                "WHERE status = 'publishing'"
            )
        )
