"""Publishes simulated fleet telemetry to Kafka.

  python -m producer.produce --backfill-hours 6 --interval 60

Backfill replays N hours of history as fast as possible (simulated clock), then
switches to live mode, emitting one tick every --interval seconds of wall time.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import timedelta

from confluent_kafka import Producer

from producer.simulator import FleetSimulator, utcnow

log = logging.getLogger("producer")


def build_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "linger.ms": 50,
            "compression.type": "lz4",
            "acks": "all",
        }
    )


def send(producer: Producer, messages) -> int:
    for topic, key, value in messages:
        while True:
            try:
                producer.produce(topic, key=key, value=value)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: let it drain
    producer.poll(0)
    return len(messages)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"))
    parser.add_argument("--backfill-hours", type=float, default=6)
    parser.add_argument("--interval", type=float, default=60, help="seconds between ticks")
    parser.add_argument("--duration-minutes", type=float, default=0, help="0 = run forever")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sim = FleetSimulator(seed=args.seed)
    producer = build_producer(args.bootstrap)

    now = utcnow()
    sim_clock = now - timedelta(hours=args.backfill_hours)
    # The fleet exists before telemetry starts; otherwise late-arriving metrics
    # from the first minutes would predate every node's first dim version.
    sent = send(producer, sim.initial_inventory(sim_clock - timedelta(hours=1)))

    step = timedelta(seconds=args.interval)
    while sim_clock < now:
        sim_clock += step
        sent += send(producer, sim.tick(sim_clock))
    producer.flush()
    log.info("backfill complete: %d messages up to %s", sent, sim_clock.isoformat())

    deadline = time.time() + args.duration_minutes * 60 if args.duration_minutes else None
    while deadline is None or time.time() < deadline:
        time.sleep(args.interval)
        n = send(producer, sim.tick(utcnow()))
        sent += n
        log.info("live tick: %d messages (total %d)", n, sent)
    producer.flush()


if __name__ == "__main__":
    main()
