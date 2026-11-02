"""Optional RabbitMQ consumer for the movie queue."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from pydantic import ValidationError

from media_catalog.database import Database
from media_catalog.models import Movie


logger = logging.getLogger("media_catalog.broker")


def handle_movie_message(body: bytes, database: Database) -> dict[str, Any]:
    """Decode a queue payload and persist it through the movie repository."""
    payload = json.loads(body.decode("utf-8"))
    movie = Movie.model_validate(payload)
    return database.save_movie(movie)


def consume_movie_queue(
    database_path: str | None = None,
    broker_url: str | None = None,
    queue_name: str | None = None,
) -> None:
    """Consume the durable queue used by the original movie service.

    Credentials belong in the `RABBITMQ_URL` environment variable and are never
    included in log messages. Install the `rabbitmq` extra to use this command.
    """
    url = broker_url or os.getenv("RABBITMQ_URL")
    if not url:
        raise ValueError("Set RABBITMQ_URL to enable RabbitMQ ingestion")
    path = database_path or os.getenv("MOVIE_DB_PATH", "./data/movies.sqlite3")
    queue = queue_name or os.getenv("MOVIE_QUEUE_NAME", "Movie")
    try:
        import pika
    except ImportError as error:
        raise RuntimeError("Install media-catalog-python[rabbitmq] to consume RabbitMQ") from error

    database = Database(path)
    database.initialize()
    connection = pika.BlockingConnection(pika.URLParameters(url))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch: Any, method: Any, properties: Any, body: bytes) -> None:
        try:
            handle_movie_message(body, database)
        except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as error:
            logger.warning("Rejected invalid movie message: %s", error.__class__.__name__)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            logger.exception("Movie message could not be persisted")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        else:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    logger.info("Consuming RabbitMQ queue %s", queue)
    channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
    try:
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()
