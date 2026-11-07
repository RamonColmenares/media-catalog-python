"""Optional RabbitMQ adapter for the same validated movie write path."""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import sqlite3
from pydantic import ValidationError

from media_catalog.content_repository import SourceRepository, payload_hash
from media_catalog.database import Database
from media_catalog.errors import DomainError
from media_catalog.models import MovieCreate

logger = logging.getLogger("media_catalog.broker")


def handle_movie_message(body: bytes, database: Database, idempotency_key: str | None = None) -> dict[str, Any]:
    """Validate and persist one message, retaining its response for redelivery."""
    payload = json.loads(body.decode("utf-8"))
    movie = MovieCreate.model_validate(payload)
    stable_key = idempotency_key or f"rabbitmq:{payload_hash(movie.model_dump(by_alias=True))}"
    return SourceRepository(database, "movie").create(movie, actor="rabbitmq", idempotency_key=stable_key)


def process_movie_delivery(ch: Any, method: Any, properties: Any, body: bytes, database: Database) -> None:
    message_id = getattr(properties, "message_id", None)
    key = f"rabbitmq:{payload_hash({'messageId': str(message_id)})}" if message_id else None
    try:
        handle_movie_message(body, database, key)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as error:
        logger.warning("Rejected invalid movie message type=%s", error.__class__.__name__)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except DomainError as error:
        if error.status_code < 500:
            logger.warning("Rejected conflicting movie message code=%s", error.code)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        else:
            logger.warning("Movie message persistence unavailable code=%s", error.code)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as error:
        logger.warning("Movie database operation failed type=%s", error.__class__.__name__)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    except Exception as error:
        logger.warning("Movie message processing failed type=%s", error.__class__.__name__)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)


def consume_movie_queue(
    database_path: str | None = None,
    broker_url: str | None = None,
    queue_name: str | None = None,
) -> None:
    """Read the durable Movie queue; broker credentials stay in environment state."""
    url = broker_url or os.getenv("RABBITMQ_URL")
    if not url:
        raise ValueError("Set RABBITMQ_URL to enable RabbitMQ ingestion")
    path = database_path or os.getenv("MOVIE_DB_PATH", "./data/movies.sqlite3")
    queue = queue_name or os.getenv("MOVIE_QUEUE_NAME", "Movie")
    try:
        import pika
    except ImportError as error:
        raise RuntimeError("Install media-catalog-python[rabbitmq] to consume RabbitMQ") from error

    database = Database(path, role="movie")
    database.initialize()
    connection = pika.BlockingConnection(pika.URLParameters(url))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch: Any, method: Any, properties: Any, body: bytes) -> None:
        process_movie_delivery(ch, method, properties, body, database)

    logger.info("Consuming configured RabbitMQ movie queue")
    channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=False)
    try:
        channel.start_consuming()
    finally:
        if connection.is_open:
            connection.close()
