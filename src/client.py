#!/usr/bin/env python3
# encoding: utf-8

"""This file presents the client that can send task to the worker and wait for results."""
import asyncio
import logging
import random
from typing import Final
from uuid import uuid4

from taskiq import TaskiqMessage, BrokerMessage, TaskiqResult
from taskiq_redis import RedisAsyncResultBackend
from taskiq_redis.exceptions import ResultIsMissingError
from taskiq_redis.redis_broker import BaseRedisBroker, ListQueueBroker

logging.basicConfig(
    format="%(asctime)s|%(levelname)s: %(message)s",
    datefmt="%H:%M:%S, %d-%b-%Y",
    level=logging.DEBUG,
)

# Redefine the shared contexts again. To illustrate the importance of the isolation between client and worker
# I am deliberately avoiding import from the worker module.
REDIS_URL: Final[str] = "redis://localhost:6379/0"  # Where is the broker running?
TASK_NAME: Final[str] = 'addition'  # What is the task name?
QUEUE: Final[str] = 'sample_queue'  # Which queue to push the tasks to?
KEY_PREFIX: Final[str] = 'sample_result'  # What is the key prefix when looking up a result by task id?


async def main() -> None:
    """Use this pattern for generic dispatch without importing the task definition."""
    task_id: str = str(uuid4())  # Ensure uniqueness to avoid one task's result being overwritten by another
    mock_input: int = random.randint(a=0, b=100)  # Make the input random
    # Form the client friendly message based on the shared context
    message: TaskiqMessage = TaskiqMessage(
        task_id=task_id,
        task_name=TASK_NAME,
        labels={},
        args=[],
        # Keyword argument name and type should be part of the
        # contract/shared context between the worker and client
        kwargs={'value': mock_input},
    )
    # Format the client friendly message to broker friendly message
    # of bytes that is serialisable in Redis
    broker: BaseRedisBroker = ListQueueBroker(url=REDIS_URL, queue_name=QUEUE)  # Put the task input details here
    result_backend = RedisAsyncResultBackend(redis_url=REDIS_URL, prefix_str=KEY_PREFIX)  # Wait for results here
    broker.with_result_backend(result_backend=result_backend)
    await broker.startup()
    final_message: BrokerMessage = broker.formatter.dumps(message=message)
    await broker.kick(message=final_message)  # Place the task in the Broker queue
    logging.debug(msg=f'Triggered task id {task_id} with message {final_message}')
    # Now you can move on to other stuff if you do not care about completion status or result
    # Or you can put the task id in some persistence media if it is the responsibility
    # of some other process to check the status or result
    # The following pattern shows how to poll the status and result
    while True:
        await asyncio.sleep(delay=1)
        try:
            result: TaskiqResult = await result_backend.get_result(task_id=task_id, with_logs=True)
            if not result.is_err:
                assert result.return_value == mock_input + 1  # Make sure the result is correct
                logging.debug(msg=f"Returned value: {result.return_value}")
            else:
                logging.debug(msg="Error found while executing task.")
            break
        except ResultIsMissingError:
            # No clear enumerated status available like Celery, hence
            # absence of a result itself is the signal that the task is still incomplete
            logging.info(msg="Result not available yet, will check again later.")
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main=main())
