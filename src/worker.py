#!/usr/bin/env python3
# encoding: utf-8

"""
This file presents the worker. Command to fire up the worker (from project root directory)
$ uv run taskiq worker src.worker:broker --log-level DEBUG --workers 1 --reload
"""
import asyncio
import logging
import random
from typing import Final

from taskiq import TaskiqResult, AsyncTaskiqTask
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend
from taskiq_redis.redis_broker import BaseRedisBroker

logging.basicConfig(
    format="%(asctime)s|%(levelname)s: %(message)s",
    datefmt="%H:%M:%S, %d-%b-%Y",
    level=logging.DEBUG,
)

# These variables represent the shared context among workers and client
# Ideally, these should be injected by environment variables
# The only other component of the shared context is the task input names and types.
REDIS_URL: Final[str] = "redis://localhost:6379/0"  # Where is the broker running?
TASK_NAME: Final[str] = 'addition'  # What is the task name?
QUEUE: Final[str] = 'sample_queue'  # Where does the worker look for task inputs to pick up?
KEY_PREFIX: Final[str] = 'sample_result'  # What key prefix to use to store the results?

broker: BaseRedisBroker = ListQueueBroker(url=REDIS_URL, queue_name=QUEUE)  # Look for tasks here
broker.with_result_backend(
    result_backend=RedisAsyncResultBackend(redis_url=REDIS_URL, prefix_str=KEY_PREFIX))  # Put the results here


@broker.task(task_name=TASK_NAME)
async def add_one(value: int) -> int:
    """A trivial task, with an artificial delay. Your real task can be as complex as you want."""
    logging.debug(msg=f'Got value: {value} for adding one.')
    await asyncio.sleep(delay=5)
    return value + 1


async def main() -> None:
    """Use this pattern for a task dispatch when you can afford to import and wait for a task."""
    await broker.startup()
    # Send the task to the broker.
    mock_input: int = random.randint(a=0, b=100)
    task: AsyncTaskiqTask = await add_one.kiq(mock_input)
    logging.debug(msg=f'Created task: {task.task_id} with arguments: {mock_input}.')
    # Wait for the result.
    result: TaskiqResult = await task.wait_result(with_logs=True)
    logging.debug(msg=f"Task execution took: {result.execution_time} seconds.")
    if not result.is_err:
        assert result.return_value == mock_input + 1
        logging.debug(msg=f"Returned value: {result.return_value}")
    else:
        logging.debug(msg="Error found while executing task.")
    # await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main=main())
