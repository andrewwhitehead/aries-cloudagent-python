import asyncio

from asynctest import TestCase as AsyncTestCase
from asynctest import mock as async_mock

from ..task_processor import TaskProcessor, PendingTask, Repeat


class RetryTask:
    def __init__(self, max_retries: int, result):
        self.attempts = 0
        self.max_retries = max_retries
        self.result = result

    async def run(self, pending: PendingTask):
        self.attempts += 1
        if self.attempts <= self.max_retries:
            raise Exception()
        return self.result


class TestTaskProcessor(AsyncTestCase):
    async def test_coro(self):
        collected = []

        async def test_task(val):
            collected.append(val)
            return val

        processor = TaskProcessor()
        await processor.run_task(test_task(1))
        await processor.run_task(test_task(2))
        future = await processor.run_task(test_task(3))
        result = await asyncio.wait_for(future, timeout=5.0)
        assert result == 3
        await asyncio.wait_for(processor.wait_done(), timeout=5.0)
        collected.sort()
        assert collected == [1, 2, 3]

    async def test_error(self):
        async def test_error():
            raise ValueError("test")

        processor = TaskProcessor()
        future = await processor.run_task(test_error())
        with self.assertRaises(ValueError):
            result = await asyncio.wait_for(future, timeout=5.0)
        await asyncio.wait_for(processor.wait_done(), timeout=5.0)

    async def test_processor_retry(self):
        test_value = "test_value"
        task = RetryTask(1, test_value)
        processor = TaskProcessor()
        future = await processor.run_retry(
            lambda pending: task.run(pending), limit=5, interval=0.01
        )
        result = await asyncio.wait_for(future, timeout=5.0)
        assert result == test_value
        await asyncio.wait_for(processor.wait_done(), timeout=5.0)
        assert task.attempts == 2

    async def test_repeat(self):
        count = 0
        async for task in Repeat.each(limit=3):
            assert task.index == count
            assert task.limit == 3
            count += 1
        assert count == 3

        with self.assertRaises(Repeat.TimeoutError):
            async for task in Repeat.each():
                async with task.timeout():
                    await asyncio.sleep(5.0)
