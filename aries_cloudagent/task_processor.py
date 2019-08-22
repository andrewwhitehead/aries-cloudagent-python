"""Classes for managing a limited set of concurrent tasks."""

import asyncio
import logging
import time
from typing import Awaitable, Callable

import async_timeout

LOGGER = logging.getLogger(__name__)


async def delay_task(delay: float, task: Awaitable):
    """Wait a given amount of time before executing an awaitable."""
    await asyncio.sleep(delay)
    return await task


class Attempt:
    def __init__(
        self,
        index: int = 0,
        limit: int = None,
        interval: float = 0.0,
        backoff: float = 0.0,
        start: float = None,
        ident: str = None,
    ):
        """Initialize an attempt instance."""
        self.ident = ident
        self.index = index
        self.limit = limit
        self.interval = interval
        self.backoff = backoff
        self.start = start or time.perf_counter()
        self.elapsed = time.perf_counter() - start if start else 0

    @property
    def backoff_interval(self) -> float:
        """Calculate the time before the next attempt."""
        if self.index:
            return pow(self.interval, 1 + (self.backoff * (self.index - 1)))
        return 0.0

    @property
    def final(self) -> bool:
        """Check if this is the last instance in the sequence."""
        return bool(self.limit and self.index >= self.limit - 1)

    def next(self):
        """Update the attempt index and elapsed time."""
        self.index += 1
        self.elapsed = time.perf_counter() - self.start

    def timeout(self, interval: float = 0):
        """Create a context manager for timing out an attempt."""
        return async_timeout.timeout(self.interval if interval == 0 else interval)


class Repeat:
    """Class for repeating a task within an async context."""

    # standard alias for callers to use
    TimeoutError = asyncio.TimeoutError

    @classmethod
    async def each(cls, limit: int = 0, interval: float = 0.0, backoff: float = 0.0):
        """Async generator for multiple repeat instances."""
        start = time.perf_counter()
        attempt = Attempt(
            index=0, limit=limit, interval=interval, backoff=backoff, start=start
        )
        while True:
            yield attempt
            if attempt.final:
                break
            attempt.next()
            wait = attempt.backoff_interval
            if wait:
                await asyncio.sleep(wait)


class PendingTask(Attempt):
    """Class for tracking pending tasks."""

    def __init__(self, ident, fn: Callable[["PendingTask"], Awaitable], **kwargs):
        """Initialize the pending task instance."""
        super(PendingTask, self).__init__(ident=ident, **kwargs)
        self.fn = fn
        self.future = asyncio.get_event_loop().create_future()
        self.running: asyncio.Future = None

    def done(self):
        """Check if the task is done."""
        return self.future.done()

    def exception(self):
        """Get the exception raised by the task, if any."""
        return self.future.exception()

    def result(self):
        """Get the result of the task."""
        return self.future.result()

    def cancel(self):
        """Cancel the running task."""
        if not self.future.done():
            self.future.cancel()
        if self.running and not self.running.done():
            self.running.cancel()

    def __await__(self):
        """Await the pending task."""
        return self.future.__await__()


class TaskProcessor:
    """Class for managing a limited set of concurrent tasks."""

    def __init__(self, *, max_pending: int = 10):
        """Instantiate the dispatcher."""
        self.done_event = asyncio.Event()
        self.done_event.set()
        self.max_pending = max_pending
        self.pending = set()
        self.pending_lock = asyncio.Lock()
        self.ready_event = asyncio.Event()
        self.ready_event.set()

    def ready(self):
        """Check if the processor is ready."""
        return self.ready_event.is_set()

    async def wait_ready(self):
        """Wait for the processor to be ready for more tasks."""
        await self.ready_event.wait()

    def done(self):
        """Check if the processor has any pending tasks."""
        return self.done_event.is_set()

    async def wait_done(self):
        """Wait for all pending tasks to complete."""
        await self.done_event.wait()

    def _enqueue_task(self, task: PendingTask):
        """Enqueue the given pending task."""
        if not task.done():
            awaitable = task.fn(task)
            if awaitable:
                wait = task.backoff_interval
                if wait:
                    awaitable = delay_task(wait, awaitable)
                task.running = asyncio.ensure_future(awaitable)
                task.running.add_done_callback(
                    lambda fut: asyncio.ensure_future(self._check_task(task))
                )
            else:
                task.future.set_result(None)
                asyncio.ensure_future(self._check_task(task))

    async def _check_task(self, task: PendingTask):
        """Complete a task."""
        if task.running and task.running.done():
            future = task.running
            task.running = None
            exception = future.exception()
            if exception:
                LOGGER.debug(
                    "Task raised exception: (%s) %s", task.ident or task, exception
                )
                if not task.final:
                    task.next()
                    asyncio.get_event_loop().call_soon(self._enqueue_task, task)
                else:
                    LOGGER.warning("Task failed: %s", task.ident or task)
                    task.future.set_exception(exception)
            else:
                task.future.set_result(future.result())
        if task.done():
            async with self.pending_lock:
                if task in self.pending:
                    self.pending.remove(task)
                else:
                    LOGGER.warning(
                        "Task not found in pending list: %s", task.ident or task
                    )
                if len(self.pending) < self.max_pending:
                    self.ready_event.set()
                if not self.pending:
                    self.done_event.set()

    async def run_retry(
        self,
        fn: Callable[[PendingTask], Awaitable],
        *,
        ident=None,
        limit: int = 5,
        interval: float = 10.0,
        backoff: float = 0.0,
        when_ready: bool = True,
    ) -> PendingTask:
        """Process a task and track the result."""
        if when_ready:
            await self.wait_ready()
        task = PendingTask(ident, fn, limit=limit, interval=interval, backoff=backoff)
        async with self.pending_lock:
            self.pending.add(task)
            self.done_event.clear()
            if len(self.pending) >= self.max_pending:
                self.ready_event.clear()
        asyncio.get_event_loop().call_soon(self._enqueue_task, task)
        return task

    async def run_task(
        self, task: Awaitable, *, ident=None, when_ready: bool = True
    ) -> PendingTask:
        """Run a single coroutine with no retries."""
        return await self.run_retry(
            lambda pending: task, ident=ident, limit=1, when_ready=when_ready
        )
