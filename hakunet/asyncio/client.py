from typing import Any

import asyncio
from asyncio import open_connection, sleep
from asyncio import Event, StreamWriter, StreamReader
from time import time_ns

from hakunet.utils import *


#Export
__all__ = ['Client']


#Type Aliases
EventHandler = Callable[["Client.Context", Any], Awaitable[None]]
Transaction = Callable[["Client.Transaction", Any], Awaitable[Any]]



class Client:
    class Context:
        def __init__(self, writer: StreamWriter):
            self.writer = writer

        async def close(self):
            self.writer.close()
            await self.writer.wait_closed()

        def is_closing(self):
            return self.writer.is_closing()

        async def send(self, data):
            write_with_len(self.writer, data)
            await self.writer.drain()

        def emit(self, event: str, *args, **kwargs):
            self.send([event, args, kwargs])

    class Transaction:
        def __init__(
            self, 
            tid: int, 
            ttype: str, 
            client: "Client", 
            writer: StreamWriter
        ):
            self.tid = tid
            self.ttype = ttype
            self.client = client
            self.writer = writer
            self.m_index = 0

        def start(self):
            write_with_len(self.writer, ['tsc_st', self.tid, self.ttype])

        async def send(self, data):
            write_with_len(self.writer, ['tsc', [self.tid, data]])
            await self.writer.drain()

        async def read(self):
            await self.client._t_event[self.tid].wait()
            data = self.client._t_mes_queue[self.tid].pop(0)
            if self.client._t_mes_queue[self.tid] == []:
                self.client._t_event[self.tid].clear()
            return data

    def __init__(self, host: str, port: str|int):
        self.host = host
        self.port = port
        self.context: Client.Context
        self.handlers: dict[str, EventHandler] = {}
        self.transactions: dict[str, Transaction] = {}
        self._t_event: dict[int, Event] = {}
        self._t_mes_queue: dict[int, list[Any]] = {}

    async def __aenter__(self):
        reader, writer = await open_connection(self.host, self.port)
        self.handle_client(reader, writer)

    async def __aexit__(self, exc_type, exc, tb):
        await sleep(0.1)
        await self.context.close()

    async def connect(self):
        reader, writer = await open_connection(self.host, self.port)
        self.handle_client(reader, writer)

    async def close(self):
        await self.context.close()

    def is_closing(self):
        return self.context.is_closing

    def on(self, event):
        def decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            self.handlers[event] = wrapper
            return wrapper
        return decorator

    def on_event(self, event, func):
        self.handlers[event] = func

    def transaction(self, ttype: str):
        def decorator(func: Transaction) -> Transaction:
            async def wrapper(*args, **kwargs):
                tsc = Client.Transaction(
                    time_ns(), ttype, self, self.context.writer
                )
                self._t_event[tsc.tid] = Event()
                self._t_mes_queue[tsc.tid] = []
                tsc.start()
                res = await func(tsc, *args, **kwargs)
                del self._t_event[tsc.tid]
                del self._t_mes_queue[tsc.tid]
                return res

            setattr(self, ttype, wrapper)
            self.transactions[ttype] = wrapper
            return wrapper

        return decorator

    async def tsc(self, ttype: str, *args, **kwargs):
        return await self.transactions[ttype](*args, **kwargs)

    async def emit(self, event: str, *args, **kwargs):
        await self.context.send([event, args, kwargs])

    def handle_client(
        self, 
        reader: StreamReader, 
        writer: StreamWriter,
    ):
        self.context = Client.Context(writer)
        asyncio.ensure_future(self.event_loop(reader))

    async def event_loop(self, reader):
        while not self.context.is_closing():
            data = await read_msg(reader)
            if data is None:
                break

            match data:
                case ['tsc', [tid, data]]:
                    self._t_mes_queue[tid].append(data)
                    self._t_event[tid].set()
                
                case [event, args, kwargs]:
                    if event in self.handlers:
                        await self.handlers[event](
                            self.context, *args, **kwargs
                        )

        await self.context.close()
