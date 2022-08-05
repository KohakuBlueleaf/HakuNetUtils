import asyncio
from asyncio import new_event_loop, start_server, Event
from asyncio import Server as AsyncioServer, StreamReader, StreamWriter
from hakunet.utils import *


#Export
__all__ = ["Server"]


#Type Aliases
EventHandler = Callable[["Server.Context", Any], Awaitable[None]]
Transaction = Callable[["Server.Context"], Awaitable[None]]


class Server:
    class Context:
        def __init__(
            self, 
            server: "Server", 
            reader: StreamReader, 
            writer: StreamWriter,
        ):
            self.server = server
            self.reader = reader
            self.writer = writer

        def close(self):
            self.writer.close()

        def is_closing(self):
            return self.writer.is_closing()

        async def send(self, data):
            await write_with_len_async(self.writer, data)

        async def emit(
            self, 
            event: str, *args, 
            broadcast=False, **kwargs,
        ):
            if broadcast:
                await self.server.emit(event, *args, **kwargs)
            else:
                await self.send([event, args, kwargs])

    class Transaction:
        def __init__(
            self, 
            server: "Server", 
            tid: int, 
            writer: StreamWriter
        ):
            self.server = server
            self.tid = tid
            self.writer = writer

        async def send(self, data):
            await write_with_len_async(
                self.writer, 
                ['tsc', [self.tid, data]],
            )

        async def read(self):
            await self.server._t_event[self.tid].wait()
            data = self.server._t_mes_queue[self.tid].pop(0)
            if self.server._t_mes_queue[self.tid] == []:
                self.server._t_event[self.tid].clear()
            return data

    def __init__(self, host: str, port: int|str):
        self.host = host
        self.port = port
        self.server: AsyncioServer
        self.server_starter = start_server(
            self.handle_client, host, port
        )
        self.contexts: list[Server.Context] = []
        self.handlers: dict[str, EventHandler] = {}
        self.transactions: dict[str, Transaction] = {}
        self._t_event: dict[int, Event] = {}
        self._t_mes_queue: dict[int, list[Any]] = {}

    def run(self):
        loop = new_event_loop()
        try:
            loop.run_until_complete(self.coro())
        except KeyboardInterrupt:
            pass

    async def coro(self):
        self.server = await self.server_starter
        async with self.server as server:
            await server.serve_forever()

    def on(self, event: str):
        def decorator(func: EventHandler) -> EventHandler:
            self.handlers[event] = func
            return func
        return decorator

    def on_event(self, event: str, func: EventHandler):
        self.handlers[event] = func

    def on_transaction(self, tsc: str):
        def decorator(func: Transaction) -> Transaction:
            async def wrapper(ctx):
                await func(ctx)
                del self._t_event[ctx.tid]
                del self._t_mes_queue[ctx.tid]
            self.transactions[tsc] = wrapper
            return wrapper
        return decorator

    async def emit(self, event, *args, **kwargs):
        for c in self.contexts:
            await c.emit(event, *args, **kwargs)

    async def handle_client(
        self, 
        reader: StreamReader, 
        writer: StreamWriter,
    ):
        new_ctx = Server.Context(self, reader, writer)
        self.contexts.append(new_ctx)
        
        while not new_ctx.is_closing():
            data = await read_msg(reader)
            if data is None:
                break

            match data:
                case ['tsc', [int(tid), data]]:
                    if (tid in self._t_event 
                        and tid in self._t_mes_queue):
                        self._t_mes_queue[tid].append(data)
                        self._t_event[tid].set()
                        
                case ['tsc_st', int(tid), str(ttype)]:
                    self._t_event[tid] = Event()
                    self._t_mes_queue[tid] = []
                    new_tsc = Server.Transaction(
                        self, tid, writer
                    )
                    asyncio.ensure_future(
                        self.transactions[ttype](new_tsc)
                    )
                    
                case [str(event), args, kwargs]:
                    if event in self.handlers:
                        handler = self.handlers[event]
                        await handler(new_ctx, *args, **kwargs)
                        
        new_ctx.close()
        new_ctx_list = []
        
        for c in self.contexts:
            if not c.is_closing():
                new_ctx_list.append(c)
        self.contexts = new_ctx_list
