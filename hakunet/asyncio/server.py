from asyncio import new_event_loop, start_server, Event
from hakunet.utils import *


class Server:
    class Context:
        def __init__(self, server, reader, writer):
            self.server = server
            self.reader = reader
            self.writer = writer

        def close(self):
            self.writer.close()

        def is_closing(self):
            return self.writer.is_closing()

        async def send(self, data):
            await write_with_len_async(self.writer, data)

        async def emit(self, event, *args, broadcast=False, **kwargs):
            if broadcast:
                await self.server.emit(event, *args, **kwargs)
            else:
                await self.send([event, args, kwargs])

    class Transaction:
        def __init__(self, server, tid, writer):
            self.server = server
            self.tid = tid
            self.writer = writer

        async def send(self, data):
            await write_with_len_async(self.writer, ['tsc', [self.tid, data]])

        async def read(self):
            await self.server._t_event[self.tid].wait()
            data = self.server._t_mes_queue[self.tid].pop(0)
            if self.server._t_mes_queue[self.tid] == []:
                self.server._t_event[self.tid].clear()
            return data

    def __init__(self, host, port,):
        self.host = host
        self.port = port
        self.server = start_server(
            self.handle_client, host, port
        )
        self.contexts = []
        self.handlers = {}
        self.transactions = {}
        self._t_event: dict[str, Event] = {}
        self._t_mes_queue: dict[str, ] = {}

    def run(self):
        loop = new_event_loop()
        try:
            loop.run_until_complete(self.coro())
        except KeyboardInterrupt:
            pass

    async def coro(self):
        self.server = await self.server
        async with self.server as server:
            await server.serve_forever()

    def on(self, event):
        def decorator(func):
            self.handlers[event] = func
            return func
        return decorator

    def on_event(self, event, func):
        self.handlers[event] = func

    def on_transaction(self, tsc):
        def decorator(func):
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

    async def handle_client(self, reader, writer):
        new_ctx = Server.Context(self, reader, writer)
        self.contexts.append(new_ctx)
        while not new_ctx.is_closing():
            data = await read_msg(reader)
            if data is None:
                break

            match data:
                case ['tsc', [tid, data]]:
                    if tid in self._t_event and tid in self._t_mes_queue:
                        self._t_mes_queue[tid].append(data)
                        self._t_event[tid].set()
                case ['tsc_st', tid, ttype]:
                    self._t_event[tid] = Event()
                    self._t_mes_queue[tid] = []
                    new_tsc = Server.Transaction(
                        self, tid, writer
                    )
                    asyncio.ensure_future(self.transactions[ttype](new_tsc))
                case [event, args, kwargs]:
                    if event in self.handlers:
                        await self.handlers[event](new_ctx, *args, **kwargs)
        new_ctx.close()

        new_ctx_list = []
        for c in self.contexts:
            if not c.is_closing():
                new_ctx_list.append(c)
        self.contexts = new_ctx_list
