from asyncio import open_connection, sleep, Event
from textwrap import wrap
from time import time_ns

from hakunet.utils import *


class Client:
  class Context:
    def __init__(self, reader, writer):
      self.reader = reader
      self.writer = writer
    
    async def close(self):
      self.writer.close()
      await self.writer.wait_closed()
    
    def is_closing(self):
      return self.writer.is_closing()
    
    async def send(self, data):
      write_with_len(self.writer, data)
      await self.writer.drain()
    
    async def read(self):
      return await read_msg(self.reader)
    
    def emit(self, event, *args, **kwargs):
      self.writer.send([event, args, kwargs])

  
  class Transaction:
    def __init__(self, tid, ttype, client, writer):
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
      if self.client._t_mes_queue[self.tid]==[]:
        self.client._t_event[self.tid].clear()
      return data

  
  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.context = None
    self._t_event: dict[str, Event] = {}
    self._t_mes_queue: dict[str,] = {}
    self.handlers = {}
    self.transactions = {}
  
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
    
  def on(self, event):
    def decorator(func):
      def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
      self.handlers[event] = wrapper
      return wrapper
    return decorator
  
  def on_event(self, event, func):
    self.handlers[event] = func
  
  def transaction(self, ttype):
    def decorator(func):
      async def wrapper(*args, **kwargs):
        tsc = Client.Transaction(time_ns(), ttype, self, self.context.writer)
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
  
  async def tsc(self, ttype, *args, **kwargs):
    return await self.transactions[ttype](*args, **kwargs)
  
  async def start_transaction(self, tsc):
    await self.transactions[tsc]
  
  async def emit(self, event, *args, **kwargs):
    await self.context.send([event, args, kwargs])
  
  def handle_client(self, reader, writer):
    self.context = Client.Context(reader, writer)
    asyncio.ensure_future(self.event_loop())
    
  async def event_loop(self):
    while not self.context.is_closing():
      data = await self.context.read()
      if data is None:
        break
      
      match data:
        case ['tsc', [tid, data]]:
          self._t_mes_queue[tid].append(data)
          self._t_event[tid].set()
        case [event, args, kwargs]:
          if event in self.handlers:
            await self.handlers[event](self.context, *args, **kwargs)
            
    await self.context.close()