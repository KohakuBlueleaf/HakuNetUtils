from dataclasses import dataclass
import random

from asyncio import start_server, open_connection, Event
from os import getpid
from hashlib import shake_256
from traceback import format_exc
from typing import Callable

from hakunet.utils import *


MAINLOOP_DELAY = 0.001
RECV_TIMEOUT = 1
DEFAULT_EVENTS = {
  'on_outbound_connect',
  'on_inbound_connect',
  'on_inbound_disconnect',
  'on_outbound_disconnect',
  'on_disconnect_outbound',
}


def error_handler(func):
  def function(*args,**kwargs):
    try:
      return func(*args, **kwargs)
    except:
      err = format_exc()
      log_error(err)
  return function


class Node:
  @dataclass()
  class NodeInfo:
    id:str
    host:str
    port:int
  
  class Context:
    '''context obj for node connection'''
    def __init__(self, parent_node, reader, writer, id, host, port):
      self.id: str = id
      self.host: str = host
      self.port: int = port
      
      self.start_flag = Event()
      self.reader = reader
      self.writer = writer
      self.parent_node:"Node" = parent_node

      self.parent_node.debug_print(
        'NodeContext.send: Started with client'
        f'({self.id}) on {self.host}:{self.port}'
      )

    @property
    def started(self):
      return self.start_flag.wait()
    
    async def stop(self):
      self.writer.close()
      await self.writer.wait_closed()
      
    async def send(self, data):
      await write_with_len_async(self.writer, data)
    
    async def emit(self, event: str, *args, broadcast=False, **kwargs):
      if broadcast:
        await self.parent_node._send_to_nodes([event, args, kwargs])
      else:
        await self.send([event, args, kwargs])
    
    async def event_loop(self):
      self.start_flag.set()
      while True:
        data = ''
        try:
          data = await read_msg(self.reader)
        except Exception as e:
          self.parent_node.debug_print('Unexpected error')
          self.parent_node.debug_print(e)
        
        if data:
          self.parent_node.message_count_recv += 1
          await self.parent_node.node_message(self, data)
        else:
          break
      self.writer.close()
      self.parent_node.debug_print('NodeContext: Stopped')

    def __str__(self):
      main = self.parent_node
      return (
        f'<NodeContext: {main.host}:{main.port} <->'
        f' {self.host}:{self.port} ({self.id})>'
      )
    __repr__ = __str__


  def __init__(self, host, port, debug=False):
    self.host = host
    self.port = port

    self.callbacks: dict[str, Callable] = {}
    self.nodes: dict[str, Node.Context] = {}
    self.clients = []

    t = (
      f'{self.host}:{self.port}'
      f'{getpid()}'
      f'{random.randint(1, 16**8)}'
    )
    self.id = shake_256(encode(t)).hexdigest(4)

    self.server = asyncio.start_server(self.handle_client, host, port)
    self.stop_flag = Event()
    self.stop_flag.clear()

    self.message_count_send = 0
    self.message_count_recv = 0
    self.message_count_rerr = 0
    self.debug = debug

  def __str__(self):
    return 'Node: {}:{}'.format(self.host, self.port)

  def __repr__(self):
    return '<Node {}:{} id: {}>'.format(self.host, self.port, self.id)
  
  @property
  def all_nodes(self):
    return list(self.nodes.values())
  
  @property
  def all_conns(self):
    yield from self.all_nodes
    yield from self.clients

  def debug_print(self, message):
    if self.debug:
      print(f'Debug: {message}')

  def print_contexts(self):
    print('Node Context overview:')
    print(f'- Total nodes connected: {len(self.nodes)}')

  async def start(self):
    async def main():
      self.server = await self.server
      addrs = [sock.getsockname() for sock in self.server.sockets][0]
      self.host = addrs[0]
      self.port = addrs[1]
      print(f'Initialise of the Node on port-{self.port} complete on node<{self.id}>')
      async with self.server as server:
        await server.serve_forever()
    asyncio.ensure_future(main())
    await asyncio.sleep(0.001)

  async def stop(self):
    print(f'Node {self.id} stopping.')
    self.server.close()
    await self.server.wait_closed()
    for n in self.nodes.values():
      await n.stop()
    print(f'Node {self.id} stopped.')
    
  async def handle_client(self, reader, writer):
    chost, cport = writer.get_extra_info('socket').getpeername()
    node_data = await read_msg(reader)
    if node_data is not None:
      node_host, node_port, node_id = node_data
      await write_with_len_async(writer, ['', 0, self.id])
      
      new_node = Node.Context(
        self, reader, writer,
        node_id, chost, cport
      )
      
      if len(node_id)==10:
        self.clients.append(new_node)
      elif new_node.id in self.nodes:
        await new_node.stop()
        return
      
      asyncio.ensure_future(new_node.event_loop())
      await new_node.started
      self.nodes[new_node.id] = new_node
      await asyncio.sleep(0.001)
      
      await self.on_inbound_connect(new_node)
      await self._send_to_nodes(
        Node.NodeInfo(node_id, node_host, node_port)
      )

  async def connect(self, host, port):
    if host == self.host and port == self.port:
      print('connect_with_node: Cannot connect with yourself!!')
      return False

    # Check if node is already connected with this node!
    for node in self.nodes.values():
      if node.host == host and node.port == port:
        return True

    try:
      reader, writer = await open_connection(host, port)
      self.debug_print('connecting to %s port %s' % (host, port))

      await write_with_len_async(writer, [self.host, self.port, self.id])
      node_data = await read_msg(reader)

      new_node = Node.Context(
        self, reader, writer,
        node_data[2], host, port
      )
      
      if new_node.id in self.nodes:
        await new_node.stop()
        return True
      
      asyncio.ensure_future(new_node.event_loop())
      await new_node.started
      self.nodes[new_node.id] = new_node
      await asyncio.sleep(0.001)
      
      await self.on_outbound_connect(new_node)
    except Exception as e:
      self.debug_print('TcpServer.connect_with_node: Could not connect with node. (' + str(e) + ')')

  async def delete_closed_contexts(self):
    deleted = []
    for n in self.nodes.values():
      if n.writer.is_closing():
        await self.on_inbound_disconnect(n)
        deleted.append(n.id)
    for i in deleted:
      del self.nodes[i]

  async def _send_to_nodes(self, data):
    self.message_count_send = self.message_count_send + 1
    for n in list(self.nodes.values()):
      await self._send_to_node(n, data)

  async def _send_to_node(self, n:Context, data):
    self.message_count_send = self.message_count_send + 1
    await self.delete_closed_contexts()
    
    if n.id in self.nodes:
      try:
        await n.send(data)
      except Exception as e:
        self.debug_print(
          f'Node send_to_node: Error while sending data to the node ({e})'
        )
        
    else:
      self.debug_print(
        'Node send_to_node: Could not send the data, node is not found!'
      )
  
  async def send(self, data):
    await self._send_to_nodes(data)
  
  async def send_to(self, target, data):
    await self._send_to_node(target, data)
  
  async def emit(self, event, *args, **kwargs):
    await self._send_to_nodes([event, args, kwargs])
  
  async def emit_to(self, target, event, *args, **kwargs):
    await self._send_to_node(target, [event, args, kwargs])

  def on_event(self, event: str, callback: Callable):
    self.callbacks[event] = callback
  
  def on(self, event: str):
    def decorator(callback):
      self.callbacks[event] = callback
      return callback
    return decorator
  
  async def callback(self, event, node, args, kwargs):
    if event in self.callbacks:
      await self.callbacks[event](node, *args, **kwargs)
  
  async def disconnect_with_node(self, node):
    if node in self.nodes:
      await self.on_disconnect_outbound(node)
      node.stop()
      del self.nodes[node.id]
    else:
      print('Node disconnect_with_node: cannot disconnect with a node with which we are not connected.')

  async def on_outbound_connect(self, node):
    self.debug_print('on_outbound_connect: ' + node.id)
    if self.callback is not None:
      await self.callback('on_outbound_connect', node, [], {})

  async def on_inbound_connect(self, node):
    self.debug_print('on_inbound_connect: ' + node.id)
    if self.callback is not None:
      await self.callback('on_inbound_connect', node, [], {})

  async def on_inbound_disconnect(self, node):
    self.debug_print('on_inbound_disconnect: ' + node.id)
    if self.callback is not None:
      await self.callback('on_inbound_disconnect', node, [], {})

  async def on_outbound_disconnect(self, node):
    self.debug_print('on_outbound_disconnect: ' + node.id)
    if self.callback is not None:
      await self.callback('on_outbound_disconnect', node, [], {})

  async def on_disconnect_outbound(self, node):
    self.debug_print('node wants to disconnect with oher outbound node: ' + node.id)
    if self.callback is not None:
      await self.callback('on_disconnect_outbound', node, [], {})

  async def node_message(self, node, data):
    self.debug_print('node_message: ' + node.id + ': ' + str(data))
    
    if isinstance(data, Node.NodeInfo):
      if data.id!=self.id and data.id not in self.nodes:
        await self.connect(data.host, data.port)
    else:
      event, args, kwargs = data
      await self.callback(event, node, args, kwargs)


class Client():
  def __init__(self, debug=False):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    t = f'Client_Node {getpid()}{random.randint(1, 16**10)}'
    self.id = shake_256(t).hexdigest(10)

    self.message_count_send = 0
    self.message_count_recv = 0
    self.message_count_rerr = 0

    self.debug = debug
    print(f'Initialisation of the Node as Client')

  def debug_print(self, message):
    if self.debug:
      print(f'Debug: {message}')

  def send(self, data):
    self.message_count_send = self.message_count_send + 1
    send_with_len(self.sock, data)

  def recv(self):
    return recv_msg(self.sock)

  def connect(self, host, port):
    try:
      self.debug_print(f'connecting to {host} port {port}')
      self.sock.connect((host, port))

      send_with_len(self.sock, self.id)
      node_data = recv_msg(self.sock)
      self.server_id = node_data[2]

    except Exception as e:
      self.debug_print(
        'TcpServer.connect_with_node:'
        f' Could not connect with node. ({e})'
      )

  def __str__(self):
    return 'Node: {}:{}'.format(self.host, self.port)

  def __repr__(self):
    return '<Node {}:{} id: {}>'.format(self.host, self.port, self.id)