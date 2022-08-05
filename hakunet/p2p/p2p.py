from dataclasses import dataclass
import time
import random

import socket
from threading import Thread, Event, RLock
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
    def function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            err = format_exc()
            log_error(err)
    return function


class Node(Thread):
    @dataclass()
    class NodeInfo:
        id: str
        host: str
        port: int

    class Context(Thread):
        '''context obj for node connection'''

        def __init__(self, parent_node, sock, id, host, port):
            super(Node.Context, self).__init__()

            self.id: str = id
            self.host: str = host
            self.port: int = port

            self.parent_node: "Node" = parent_node
            self.sock: socket.socket = sock
            self.terminate_flag = Event()

            self.info = {}
            self.parent_node.debug_print(
                'NodeContext.send: Started with client'
                f'({self.id}) on {self.host}:{self.port}'
            )

        def stop(self):
            self.terminate_flag.set()

        def send(self, data):
            send_with_len(self.sock, data)

        def emit(self, event: str, *args, broadcast=False, **kwargs):
            if broadcast:
                self.parent_node._send_to_nodes([event, args, kwargs])
            else:
                self.send([event, args, kwargs])

        def run(self):
            self.sock.settimeout(RECV_TIMEOUT)

            while not self.terminate_flag.is_set():
                data = ''
                try:
                    data = recv_msg(self.sock)
                except socket.timeout:
                    self.parent_node.debug_print('NodeContext: timeout')
                except Exception as e:
                    self.terminate_flag.set()
                    self.parent_node.debug_print('Unexpected error')
                    self.parent_node.debug_print(e)

                if data:
                    self.parent_node.message_count_recv += 1
                    self.parent_node.node_message(self, data)

                time.sleep(MAINLOOP_DELAY)

            self.sock.settimeout(None)
            self.sock.close()
            self.parent_node.debug_print('NodeContext: Stopped')

        def set_info(self, key, value):
            self.info[key] = value

        def get_info(self, key):
            return self.info[key]

        def __str__(self):
            main = self.parent_node
            return (
                f'<NodeContext: {main.host}:{main.port} <->'
                f' {self.host}:{self.port} ({self.id})>'
            )
        __repr__ = __str__

    def __init__(self, host, port, debug=False):
        super(Node, self).__init__()
        self.terminate_flag = Event()

        self.host = host
        self.port = port

        self.callbacks: dict[str, Callable[[Node.Context, Any], None]] = {}

        self.nodes: dict[str, Node.Context] = ThDict()
        self.node_list_lock = RLock()
        self.connect_lock = RLock()
        self.clients = []

        t = (
            f'{self.host}:{self.port}'
            f'{getpid()}'
            f'{random.randint(1, 16**8)}'
        )
        self.id = shake_256(encode(t)).hexdigest(4)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.init_server()

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

    def init_server(self):
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        self.sock.settimeout(RECV_TIMEOUT)
        self.sock.listen(10)
        if not self.port:
            self.port = self.sock.getsockname()[1]
        print(
            f'Initialise of the Node on port-{self.port} complete on node<{self.id}>')

    def create_new_context(self, context, id, host, port):
        return Node.Context(self, context, id, host, port)

    def print_contexts(self):
        print('Node Context overview:')
        print(f'- Total nodes connected: {len(self.nodes)}')

    def delete_closed_contexts(self):
        deleted = []
        for n in self.nodes.values():
            if n.terminate_flag.is_set():
                self.on_inbound_disconnect(n)
                n.join()
                deleted.append(n.id)
        for i in deleted:
            del self.nodes[i]

    def _send_to_nodes(self, data, exclude=[]):
        self.message_count_send = self.message_count_send + 1
        for n in list(self.nodes.values()):
            self._send_to_node(n, data)

    def _send_to_node(self, n: Context, data):
        self.message_count_send = self.message_count_send + 1
        self.delete_closed_contexts()

        if n.id in self.nodes:
            try:
                n.send(data)
            except Exception as e:
                self.debug_print(
                    f'Node send_to_node: Error while sending data to the node ({e})'
                )

        else:
            self.debug_print(
                'Node send_to_node: Could not send the data, node is not found!'
            )

    def send(self, data):
        self._send_to_nodes(data)

    def send_to(self, target, data):
        self._send_to_node(target, data)

    def emit(self, event, *args, **kwargs):
        self._send_to_nodes([event, args, kwargs])

    def emit_to(self, target, event, *args, **kwargs):
        self._send_to_node(target, [event, args, kwargs])

    def run(self):
        # Check whether the thread needs to be closed
        while not self.terminate_flag.is_set():
            try:
                self.debug_print('Node: Wait for incoming Context')
                context, client_address = self.sock.accept()

                # Basic information exchange (not secure) of the id's of the nodes!
                node_data = recv_msg(context)
                node_host, node_port, node_id = node_data
                send_with_len(context, ['', 0, self.id])

                new_node = self.create_new_context(
                    context, node_id, client_address[0], client_address[1]
                )
                new_node.start()

                with self.node_list_lock:
                    if len(node_id) == 10:
                        self.clients.append(new_node)
                    else:
                        # use inbound conn to replace outbound conn
                        if new_node.id in self.nodes:
                            self.nodes[new_node.id].stop()
                        self.nodes[new_node.id] = new_node
                self.on_inbound_connect(new_node)

                if node_data:
                    node_host, node_port, node_id = node_data
                    self._send_to_nodes(Node.NodeInfo(
                        node_id, node_host, node_port))

            except socket.timeout:
                self.debug_print('Node: Context timeout!')
            except Exception as e:
                print(e)

            time.sleep(MAINLOOP_DELAY)

        print('Node stopping...')
        for t in self.all_conns:
            t.stop()

        for t in self.all_conns:
            t.join()

        self.sock.settimeout(None)
        self.sock.close()
        print('Node stopped')

    def stop(self):
        self.terminate_flag.set()

    def connect(self, host, port):
        if host == self.host and port == self.port:
            print('connect_with_node: Cannot connect with yourself!!')
            return False

        # Check if node is already connected with this node!
        for node in self.nodes.values():
            if node.host == host and node.port == port:
                return True

        try:
            with self.connect_lock:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.debug_print('connecting to %s port %s' % (host, port))
                sock.connect((host, port))

                send_with_len(sock, [self.host, self.port, self.id])
                node_data = recv_msg(sock)

                new_node = self.create_new_context(
                    sock, node_data[2], host, port)
                new_node.start()

                with self.node_list_lock:
                    if node_data[2] in self.nodes:
                        self.nodes[node_data[2]].stop()

                    self.nodes[new_node.id] = new_node
                self.on_outbound_connect(new_node)
            return True
        except Exception as e:
            self.debug_print(
                'TcpServer.connect_with_node: Could not connect with node. (' + str(e) + ')')

    def callback(self, event, node, args, kwargs):
        if event in self.callbacks:
            self.callbacks[event](node, *args, **kwargs)

    def on_event(self, event: str, callback: Callable[["Node.Context", Any], None]):
        self.callbacks[event] = callback

    def on(self, event: str):
        def decorator(callback):
            self.callbacks[event] = callback
            return callback
        return decorator

    def disconnect_with_node(self, node):
        if node in self.nodes:
            self.on_disconnect_outbound(node)
            node.stop()
            node.join()  # When this is here, the application is waiting and waiting
            del self.nodes[node.id]
        else:
            print(
                'Node disconnect_with_node: cannot disconnect with a node with which we are not connected.')

    def on_outbound_connect(self, node):
        self.debug_print('on_outbound_connect: ' + node.id)
        if self.callback is not None:
            self.callback('on_outbound_connect', node, [], {})

    def on_inbound_connect(self, node):
        self.debug_print('on_inbound_connect: ' + node.id)
        if self.callback is not None:
            self.callback('on_inbound_connect', node, [], {})

    def on_inbound_disconnect(self, node):
        self.debug_print('on_inbound_disconnect: ' + node.id)
        if self.callback is not None:
            self.callback('on_inbound_disconnect', node, [], {})

    def on_outbound_disconnect(self, node):
        self.debug_print('on_outbound_disconnect: ' + node.id)
        if self.callback is not None:
            self.callback('on_outbound_disconnect', node, [], {})

    def on_disconnect_outbound(self, node):
        self.debug_print(
            'node wants to disconnect with oher outbound node: ' + node.id)
        if self.callback is not None:
            self.callback('on_disconnect_outbound', node, [], {})

    def node_message(self, node, data):
        self.debug_print('node_message: ' + node.id + ': ' + str(data))
        if isinstance(data, Node.NodeInfo):
            if data.id != self.id and data.id not in self.nodes:
                self.connect(data.host, data.port)
        else:
            event, args, kwargs = data
            self.callback(event, node, args, kwargs)