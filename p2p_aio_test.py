from venv import create
from hakunet.asyncio.p2p import Node
from time import sleep
import asyncio


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
main_node = Node('127.0.0.1', 10000)
nodes: list[Node]
NODE_AMOUNT = 5


def create_node(host) -> Node:
  node = Node(host, 0)
  @node.on('message')
  async def message(ctx:Node.Context, message):
    print(
      '========================\n'
      f' {ctx.id}  ->  {node.id}\n'
      f' Mes: {message}\n'
      '========================'
    )
    await ctx.emit('reply', 'received')
  
  @node.on('reply')
  async def reply(ctx:Node.Context, message):
    print(
      '========================\n'
      f' {ctx.id}  ->  {node.id}\n'
      f' Reply: {message}\n'
      '========================'
    )
  
  return node


async def make_nodes():
  global nodes, main_node
  await main_node.start()
  
  nodes = [
    create_node('127.0.0.1')
    for _ in range(NODE_AMOUNT)
  ]
  
  #start node
  for node in nodes:
    await node.start()

  #connect to other node
  for i in range(NODE_AMOUNT):
    await nodes[i].connect('127.0.0.1', 10000)
  await asyncio.sleep(0.01 * NODE_AMOUNT)


async def main():
  global nodes
  try:
    await make_nodes()
    
    #print connections of all nodes
    for node in nodes:
      print(sorted(node.nodes))
    
    #send message test
    await nodes[0].emit('message', message='message from n0')
    await asyncio.sleep(0.002 * NODE_AMOUNT)
  finally:
    await main_node.stop()
    for node in nodes:
      await node.stop()


if __name__ == '__main__':
  asyncio.new_event_loop().run_until_complete(main())