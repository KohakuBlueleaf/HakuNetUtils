from hakunet.p2p import Node
from time import sleep


main_node = Node('127.0.0.1', 10000)
nodes: list[Node]
NODE_AMOUNT = 3


def create_node(host) -> Node:
    node = Node(host, 0)

    @node.on('message')
    def message(ctx: Node.Context, message):
        print(
            '========================\n'
            f' {ctx.id}  ->  {node.id}\n'
            f' Mes: {message}\n'
            '========================'
        )
        ctx.emit('reply', 'received')

    @node.on('reply')
    def reply(ctx: Node.Context, message):
        print(
            '========================\n'
            f' {ctx.id}  ->  {node.id}\n'
            f' Reply: {message}\n'
            '========================'
        )

    return node


def make_nodes():
    global nodes, main_node

    main_node.start()
    nodes = []
    for i in range(NODE_AMOUNT):
        nodes.append(create_node('127.0.0.1'))

    # start node
    for node in nodes:
        node.start()
    sleep(0.1)

    # connect to other node
    for i in range(NODE_AMOUNT):
        nodes[i].connect('127.0.0.1', 10000)
    sleep(0.1)

    # print connections of all nodes
    for node in nodes:
        print(sorted(node.nodes))


def main():
    global nodes
    try:
        make_nodes()

        # send message test
        nodes[0].emit('message', message='message from n0')
        sleep(0.2)
    finally:
        main_node.stop()
        for node in nodes:
            node.stop()


if __name__ == '__main__':
    main()
