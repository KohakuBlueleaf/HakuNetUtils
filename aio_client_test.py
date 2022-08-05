from asyncio import new_event_loop, ensure_future, sleep
from hakunet.asyncio import Client


client = Client('127.0.0.1', 10000)


@client.on('reply')
async def reply(ctx: Client.Transaction, mes):
    print(mes)


@client.transaction('fib')
async def fib(ctx: Client.Transaction, n):
    await ctx.send(n)

    for _ in range(n):
        print(ctx.tid, await ctx.read())
        await sleep(0.001)


async def main():
    async with client:
        await client.emit('mes', 'mes-test')
        ensure_future(client.tsc('fib', 10))
        await sleep(0.04)
        ensure_future(client.tsc('fib', 10))
        await sleep(0.12)


loop = new_event_loop()
loop.run_until_complete(main())
