from hakunet.asyncio.server import Server


server = Server('127.0.0.1', 10000)


@server.on('mes')
async def mes(ctx: Server.Context, mes):
  print(mes)
  await ctx.emit('reply', 'reply-test')


@server.on_transaction('fib')
async def fib(ctx: Server.Transaction):
  n = await ctx.read()
  a = 0
  b = 1
  for _ in range(n):
    a, b = b, a+b
    await ctx.send(a)


server.run()
