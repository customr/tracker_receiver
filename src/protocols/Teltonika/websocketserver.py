import asyncio
import websockets


class Server:
	clients = set()

	async def register(self, ws):
		self.clients.add(ws)


	async def unregister(self, ws):
		self.clients.remove(ws)


	async def send_to_clients(self, message):
		if self.clients:
			await asyncio.wait([client.send(message) for client in self.clients])


	async def ws_handler(self, ws, uri):
		await self.register(ws)
		try:
			self.distribute(ws)
		finally:
			self.unregister(ws)


	async def distribute(self, ws):
		async for message in ws:
			await self.send_to_clients(message)