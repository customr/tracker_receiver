from websocket_server import WebsocketServer


def message_received(client, server, message):
	server.send_message_to_all(message)


PORT=5678
server = WebsocketServer(PORT)
server.set_fn_message_received(message_received)
server.run_forever()