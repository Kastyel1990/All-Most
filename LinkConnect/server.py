import asyncio
import websockets
import aiohttp
from aiohttp import web
from urllib.parse import urlparse, parse_qs

agent_connections = {}  # token -> websocket

# noVNC frontend
NOVNC_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Remote Desktop</title>
  <script src="/static/novnc/app/ui.js"></script>
  <script src="/static/novnc/app/rfb.js"></script>
  <style>html, body, #noVNC_canvas { width:100%; height:100%; margin:0; background:#000; }</style>
</head>
<body>
  <canvas id="noVNC_canvas"></canvas>
  <script>
    const rfb = new RFB(document.getElementById('noVNC_canvas'), "ws://" + location.host + "/client/{{token}}");
    rfb.viewOnly = false;
  </script>
</body>
</html>
"""

# Serve noVNC HTML session
async def handle_session(request):
    token = request.match_info.get('token')
    if token not in agent_connections:
        return web.Response(status=404, text="Session not found")
    html = NOVNC_HTML.replace("{{token}}", token)
    return web.Response(text=html, content_type='text/html')

# Static files (noVNC)
async def serve_static(request):
    path = request.match_info['path']
    return web.FileResponse(f'static/novnc/{path}')

# WebSocket: client (browser)
async def client_ws_handler(request):
    token = request.match_info.get('token')
    if token not in agent_connections:
        return web.Response(status=404)

    ws_browser = web.WebSocketResponse()
    await ws_browser.prepare(request)

    ws_agent = agent_connections[token]

    async def browser_to_agent():
        async for msg in ws_browser:
            if msg.type == web.WSMsgType.BINARY:
                await ws_agent.send(msg.data)

    async def agent_to_browser():
        try:
            async for data in ws_agent:
                await ws_browser.send_bytes(data)
        except websockets.exceptions.ConnectionClosed:
            pass

    await asyncio.gather(browser_to_agent(), agent_to_browser())
    return ws_browser

# WebSocket: agent connects here
async def agent_ws_handler(websocket):
    # path будет вида "/agent?token=..."
    parsed = urlparse(websocket.path)
    qs = parse_qs(parsed.query)
    token = qs.get("token", [None])[0]
    if not token:
        await websocket.close()
        return

    print(f"[+] Агент подключился: {token}")
    agent_connections[token] = websocket

    try:
        # ждём, пока агент отключится
        await websocket.wait_closed()
    finally:
        if agent_connections.get(token) is websocket:
            del agent_connections[token]
            print(f"[-] Агент отключился: {token}")

# HTTP app (aiohttp) для статики и сессий
app = web.Application()
app.router.add_get('/session/{token}', handle_session)
app.router.add_get('/static/novnc/{path:.*}', serve_static)
app.router.add_get('/client/{token}', client_ws_handler)

# Запуск aiohttp и websockets параллельно
async def start_servers():
    # HTTP server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    print("[*] HTTP сервер запущен на порту 8000")

    # WebSocket сервер для агентов
    ws_server = await websockets.serve(agent_ws_handler, "0.0.0.0", 8001)
    print("[*] WebSocket сервер для агентов запущен на порту 8001")

    await asyncio.Future()  # run forever

if __name__ == '__main__':
    asyncio.run(start_servers())
