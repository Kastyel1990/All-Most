import asyncio
from aiohttp import web
import ssl

agent_connections = {}  # token -> websocket

NOVNC_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Remote Desktop</title>
  <style>html, body, #noVNC_canvas { width:100%; height:100%; margin:0; background:#000; }</style>
</head>
<body>
  <canvas id="noVNC_canvas"></canvas>
  <script type="module">
    import RFB from "/static/novnc/core/rfb.js";
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const rfb = new RFB(
      document.getElementById('noVNC_canvas'),
      protocol + '//' + location.host + '/client/{{token}}'
    );
    rfb.viewOnly = false;
  </script>
</body>
</html>
"""

async def handle_session(request):
    token = request.match_info.get('token')
    if token not in agent_connections:
        return web.Response(status=404, text="Session not found")
    html = NOVNC_HTML.replace("{{token}}", token)
    return web.Response(text=html, content_type='text/html')

async def client_ws_handler(request):
    token = request.match_info.get('token')
    if token not in agent_connections:
        return web.Response(status=404)

    ws_browser = web.WebSocketResponse()
    await ws_browser.prepare(request)

    ws_agent = agent_connections[token]

    async def relay(src, dst):
        try:
            async for msg in src:
                if msg.type == web.WSMsgType.BINARY:
                    await dst.send_bytes(msg.data)
                elif msg.type == web.WSMsgType.TEXT:
                    await dst.send_str(msg.data)
                elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                    break
        finally:
            await dst.close()

    task1 = asyncio.create_task(relay(ws_browser, ws_agent))
    task2 = asyncio.create_task(relay(ws_agent, ws_browser))
    await asyncio.wait([task1, task2], return_when=asyncio.FIRST_COMPLETED)

    return ws_browser

async def agent_ws_handler(request):
    token = request.rel_url.query.get("token")
    if not token:
        return web.Response(status=400, text="Missing token")

    ws_agent = web.WebSocketResponse()
    await ws_agent.prepare(request)
    agent_connections[token] = ws_agent

    print(f"[+] Агент подключился: {token}")

    try:
        while not ws_agent.closed:
            await asyncio.sleep(1)
    finally:
        agent_connections.pop(token, None)
        print(f"[-] Агент отключился: {token}")

    return ws_agent

# Путь к распакованному архиву noVNC
NOVNC_DIR = "/root/Projects/RecoteConnectFromLink/static/"

app = web.Application()
app.router.add_get('/session/{token}', handle_session)
app.router.add_get('/client/{token}', client_ws_handler)
app.router.add_get('/agent', agent_ws_handler)
app.router.add_static('/static/', NOVNC_DIR)

if __name__ == '__main__':
    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain('server.crt', 'server.key')
    web.run_app(app, port=8443, ssl_context=ssl_context)
