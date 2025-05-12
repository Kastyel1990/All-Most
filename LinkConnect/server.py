import asyncio
from aiohttp import web

agent_connections = {}  # token -> websocket

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

    async def browser_to_agent():
        async for msg in ws_browser:
            if msg.type == web.WSMsgType.BINARY:
                await ws_agent.send_bytes(msg.data)

    async def agent_to_browser():
        async for data in ws_agent:
            await ws_browser.send_bytes(data)

    await asyncio.gather(browser_to_agent(), agent_to_browser())
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
        async for _ in ws_agent:
            pass
    finally:
        agent_connections.pop(token, None)
        print(f"[-] Агент отключился: {token}")

    return ws_agent

app = web.Application()
app.router.add_get('/session/{token}', handle_session)
app.router.add_get('/client/{token}', client_ws_handler)
app.router.add_get('/agent', agent_ws_handler)

if __name__ == '__main__':
    web.run_app(app, port=8000)
