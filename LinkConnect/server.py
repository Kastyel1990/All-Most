import asyncio
from aiohttp import web
import ssl

agent_connections = {}  # token -> {'ws_agent': ..., 'browser_future': ...}

# NOVNC_HTML = """
# <!DOCTYPE html>
# <html>
# <head>
#   <title>Remote Desktop</title>
#   <style>html, body, #noVNC_canvas { width:100%; height:100%; margin:0; background:#000; }</style>
# </head>
# <body>
#   <canvas id="noVNC_canvas"></canvas>
#   <script type="module">
#     import RFB from "/static/novnc/core/rfb.js";
#     const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
#     localStorage.debug = 'noVNC:*';
#     const rfb = new RFB(
#       document.getElementById('noVNC_canvas'),
#       protocol + '//' + location.host + '/client/{{token}}'
#     );
#     rfb.viewOnly = false;
#   </script>
# </body>
# </html>
# """

NOVNC_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>noVNC Test</title>
  <meta charset="utf-8">
  <style>
    html, body { height: 100%; margin: 0; background: #000; }
    #vnc_container { width: 100vw; height: 100vh; }
  </style>
</head>
<body>
  <div id="vnc_container"></div>
  <script type="module">
    import RFB from "/static/novnc/core/rfb.js";
    localStorage.debug = 'noVNC:*'; // включаем отладку
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const rfb = new RFB(
      document.getElementById('vnc_container'),
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

async def agent_ws_handler(request):
    token = request.rel_url.query.get("token")
    if not token:
        return web.Response(status=400, text="Missing token")

    ws_agent = web.WebSocketResponse()
    await ws_agent.prepare(request)

    browser_future = asyncio.Future()
    agent_connections[token] = {"ws_agent": ws_agent, "browser_future": browser_future}
    print(f"[+] Агент подключился: {token}")

    try:
        wait_browser = browser_future  # просто future!
        wait_agent = asyncio.create_task(_wait_ws_closed(ws_agent))
        done, pending = await asyncio.wait(
            [wait_browser, wait_agent],
            return_when=asyncio.FIRST_COMPLETED
        )
        if browser_future.done():
            ws_browser = browser_future.result()
            await relay(ws_agent, ws_browser)
    finally:
        agent_connections.pop(token, None)
        print(f"[-] Агент отключился: {token}")
        if not browser_future.done():
            browser_future.set_exception(Exception("Агент отключился до подключения браузера"))

    return ws_agent

async def _wait_ws_closed(ws):
    while not ws.closed:
        await asyncio.sleep(0.1)

async def client_ws_handler(request):
    token = request.match_info.get('token')
    if token not in agent_connections:
        return web.Response(status=404, text="No agent for this session")
    ws_browser = web.WebSocketResponse()
    await ws_browser.prepare(request)
    browser_future = agent_connections[token]["browser_future"]
    if not browser_future.done():
        browser_future.set_result(ws_browser)
    # Ждём закрытия браузерного WebSocket (aiohttp не имеет wait_closed)
    while not ws_browser.closed:
        await asyncio.sleep(0.1)
    return ws_browser

FIRST_PACKET_LOGGED = False

async def relay(ws_agent, ws_browser):
    async def relay_one(src, dst, direction):
      global FIRST_PACKET_LOGGED
      try:
          async for msg in src:
              if msg.type == web.WSMsgType.BINARY:
                  if direction == "agent->browser" and not FIRST_PACKET_LOGGED:
                      print("[DEBUG] First packet agent->browser:", msg.data[:64])
                      FIRST_PACKET_LOGGED = True
                  await dst.send_bytes(msg.data)
                  print(f"[relay {direction}] type={msg.type} len={len(msg.data)}")
              elif msg.type == web.WSMsgType.TEXT:
                  await dst.send_str(msg.data)
              else:
                  break
      except Exception as e:
          print(f"[!] Relay error ({direction}): {e}")
      finally:
          await dst.close()

    await asyncio.gather(
      relay_one(ws_agent, ws_browser, "agent->browser"),
      relay_one(ws_browser, ws_agent, "browser->agent")
    )

# Путь к noVNC — скорректируйте под свою структуру!
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
