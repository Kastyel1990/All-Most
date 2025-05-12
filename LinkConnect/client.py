import subprocess
import time
import argparse
import uuid
import os
import shutil
import socket
import threading
import websocket
from websocket import create_connection

def is_tool_installed(tool):
    return shutil.which(tool) is not None

def install_tool(tool, pkg_managers):
    print(f"[*] Установка {tool}...")
    for manager, install_cmd in pkg_managers.items():
        if shutil.which(manager):
            try:
                subprocess.run(install_cmd + [tool], check=True)
                return True
            except subprocess.CalledProcessError:
                print(f"[!] Не удалось установить {tool} с помощью {manager}")
    return False

def ensure_dependencies():
    missing = []
    for tool in ["x11vnc"]:
        if not is_tool_installed(tool):
            missing.append(tool)

    if not missing:
        return True

    print(f"[!] Отсутствуют необходимые пакеты: {', '.join(missing)}")

    pkg_managers = {
        "apt": ["sudo", "apt", "install", "-y"],
        "apt-get": ["sudo", "apt-get", "install", "-y"],
        "dnf": ["sudo", "dnf", "install", "-y"]
    }

    for tool in missing:
        if not install_tool(tool, pkg_managers):
            print(f"[!] Установите {tool} вручную.")
            return False

    return True

def start_x11vnc(display=":0", port=5900):
    return subprocess.Popen([
        "x11vnc", "-display", display,
        "-rfbport", str(port),
        "-forever", "-nopw", "-shared",
        "-o", "/dev/null"
    ])

def find_free_port():
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def connect_vnc(port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", port))
    return sock

def tunnel_vnc_to_ws(vnc_sock, ws):
    try:
        while True:
            data = vnc_sock.recv(4096)
            if not data:
                break
            ws.send(data, opcode=websocket.ABNF.OPCODE_BINARY)
    except Exception as e:
        print(f"[!] Ошибка VNC->WS: {e}")
    finally:
        ws.close()

def tunnel_ws_to_vnc(ws, vnc_sock):
    try:
        while True:
            data = ws.recv()
            if not data:
                break
            vnc_sock.send(data)
    except Exception as e:
        print(f"[!] Ошибка WS->VNC: {e}")
    finally:
        vnc_sock.close()

def main():
    parser = argparse.ArgumentParser(description="Reverse VNC Agent")
    parser.add_argument("--duration", type=int, default=3600, help="Сколько секунд действует сессия (по умолчанию: 3600)")
    parser.add_argument("--display", default=":0", help="DISPLAY для x11vnc (по умолчанию: :0)")
    parser.add_argument("--server-host", default="185.105.118.106", help="Хост сервера (по умолчанию: localhost)")
    parser.add_argument("--server-port", type=int, default=8000, help="Порт сервера (по умолчанию: 8000)")
    args = parser.parse_args()

    if not ensure_dependencies():
        return

    vnc_port = find_free_port()
    token = str(uuid.uuid4())
    link = f"http://{args.server_host}:{args.server_port}/session/{token}"

    print(f"[*] Запуск x11vnc на порту {vnc_port}...")
    vnc_proc = start_x11vnc(display=args.display, port=vnc_port)
    time.sleep(2)

    try:
        print(f"[*] Подключение к WebSocket серверу...")
        ws_url = f"ws://{args.server_host}:8001/agent?token={token}"
        ws = websocket.create_connection(
            ws_url,
            header=["Sec-WebSocket-Protocol: binary"]
        )
        vnc_sock = connect_vnc(vnc_port)

        print(f"[+] Сессия доступна по ссылке: {link}")
        print(f"[i] Время жизни: {args.duration} секунд")

        t1 = threading.Thread(target=tunnel_vnc_to_ws, args=(vnc_sock, ws))
        t2 = threading.Thread(target=tunnel_ws_to_vnc, args=(ws, vnc_sock))
        t1.start()
        t2.start()

        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("[!] Прервано пользователем.")
    except Exception as e:
        print(f"[!] Ошибка в основном процессе: {e}")
    finally:
        print("[*] Завершение сессии...")
        vnc_proc.terminate()

if __name__ == "__main__":
    main()
