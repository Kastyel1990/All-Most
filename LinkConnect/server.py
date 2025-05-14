import subprocess
import time
import argparse
import uuid
import shutil
import socket
import threading
import os
import sys
import websocket
from websocket import create_connection
import ssl

def local_tool_path(tool):
    """Ищет бинарник рядом со скриптом."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(script_dir, tool)
    if os.path.isfile(local_path) and os.access(local_path, os.X_OK):
        return local_path
    return None

def find_tool(tool):
    """Возвращает путь к инструменту: локальный или из PATH, иначе None."""
    local = local_tool_path(tool)
    if local:
        return local
    system = shutil.which(tool)
    return system

def is_tool_installed(tool):
    """Проверяет, установлен ли инструмент или лежит рядом со скриптом."""
    return shutil.which(tool) is not None or local_tool_path(tool) is not None

def install_tool(tool, pkg_managers):
    """Устанавливает указанный инструмент."""
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
    """Проверяет, есть ли x0vncserver (рядом или в системе)."""
    if not is_tool_installed("x0vncserver"):
        print("[!] Не найден x0vncserver! Положите исполняемый файл x0vncserver рядом со скриптом.")
        return False
    return True

def ask_sudo_password():
    """Явно запросить пароль sudo у пользователя (если надо)."""
    try:
        # -v обновит таймер sudo или запросит пароль
        subprocess.run(['sudo', '-v'], check=True)
    except subprocess.CalledProcessError:
        print("Ошибка: sudo не доступен или неверный пароль.")
        sys.exit(1)

def start_vnc_server(port=5900, display=":0"):
    # Проверяем локальный бинарник
    x0vnc_path = local_tool_path("x0vncserver") or shutil.which("x0vncserver")
    if x0vnc_path:
        return subprocess.Popen([
            x0vnc_path,
            "-display", display,
            "-rfbport", str(port),
            "-SecurityTypes", "None",
            "-AlwaysShared"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Fallback на x11vnc если есть (по $PATH)
    x11vnc_path = shutil.which("x11vnc")
    if x11vnc_path:
        return subprocess.Popen([
            x11vnc_path,
            "-rfbport", str(port),
            "-forever",
            "-shared",
            "-nopw",
            "-display", display,
            "-repeat",
            "-noncache"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print("[!] Нет подходящего VNC-сервера (x0vncserver или x11vnc).")
    sys.exit(1)

def find_free_port():
    """Находит свободный порт."""
    sock = socket.socket()
    sock.bind(('', 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

def connect_vnc(port=5900):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("127.0.0.1", port))
    return sock

def tunnel_vnc_to_ws(vnc_sock, ws):
    """Передает данные из VNC в WebSocket."""
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
    """Передает данные из WebSocket в VNC."""
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
    parser.add_argument("--server-port", type=int, default=8443, help="Порт сервера (по умолчанию: 8443)")
    args = parser.parse_args()

    if not ensure_dependencies():
        return

    vnc_port = find_free_port()
    token = str(uuid.uuid4())
    link = f"https://{args.server_host}:{args.server_port}/session/{token}"

    print(f"[*] Запуск x0vncserver на порту {vnc_port}...")
    vnc_proc = start_vnc_server(port=vnc_port, display=args.display)
    time.sleep(5)

    if vnc_proc.poll() is not None:
        print("[!] VNC сервер завершился с ошибкой!")
        stdout, stderr = vnc_proc.communicate()
        print("STDOUT:", stdout.decode())
        print("STDERR:", stderr.decode())
        sys.exit(1)

    try:
        print(f"[*] Подключение к WebSocket серверу...")
        protocol = "wss" if args.server_port == 443 or args.server_port == 8443 else "ws"
        ws_url = f"{protocol}://{args.server_host}:{args.server_port}/agent?token={token}"
        ssl_context = ssl._create_unverified_context()
        ws = websocket.create_connection(ws_url, sslopt={"cert_reqs": ssl.CERT_NONE})
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
