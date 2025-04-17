import socket
import json
import sys
import threading
import signal
import time
import argparse
from datetime import datetime


active_clients = {}
client_lock = threading.Lock()
active_nicknames = set()
shutdown_event = threading.Event()
BUFFER_SIZE = 4096


def parse_arguments():
    """Parse command line arguments using argparse."""
    parser = argparse.ArgumentParser(description="ChatServer for TCP connections")
    parser.add_argument("port", type=int, help="Port to listen on")

    def error_handler(message):
        print(f"ERR - arg 1")
        sys.exit(1)

    parser.error = error_handler

    try:
        args = parser.parse_args()

        if args.port <= 0 or args.port >= 65536:
            print(f"ERR - arg 1")
            sys.exit(1)

        return args.port
    except SystemExit:
        sys.exit(1)


def get_timestamp():
    """Returns current timestamp in the required format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_message(client_socket, message_data):
    """Sends a JSON message to a client."""
    try:
        message_json = json.dumps(message_data)
        client_socket.sendall(message_json.encode("utf-8"))
        return True
    except (BrokenPipeError, ConnectionResetError) as e:
        print(f"ERR - {e}")
        return False
    except Exception as e:
        print(f"ERR - {e}")
        return False


def broadcast_message(sender_socket, message_data):
    """Broadcasts message to all clients except the sender."""
    broadcasted_to = []

    with client_lock:
        for client_socket, client_info in active_clients.items():
            if client_socket != sender_socket:
                if send_message(client_socket, message_data):
                    broadcasted_to.append(client_info["nickname"])

    if broadcasted_to:
        print(f"Broadcasted: {', '.join(broadcasted_to)}")


def handle_client(client_socket, client_address):
    """Handles client connection and messages."""
    client_nickname = None
    client_id = None

    try:
        while not shutdown_event.is_set():
            data = client_socket.recv(BUFFER_SIZE)
            if not data:
                break

            # Parse the message - no need for try/catch as this is internal use
            message = json.loads(data.decode("utf-8"))
            message_type = message.get("type")

            if message_type == "nickname":
                client_nickname = message.get("nickname")
                client_id = message.get("clientID")

                with client_lock:
                    if client_nickname in active_nicknames:
                        error_response = {
                            "type": "error",
                            "message": "Nickname is already in use. Please choose a different one.",
                        }
                        send_message(client_socket, error_response)
                        return

                    active_nicknames.add(client_nickname)
                    active_clients[client_socket] = {
                        "nickname": client_nickname,
                        "clientID": client_id,
                    }

                # Acknowledge successful connection
                send_message(client_socket, {"type": "ok"})

                timestamp = get_timestamp()
                print(f"{timestamp} :: {client_nickname}: connected.")

            elif message_type == "message":
                if client_nickname:
                    message_content = message.get("message", "")
                    timestamp = message.get("timestamp", get_timestamp())
                    msg_size = len(message_content)

                    print(
                        f"Received: IP:{client_address[0]}, Port:{client_address[1]}, "
                        f"Client-Nickname:{client_nickname}, ClientID:{client_id}, "
                        f"Date/Time:{timestamp}, Msg-Size:{msg_size}"
                    )

                    broadcast_data = {
                        "type": "broadcast",
                        "nickname": client_nickname,
                        "message": message_content,
                        "timestamp": timestamp,
                    }

                    broadcast_message(client_socket, broadcast_data)

            elif message_type == "disconnect":
                break

    except (ConnectionResetError, BrokenPipeError) as e:
        print(f"ERR - {e}")
    except json.JSONDecodeError as e:
        print(f"ERR - Invalid JSON: {e}")
    except Exception as e:
        print(f"ERR - {e}")
    finally:
        if client_nickname:
            with client_lock:
                if client_socket in active_clients:
                    del active_clients[client_socket]
                active_nicknames.discard(client_nickname)

            timestamp = get_timestamp()
            print(f"{timestamp} :: {client_nickname}: disconnected.")

        try:
            client_socket.close()
        except Exception:
            pass


def signal_handler(sig, frame):
    """Handles Ctrl+C and other termination signals."""
    print("\nShutting down server...")
    shutdown_event.set()

    # Give ongoing operations a chance to complete
    time.sleep(0.5)

    # Close all client connections
    with client_lock:
        for client_socket in list(active_clients.keys()):
            try:
                client_socket.close()
            except Exception:
                pass
        active_clients.clear()
        active_nicknames.clear()

    sys.exit(0)


def run_server(port):
    """Runs the server on the specified port."""
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind(("", port))
        except OSError:
            print(f"ERR - cannot create ChatServer socket using port number {port}")
            sys.exit(1)

        server_socket.listen(5)

        host_ip = socket.gethostbyname(socket.gethostname())
        print(f"ChatServer started with server IP: {host_ip}, port: {port} ...")

        while not shutdown_event.is_set():
            try:
                server_socket.settimeout(
                    1.0
                )  # Allow checking shutdown_event periodically
                client_socket, client_address = server_socket.accept()

                client_thread = threading.Thread(
                    target=handle_client,
                    args=(client_socket, client_address),
                    daemon=True,
                )
                client_thread.start()

            except socket.timeout:
                continue
            except Exception as e:
                if not shutdown_event.is_set():
                    print(f"ERR - {e}")
                break

    except Exception as e:
        print(f"ERR - {e}")

    finally:
        if "server_socket" in locals():
            try:
                server_socket.close()
            except Exception:
                pass


def main():
    """Main server function."""
    port = parse_arguments()

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    run_server(port)


if __name__ == "__main__":
    main()
