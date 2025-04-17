import socket
import json
import sys
import threading
import signal
import time
import argparse
from datetime import datetime

active_clients = {}  # Maps client sockets to their nickname and info
client_lock = threading.Lock()  # Lock for thread-safe client dictionary access
active_nicknames = set()  # Set of currently used nicknames
shutdown_event = threading.Event()  # Event to signal all threads to terminate
BUFFER_SIZE = 4096


def parse_arguments():
    """Parse command line arguments using argparse."""
    parser = argparse.ArgumentParser(description="ChatServer for TCP connections")
    parser.add_argument("port", type=int, help="Port to listen on")

    # Custom error handler to match the required format
    def error_handler(message):
        print(f"ERR - arg 1")
        sys.exit(1)

    parser.error = error_handler

    try:
        args = parser.parse_args()

        # Validate port range
        if args.port <= 0 or args.port >= 65536:
            print(f"ERR - arg 1")
            sys.exit(1)

        return args.port
    except SystemExit:
        # This catches the sys.exit from argparse's error handling
        sys.exit(1)


def get_timestamp():
    """Returns current timestamp in the required format."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_connection(nickname, status):
    """Logs client connection status."""
    timestamp = get_timestamp()
    print(f"{timestamp} :: {nickname}: {status}.")


def log_received_message(
    client_address, client_nickname, client_id, timestamp, msg_size
):
    """Logs received message details."""
    print(
        f"Received: IP:{client_address[0]}, Port:{client_address[1]}, "
        f"Client-Nickname:{client_nickname}, ClientID:{client_id}, "
        f"Date/Time:{timestamp}, Msg-Size:{msg_size}"
    )


def log_broadcast(nicknames):
    """Logs the list of clients a message was broadcasted to."""
    if nicknames:
        print(f"Broadcasted: {', '.join(nicknames)}")


def create_error_response(message):
    """Creates an error response object."""
    return {"type": "error", "message": message}


def create_ok_response():
    """Creates a success response object."""
    return {"type": "ok"}


def create_broadcast_message(nickname, message_content, timestamp):
    """Creates a broadcast message object."""
    return {
        "type": "broadcast",
        "nickname": nickname,
        "message": message_content,
        "timestamp": timestamp,
    }


def encode_message(message_data):
    """Encodes a message object to JSON string and then to bytes."""
    try:
        message_json = json.dumps(message_data)
        return message_json.encode("utf-8")
    except Exception as e:
        print(f"ERR - Error encoding message: {e}")
        return None


def send_message(client_socket, message_data):
    """Sends a JSON message to a client."""
    try:
        encoded_message = encode_message(message_data)
        if encoded_message:
            client_socket.sendall(encoded_message)
            return True
        return False
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
        clients_to_broadcast = [
            (socket, info)
            for socket, info in active_clients.items()
            if socket != sender_socket
        ]

    for client_socket, client_info in clients_to_broadcast:
        if send_message(client_socket, message_data):
            broadcasted_to.append(client_info["nickname"])

    log_broadcast(broadcasted_to)


def receive_data(client_socket):
    """Receives raw data from client socket."""
    try:
        data = client_socket.recv(BUFFER_SIZE)
        return data
    except Exception as e:
        print(f"ERR - Error receiving data: {e}")
        return None


def parse_json_message(data):
    """Parses raw data into JSON message."""
    if not data:
        return None

    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"ERR - Invalid JSON: {e}")
        return None
    except Exception as e:
        print(f"ERR - Error parsing message: {e}")
        return None


def process_nickname_request(client_socket, message, client_address):
    """Processes a nickname registration request."""
    client_nickname = message.get("nickname")
    client_id = message.get("clientID")

    with client_lock:
        if client_nickname in active_nicknames:
            error_msg = "Nickname is already in use. Please choose a different one."
            send_message(client_socket, create_error_response(error_msg))
            return None, None

        active_nicknames.add(client_nickname)
        active_clients[client_socket] = {
            "nickname": client_nickname,
            "clientID": client_id,
        }

    send_message(client_socket, create_ok_response())
    log_connection(client_nickname, "connected")

    return client_nickname, client_id


def process_message_request(
    client_socket, message, client_address, client_nickname, client_id
):
    """Processes a chat message request."""
    message_content = message.get("message", "")
    timestamp = message.get("timestamp", get_timestamp())
    msg_size = len(message_content)

    log_received_message(
        client_address, client_nickname, client_id, timestamp, msg_size
    )

    broadcast_data = create_broadcast_message(
        client_nickname, message_content, timestamp
    )
    broadcast_message(client_socket, broadcast_data)


def remove_client(client_socket, client_nickname):
    """Removes a client from active clients list."""
    with client_lock:
        if client_socket in active_clients:
            del active_clients[client_socket]
        active_nicknames.discard(client_nickname)

    try:
        client_socket.close()
    except Exception:
        pass

    if client_nickname:
        log_connection(client_nickname, "disconnected")


def handle_client_messages(client_socket, client_address, client_nickname, client_id):
    """Handles messages from an authenticated client."""
    try:
        while not shutdown_event.is_set():
            data = receive_data(client_socket)
            if not data:
                break

            message = parse_json_message(data)
            if not message:
                break

            message_type = message.get("type")

            if message_type == "message":
                process_message_request(
                    client_socket, message, client_address, client_nickname, client_id
                )
            elif message_type == "disconnect":
                break

    except Exception as e:
        print(f"ERR - {e}")
    finally:
        remove_client(client_socket, client_nickname)


def handle_client(client_socket, client_address):
    """Initial handler for new client connections."""
    client_nickname = None
    client_id = None

    try:
        # Wait for initial nickname registration
        data = receive_data(client_socket)
        if not data:
            return

        message = parse_json_message(data)
        if not message:
            return

        message_type = message.get("type")

        if message_type == "nickname":
            client_nickname, client_id = process_nickname_request(
                client_socket, message, client_address
            )

            if client_nickname and client_id:
                handle_client_messages(
                    client_socket, client_address, client_nickname, client_id
                )

    except Exception as e:
        print(f"ERR - {e}")
    finally:
        if client_nickname is None:
            try:
                client_socket.close()
            except Exception:
                pass


def setup_server_socket(port):
    """Creates and configures the server socket."""
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server_socket.bind(("", port))
        except OSError as e:
            print(f"ERR - cannot create ChatServer socket using port number {port}")
            sys.exit(1)

        server_socket.listen(5)
        return server_socket

    except Exception as e:
        print(f"ERR - Error setting up server socket: {e}")
        sys.exit(1)


def get_local_ip():
    """Gets the local IP address of the server."""
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "127.0.0.1"  # Fallback to localhost


def accept_connections(server_socket):
    """Accepts and handles incoming client connections."""
    while not shutdown_event.is_set():
        try:
            server_socket.settimeout(1.0)  # Allow checking shutdown_event periodically
            client_socket, client_address = server_socket.accept()

            client_thread = threading.Thread(
                target=handle_client, args=(client_socket, client_address), daemon=True
            )
            client_thread.start()

        except socket.timeout:
            continue
        except Exception as e:
            if not shutdown_event.is_set():
                print(f"ERR - {e}")
            break


def close_all_connections():
    """Closes all client connections."""
    with client_lock:
        for client_socket in list(active_clients.keys()):
            try:
                client_socket.close()
            except Exception:
                pass
        active_clients.clear()
        active_nicknames.clear()


def signal_handler(sig, frame):
    """Handles Ctrl+C and other termination signals."""
    print("\nShutting down server...")
    shutdown_event.set()

    # Give ongoing operations a chance to complete
    time.sleep(0.5)
    close_all_connections()
    sys.exit(0)


def main():
    """Main server function."""
    port = parse_arguments()

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    server_socket = setup_server_socket(port)

    try:
        host_ip = get_local_ip()
        print(f"ChatServer started with server IP: {host_ip}, port: {port} ...")

        accept_connections(server_socket)

    except Exception as e:
        print(f"ERR - {e}")

    finally:
        if server_socket:
            try:
                server_socket.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
