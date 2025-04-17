import socket
import json
import sys
import threading
from datetime import datetime

BUFFER_SIZE = 4096
CHARS_RCV = 0
CHARS_SENT = 0
MSGS_SENT = 0
MSGS_RCV = 0


def parse_arguments():
    """Parses command-line arguments according to assignment requirements."""
    if len(sys.argv) != 5:
        print(f"ERR - arg count (expected 4, got {len(sys.argv)-1})")
        sys.exit(1)

    hostname = sys.argv[1]

    try:
        port = int(sys.argv[2])
        if port <= 0 or port >= 65536:
            print(f"ERR - arg 2")
            sys.exit(1)
    except ValueError:
        print(f"ERR - arg 2")
        sys.exit(1)

    nickname = sys.argv[3]
    client_id = sys.argv[4]

    return hostname, port, nickname, client_id


def create_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_message(sock, data):
    try:
        message_json = json.dumps(data)
        sock.sendall(message_json.encode("utf-8"))
        return True
    except (BrokenPipeError, ConnectionResetError) as e:
        print(f"ERR - {e}")
        return False
    except Exception as e:
        print(f"ERR - {e}")
        return False


def receive_message(sock):
    """Receives, decodes, and parses JSON data from the socket.
    Returns message dict or None on error/closed connection."""
    try:
        resp_bytes = sock.recv(BUFFER_SIZE)

        if not resp_bytes:
            return None

        resp_str = resp_bytes.decode("utf-8")
        try:
            resp_data = json.loads(resp_str)
            return resp_data
        except json.JSONDecodeError as e:
            print(f"ERR - {e}")
            return {"type": "error", "message": "Invalid JSON received from server"}

    except ConnectionResetError as e:
        print(f"ERR - {e}")
        return None
    except OSError as e:
        if e.errno == 9:  # Bad file descriptor (socket closed)
            return None
        else:
            print(f"ERR - {e}")
            return None
    except Exception as e:
        print(f"ERR - {e}")
        return None


def receive_messages_loop(sock, nickname, shutdown_event):
    global MSGS_RCV
    global CHARS_RCV
    while not shutdown_event.is_set():
        received_data = receive_message(sock)

        if received_data is None:
            if not shutdown_event.is_set():
                print("Connection lost to server. Press Enter to exit.")
                shutdown_event.set()
            break

        elif isinstance(received_data, dict):
            msg_type = received_data.get("type", "unknown")

            if msg_type == "broadcast":
                sender = received_data.get("nickname", "Server")
                message = received_data.get("message", "")
                timestamp = received_data.get("timestamp", "")

                print(f"{timestamp} :: {sender}: {message}")
                CHARS_RCV += len(message)
                MSGS_RCV += 1

            elif msg_type == "error":
                message = received_data.get("message", "Unknown error")
                print(f"Server error: {message}")


def run_client(host, port, nickname, client_id):
    """Handles the main client connection, input, and starts receive thread."""
    global MSGS_SENT
    global CHARS_SENT
    start_time = create_timestamp()
    shutdown_event = threading.Event()
    receive_thread = None

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((host, port))
            print(
                f"ChatClient started with server IP: {host}, port: {port}, "
                f"nickname: {nickname}, client ID: {client_id}, Date/Time: {create_timestamp()}"
            )

            initial_data = {
                "type": "nickname",
                "nickname": nickname,
                "clientID": client_id,
                "timestamp": create_timestamp(),
            }
            if not send_message(client_socket, initial_data):
                print("Failed to send initial identification. Exiting.")
                return

            initial_response = receive_message(client_socket)

            if initial_response is None:
                print("No response from server or connection lost. Exiting.")
                return

            if initial_response.get("type") == "error":
                error_msg = initial_response.get(
                    "message", "Unknown error from server."
                )
                print(f"Server Error: {error_msg}")
                print("Exiting due to server error.")
                return

            receive_thread = threading.Thread(
                target=receive_messages_loop,
                args=(client_socket, nickname, shutdown_event),
                daemon=True,
            )
            receive_thread.start()

            print("Enter message:")

            while not shutdown_event.is_set():
                try:
                    message_text = input()

                    if shutdown_event.is_set():
                        break

                    if message_text.lower() == "disconnect":
                        disconnect_message = {
                            "type": "disconnect",
                            "nickname": nickname,
                            "clientID": client_id,
                        }
                        send_message(client_socket, disconnect_message)
                        shutdown_event.set()
                        break

                    message_data = {
                        "type": "message",
                        "nickname": nickname,
                        "message": message_text,
                        "timestamp": create_timestamp(),
                    }
                    MSGS_SENT += 1
                    CHARS_SENT += len(message_text)

                    if not send_message(client_socket, message_data):
                        shutdown_event.set()
                        break

                except KeyboardInterrupt:
                    shutdown_event.set()
                    disconnect_message = {
                        "type": "disconnect",
                        "nickname": nickname,
                        "clientID": client_id,
                    }
                    send_message(client_socket, disconnect_message)
                    break
                except EOFError:
                    print("Disconnecting (EOF detected)")
                    shutdown_event.set()
                    disconnect_message = {
                        "type": "disconnect",
                        "nickname": nickname,
                        "clientID": client_id,
                    }
                    send_message(client_socket, disconnect_message)
                    break
                except Exception as e:
                    if not shutdown_event.is_set():
                        print(f"ERR - {e}")
                        shutdown_event.set()
                    break

    except socket.gaierror as e:
        print(f"ERR - {e}")
        shutdown_event.set()
    except ConnectionRefusedError as e:
        print(f"ERR - {e}")
        shutdown_event.set()
    except socket.timeout as e:
        print(f"ERR - {e}")
        shutdown_event.set()
    except Exception as e:
        print(f"ERR - {e}")
        shutdown_event.set()
    finally:
        shutdown_event.set()
        if receive_thread and receive_thread.is_alive():
            receive_thread.join(timeout=1.0)
        end_time_str = create_timestamp()
        print(
            f"Summary: start: {start_time}, end: {end_time_str}, "
            f"msg sent:{MSGS_SENT}, msg rcv:{MSGS_RCV}, "
            f"char sent:{CHARS_SENT}, char rcv:{CHARS_RCV}"
        )


def main():
    """Parses arguments and starts the client."""
    hostname, port, nickname, client_id = parse_arguments()
    run_client(hostname, port, nickname, client_id)


if __name__ == "__main__":
    main()
