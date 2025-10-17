import socket
import struct


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Listening on localhost:9092")

    while True:
        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        with conn:
            data = conn.recv(1024)
            print(f"Received {len(data)} bytes: {data.hex()}")

            # Extract request_api_version (bytes 6–8)
            if len(data) >= 8:
                request_api_version_bytes = data[6:8]
                request_api_version = struct.unpack(">h", request_api_version_bytes)[0]
            else:
                request_api_version = 0
            print(f"Parsed request_api_version = {request_api_version}")

            # Extract correlation_id (bytes 8–12)
            if len(data) >= 12:
                correlation_id_bytes = data[8:12]
                correlation_id = struct.unpack(">i", correlation_id_bytes)[0]
            else:
                correlation_id = 0
            print(f"Parsed correlation_id = {correlation_id}")

            # Determine error_code
            # Supports versions 0–4; anything else = error 35
            error_code = 0 if 0 <= request_api_version <= 4 else 35
            print(f"Responding with error_code = {error_code}")

            # Construct response
            message_size = struct.pack(">i", 0)  # placeholder (not validated yet)
            correlation_id_bytes = struct.pack(">i", correlation_id)
            error_code_bytes = struct.pack(">h", error_code)

            response = message_size + correlation_id_bytes + error_code_bytes

            conn.sendall(response)
            print(f"Sent correlation_id = {correlation_id}, error_code = {error_code}")


if __name__ == "__main__":
    main()
