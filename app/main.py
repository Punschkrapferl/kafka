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

            request_api_version = struct.unpack(">h", data[6:8])[0] if len(data) >= 8 else 0
            correlation_id = struct.unpack(">i", data[8:12])[0] if len(data) >= 12 else 0

            error_code = 0 if 0 <= request_api_version <= 4 else 35

            correlation_id_bytes = struct.pack(">i", correlation_id)
            error_code_bytes = struct.pack(">h", error_code)

            if error_code == 0:
                # Compact array (1 element → 0x02)
                api_keys_count_bytes = b"\x02"
                api_key_bytes = struct.pack(">h", 18)  # ApiVersions
                min_version_bytes = struct.pack(">h", 0)
                max_version_bytes = struct.pack(">h", 4)
                api_entry_bytes = api_key_bytes + min_version_bytes + max_version_bytes
                tag_buffer = b"\x00"
                body = correlation_id_bytes + error_code_bytes + api_keys_count_bytes + api_entry_bytes + tag_buffer
            else:
                tag_buffer = b"\x00"
                body = correlation_id_bytes + error_code_bytes + tag_buffer

            message_size = struct.pack(">i", len(body))
            response = message_size + body

            conn.sendall(response)
            print(f"Sent ({len(response)} bytes): {response.hex()}")


if __name__ == "__main__":
    main()
