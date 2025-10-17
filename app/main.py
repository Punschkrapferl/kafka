import socket
import struct


def main():
    server_socket = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Listening on localhost:9092")

    while True:
        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        with conn:
            data = conn.recv(1024)

            request_api_version = struct.unpack(">h", data[6:8])[0] if len(data) >= 8 else 0
            correlation_id = struct.unpack(">i", data[8:12])[0] if len(data) >= 12 else 0

            error_code = 0 if 0 <= request_api_version <= 4 else 35
            throttle_time_ms = 0

            correlation_id_bytes = struct.pack(">i", correlation_id)
            throttle_time_bytes = struct.pack(">i", throttle_time_ms)
            error_code_bytes = struct.pack(">h", error_code)

            if error_code == 0:
                api_keys_count = b"\x02"  # 1 element compact array
                # ApiVersions entry with tag buffer
                api_key_bytes = struct.pack(">h", 18)  # APIKey
                min_version_bytes = struct.pack(">h", 0)
                max_version_bytes = struct.pack(">h", 4)
                api_key_tag_buffer = b"\x00"  # required per-entry
                api_entry_bytes = api_key_bytes + min_version_bytes + max_version_bytes + api_key_tag_buffer

                outer_tag_buffer = b"\x00"
                body = (correlation_id_bytes + throttle_time_bytes +
                        error_code_bytes + api_keys_count +
                        api_entry_bytes + outer_tag_buffer)
            else:
                outer_tag_buffer = b"\x00"
                body = correlation_id_bytes + throttle_time_bytes + error_code_bytes + outer_tag_buffer

            message_size = struct.pack(">i", len(body))
            response = message_size + body

            conn.sendall(response)
            print(f"Sent response ({len(response)} bytes): {response.hex()}")


if __name__ == "__main__":
    main()
