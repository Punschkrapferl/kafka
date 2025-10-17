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
            # Read up to 1024 bytes from the client
            data = conn.recv(1024)
            print(f"Received {len(data)} bytes: {data.hex()}")

            # Extract correlation_id from offset 8–12 (after size + api_key + api_version)
            if len(data) >= 12:
                correlation_id_bytes = data[8:12]
                correlation_id = struct.unpack(">i", correlation_id_bytes)[0]
            else:
                correlation_id = 0  # fallback if message is too short

            print(f"Parsed correlation_id = {correlation_id}")

            # Prepare response
            message_size = struct.pack(">i", 0)  # placeholder for now
            response = message_size + struct.pack(">i", correlation_id)

            conn.sendall(response)
            print(f"Sent correlation_id = {correlation_id}")


if __name__ == "__main__":
    main()
