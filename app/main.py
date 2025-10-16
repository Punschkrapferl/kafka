import socket
import struct


def main():
    print("Logs from your program will appear here!")

    # Create a TCP server socket on port 9092
    server_socket = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Listening on localhost:9092")

    while True:
        # Wait for a client to connect
        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        with conn:
            # Read the request (we can ignore the content for this stage)
            _ = conn.recv(1024)

            # Prepare Kafka-style response
            # message_size: 4 bytes (any value works now, so 0)
            # correlation_id: 4 bytes (must be 7)
            message_size = struct.pack(">i", 0)
            correlation_id = struct.pack(">i", 7)
            response = message_size + correlation_id

            # Send the response back to the client
            conn.sendall(response)
            print("Sent correlation_id = 7")


if __name__ == "__main__":
    main()
