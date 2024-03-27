import socket
import json
import time

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def disconnect(self):
        self.socket.close()

    def send_message(self, message):
        self.socket.sendall(message.encode())

    def receive_message(self):
        return self.socket.recv(1024).decode()

if __name__ == "__main__":
    client = Client("localhost", 12345)  # Assuming server is running on localhost:12345
    client.connect()

    print("Connected to server")

    # Simulate client operations
    time.sleep(1)
    print("Creating table...")
    client.send_message("CREATE TABLE users")
    response = client.receive_message()
    print("Server response:", response)

    time.sleep(1)
    print("Inserting row...")
    row_data = {"id": 1, "name": "John"}
    client.send_message(f"INSERT INTO users {json.dumps(row_data)}")
    response = client.receive_message()
    print("Server response:", response)

    time.sleep(1)
    print("Retrieving data...")
    client.send_message("SELECT * FROM users")
    response = client.receive_message()
    print("Data from server:", response)

    client.disconnect()
    print("Disconnected from server")
