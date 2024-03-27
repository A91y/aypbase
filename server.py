import threading
import socket
import os
import json

# Define the directory for storing data
DATA_DIRECTORY = "data"

# Database storage using files
class Database:
    def __init__(self):
        self.create_data_directory()

    def create_data_directory(self):
        if not os.path.exists(DATA_DIRECTORY):
            os.makedirs(DATA_DIRECTORY)

    def save_table_data(self, table_name, data):
        file_path = os.path.join(DATA_DIRECTORY, f"{table_name}.ayp")
        with open(file_path, 'w') as file:
            json.dump(data, file)

    def load_table_data(self, table_name):
        file_path = os.path.join(DATA_DIRECTORY, f"{table_name}.ayp")
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                return json.load(file)
        else:
            return []

    def create_table(self, table_name):
        file_path = os.path.join(DATA_DIRECTORY, f"{table_name}.ayp")
        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                json.dump([], file)

    def insert_row(self, table_name, row_data):
        data = self.load_table_data(table_name)
        data.append(row_data)
        self.save_table_data(table_name, data)

    def get_table_data(self, table_name):
        return self.load_table_data(table_name)

# Server handling client connections
class Server:
    def __init__(self, db, host, port):
        self.db = db
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host, port))
        self.socket.listen(1)

    def start(self):
        print(f"Server listening on {self.host}:{self.port}")
        while True:
            client_socket, _ = self.socket.accept()
            client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
            client_thread.start()

    def handle_client(self, client_socket):
        with client_socket:
            while True:
                try:
                    request = client_socket.recv(1024).decode().strip()
                    if not request:
                        break
                    if request.startswith("CREATE TABLE"):
                        *_, table_name = request.split(" ", 2)
                        print("----CREATE TABLE----", "\ntable_name:", table_name, "\n----CREATE TABLE----")
                        self.db.create_table(table_name)
                        client_socket.sendall(f"Table '{table_name}' created successfully!".encode())
                    elif request.startswith("INSERT INTO"):
                        *_, table_name, row_data = request.split(" ", 3)
                        print("----INSERT INTO----", "\ntable_name:", table_name, "\nrow_data:", row_data, "\n----INSERT INTO----")
                        row_data = json.loads(row_data)
                        self.db.insert_row(table_name, row_data)
                        client_socket.sendall(b"Row inserted successfully!")
                    elif request.startswith("SELECT * FROM"):
                        *_, table_name = request.split(" ", 3)
                        print("----SELECT * FROM----", "\ntable_name:", table_name, "\n----SELECT * FROM----")
                        table_data = self.db.get_table_data(table_name)
                        client_socket.sendall(json.dumps(table_data).encode())
                    else:
                        client_socket.sendall(b"Invalid request!")
                except Exception as e:
                    print("Error:", e)
                    client_socket.sendall(str(e).encode())
                    break

if __name__ == "__main__":
    db = Database()
    server = Server(db, "localhost", 12345)
    server.start()
