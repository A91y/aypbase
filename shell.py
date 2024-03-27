import socket
import json

SERVER_HOST = "localhost"
SERVER_PORT = 12345

def send_query(query):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((SERVER_HOST, SERVER_PORT))
        s.sendall(query.encode())
        response = s.recv(1024).decode()
        return response

def main():
    print("Welcome to Interactive Database Shell")
    print("Available commands:")
    print("CREATE TABLE table_name")
    print("INSERT INTO table_name row_data")
    print("SELECT * FROM table_name")
    print("EXIT to quit")

    while True:
        query = input(">>> ").strip()
        if query.upper() == "EXIT":
            print("Exiting...")
            break
        response = send_query(query)
        print("Response:", response)

if __name__ == "__main__":
    main()
