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

def print_help():
    print("Available commands:")
    print("CREATE TABLE table_name")
    print("INSERT INTO table_name row_data")
    print("SELECT * FROM table_name [WHERE condition] [ORDER BY column_name ASC|DESC] [LIMIT number]")
    print("DELETE TABLE table_name")
    print("SHOW TABLES")
    print("HELP")
    print("EXIT to quit")

def main():
    print("Welcome to Interactive Database Shell")
    print("Type 'HELP' for available commands")

    while True:
        query = input(">>> ").strip()
        if not query:
            continue
        if query.upper() == "EXIT":
            print("Exiting...")
            break
        elif query.upper() == "HELP":
            print_help()
        else:
            response = send_query(query)
            print("Response:", response)

if __name__ == "__main__":
    main()
