import threading
import socket
import os
import json
import argparse
import logging

# Define the directory for storing data
DATA_DIRECTORY = "data"

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database storage using files


class Database:
    def __init__(self, verbose=False):
        self.verbose = verbose
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
            return True  # Table created successfully
        else:
            if self.verbose:
                logger.warning(f"Table '{table_name}' already exists!")
            return False  # Table already exists

    def insert_row(self, table_name, row_data):
        data = self.load_table_data(table_name)
        data.append(row_data)
        self.save_table_data(table_name, data)
        return True

    def get_table_data(self, table_name):
        return self.load_table_data(table_name)

    def delete_table(self, table_name):
        file_path = os.path.join(DATA_DIRECTORY, f"{table_name}.ayp")
        if os.path.exists(file_path):
            os.remove(file_path)
            if self.verbose:
                logger.info(f"Table '{table_name}' deleted successfully!")
            return True
        else:
            if self.verbose:
                logger.warning(f"Table '{table_name}' does not exist!")
            return False

    def select_data(self, table_name, conditions=None, order_by=None):
        table_data = self.load_table_data(table_name)
        if conditions:
            table_data = self.filter_data(table_data, conditions)
        if order_by:
            table_data = self.sort_data(table_data, order_by)
        return table_data

    def filter_data(self, table_data, conditions):
        filtered_data = []
        for row in table_data:
            if self.check_conditions(row, conditions):
                filtered_data.append(row)
        return filtered_data

    def check_conditions(self, row, conditions):
        for condition in conditions:
            column, operator, value = condition
            try:
                value = int(value)
            except:
                value = value
            if operator == "=":
                if row.get(column) != value:
                    return False
            elif operator == ">":
                if row.get(column) <= value:
                    return False
            elif operator == "<":
                if row.get(column) >= value:
                    return False
            elif operator == "!=":
                if row.get(column) == value:
                    return False
            elif operator == ">=":
                if row.get(column) < value:
                    return False
            elif operator == "<=":
                if row.get(column) > value:
                    return False
        return True

    def sort_data(self, table_data, order_by):
        column, order = order_by[0]
        allowed_orders = ["ASC", "DESC"]
        if order.upper() not in allowed_orders:
            if self.verbose:
                logger.warning(
                    f"Invalid order: {order}. Using default order 'ASC'")
            order = "ASC"
        return sorted(table_data, key=lambda x: x.get(column), reverse=(order.upper() == "DESC"))\


# Server handling client connections


class Server:
    def __init__(self, db, host, port, verbose=False):
        self.db = db
        self.host = host
        self.port = port
        self.verbose = verbose
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((host, port))
        self.socket.listen(1)

    def start(self):
        logger.info(f"Server listening on {self.host}:{self.port}")
        while True:
            client_socket, _ = self.socket.accept()
            client_thread = threading.Thread(
                target=self.handle_client, args=(client_socket,))
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
                        if self.verbose:
                            logger.info(
                                f"CREATE TABLE: table_name={table_name}")
                        self.db.create_table(table_name)
                        client_socket.sendall(
                            f"Table '{table_name}' created successfully!".encode())
                    elif request.startswith("INSERT INTO"):
                        *_, table_name, row_data = request.split(" ", 3)
                        if self.verbose:
                            logger.info(
                                f"INSERT INTO: table_name={table_name}, row_data={row_data}")
                        row_data = json.loads(row_data)
                        self.db.insert_row(table_name, row_data)
                        client_socket.sendall(b"Row inserted successfully!")
                    elif request.startswith("SELECT * FROM"):
                        parts = request.split(" ")
                        print(parts)
                        table_name = parts[3]
                        print(table_name)
                        conditions = None
                        order_by = None
                        if len(parts) > 4:
                            for i in range(4, len(parts)):
                                print(i, parts[i])
                                if parts[i].upper() == "WHERE":
                                    conditions = []
                                    while i + 2 < len(parts) and parts[i + 2].upper() not in ("ORDER", "LIMIT"):
                                        conditions.append(
                                            (parts[i + 1], parts[i + 2], parts[i + 3]))
                                        i += 3
                                        print("Inside WHERE",
                                              conditions, i, parts[i])
                                elif parts[i].upper() == "ORDER" and parts[i + 1].upper() == "BY":
                                    order_by = []
                                    print("Inside ORDER BY UP",
                                          order_by, i, parts[i])
                                    while i + 2 < len(parts) and parts[i + 2].upper() != "LIMIT":
                                        print(parts[i + 2], parts[i + 3])
                                        order_by.append(
                                            (parts[i + 2], parts[i + 3]))
                                        i += 3
                                        print("Inside ORDER BY WHILE",
                                              order_by, i, parts[i])
                                print("Outside WHERE", conditions,
                                      "Outside ORDER BY", order_by, i, parts[i])
                        if self.verbose:
                            logger.info(
                                f"SELECT * FROM: table_name={table_name}, conditions={conditions}, order_by={order_by}")
                        table_data = self.db.select_data(
                            table_name, conditions, order_by)
                        client_socket.sendall(json.dumps(table_data).encode())
                    elif request.startswith("DELETE TABLE"):
                        *_, table_name = request.split(" ", 2)
                        if self.verbose:
                            logger.info(
                                f"DELETE TABLE: table_name={table_name}")
                        self.db.delete_table(table_name)
                        client_socket.sendall(
                            f"Table '{table_name}' deleted successfully!".encode())
                    elif request.startswith("SHOW TABLES"):
                        if self.verbose:
                            logger.info("SHOW TABLES")
                        tables = os.listdir(DATA_DIRECTORY)
                        tables = [table.replace('.ayp', '')
                                  for table in tables]
                        client_socket.sendall(json.dumps(tables).encode())
                    else:
                        if self.verbose:
                            logger.warning("Invalid request!")
                        client_socket.sendall(b"Invalid request!")
                except Exception as e:
                    if self.verbose:
                        logger.error(f"Error: {e}")
                    client_socket.sendall(str(e).encode())
                    break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database server")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable verbose mode")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        db = Database(verbose=True)
    else:
        logger.setLevel(logging.INFO)
        db = Database()

    server = Server(db, "localhost", 12345, args.verbose)
    server.start()
