# backend_server.py
import sys
from os import path

sys.path.append(path.abspath(path.join(path.dirname(__file__), '../grpc')))

import grpc
from concurrent import futures
import time
import json
import threading
from mysql.connector import Error, pooling
import mysql.connector
import backend_pb2
import backend_pb2_grpc
import heartbeat_pb2
import heartbeat_pb2_grpc

MYSQL_CONFIG = {
    'pool_name': 'mypool',
    'pool_reset_session': True,
    'pool_size': 20,
    'host': 'localhost',
    'user': 'cacheuser',
    'password': 'yourpassword',
    'database': 'university'
}

# Unique identifier for this backend instance
BACKEND_ID = f"backend-{int(time.time())}"

class BackendService(backend_pb2_grpc.BackendServiceServicer):
    def __init__(self):
        try:
            # Create a connection pool with a higher pool size for heavy concurrency.
            self.connection_pool = mysql.connector.pooling.MySQLConnectionPool(**MYSQL_CONFIG)
            print("Connection pool established with size 20")
        except Error as err:
            print(f"Error establishing connection pool: {err}")
            self.connection_pool = None

    def ExecuteSQL(self, request, context):
        if not self.connection_pool:
            error_msg = "No connection pool available."
            return backend_pb2.BackendResponse(result=json.dumps({"error": error_msg}))

        for attempt in range(3):
            try:
                connection = self.connection_pool.get_connection()
                break
            except mysql.connector.PoolError:
                time.sleep(0.1 * 2**attempt)
        else:
            # after 3 failed attempts
            return backend_pb2.BackendResponse(
                result=json.dumps({"error": "Database connections exhausted"})
            )

        # Parse the SQL query and parameters.
        sql_query = request.sql_query
        try:
            params_dict = json.loads(request.params)
            params = params_dict.get("params", ())
        except Exception as e:
            print("Error parsing parameters:", e)
            params = ()

        print(f"Backend received SQL: {sql_query} with params: {params}")

        try:
            # Create a cursor from the connection.
            cursor = connection.cursor(dictionary=True)
            cursor.execute(sql_query, params)

            # For SELECT queries, fetch all rows; for others, commit the transaction.
            if sql_query.strip().lower().startswith("select"):
                result = cursor.fetchall()
            else:
                connection.commit()
                result = {"affected_rows": cursor.rowcount}

            cursor.close()
        except Error as err:
            error_msg = f"Database error: {err}"
            print(error_msg)
            return backend_pb2.BackendResponse(result=json.dumps({"error": error_msg}))
        finally:
            # Always close the connection to return it to the pool.
            connection.close()

        return backend_pb2.BackendResponse(result=json.dumps(result))

class HeartbeatService(heartbeat_pb2_grpc.HeartbeatServiceServicer):
    def CheckHealth(self, request, context):
        """
        Handle health check requests from the gateway
        """
        # Log the health check (but not too frequently to avoid log spam)
        if int(time.time()) % 10 == 0:  # Log only once every ~10 seconds
            print(f"Received health check from gateway {request.gateway_id}")
        
        # Simply respond that the backend is alive
        return heartbeat_pb2.HeartbeatResponse(received=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    backend_pb2_grpc.add_BackendServiceServicer_to_server(BackendService(), server)
    heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(HeartbeatService(), server)
    
    server.add_insecure_port('[::]:50055')
    
    print(f"Backend server {BACKEND_ID} listening on port 50055")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()