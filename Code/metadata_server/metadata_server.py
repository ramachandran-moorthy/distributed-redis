# import grpc
# from concurrent import futures
# import sys
# import os
# import hashlib

# # Add the grpc folder to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

# import metadata_cache_channel_pb2
# import metadata_cache_channel_pb2_grpc

# class MetadataServer(metadata_cache_channel_pb2_grpc.MetadataServiceServicer):
#     def __init__(self):
#         self.cache_stubs = {}  # server_id -> stub
#         self.cache_server_ports = {}  # server_id -> port
#         self.query_map = {}  # hash -> server_id
#         self.server_loads = {}  # server_id -> number of keys
#         self.query_counts = {}  # server_id -> number of queries processed

#     def hash_query(self, query):
#         return hashlib.sha256(query.encode()).hexdigest()

#     def get_least_loaded_server(self):
#         if not self.server_loads:
#             print("MetadataServer: No cache servers registered yet")
#             return None
#         min_load = float('inf')
#         min_server_id = 0
#         for server_id, load in self.server_loads.items():
#             if load < min_load:
#                 min_load = load
#                 min_server_id = server_id
#         print(f"MetadataServer: Selected Server {min_server_id} with load {min_load}")
#         return min_server_id

#     def query_file(self, query):
#         try:
#             with open('cache_data.txt', 'r') as f:
#                 for line in f:
#                     key, value = line.strip().split(':', 1)
#                     if key == query:
#                         return value
#             return "no data"
#         except FileNotFoundError:
#             print("MetadataServer: cache_data.txt not found")
#             return "file not found"
#         except Exception as e:
#             print(f"MetadataServer: File error: {e}")
#             return "file error"

#     def RegisterServer(self, request, context):
#         server_id = request.server_id
#         port = request.port
#         self.cache_server_ports[server_id] = port
#         self.cache_stubs[server_id] = metadata_cache_channel_pb2_grpc.CacheServiceStub(
#             grpc.insecure_channel(f'localhost:{port}')
#         )
#         self.server_loads[server_id] = 0
#         self.query_counts[server_id] = 0
#         print(f"MetadataServer: Registered CacheServer {server_id} on port {port}")
#         print(f"MetadataServer: Current servers: {self.cache_server_ports}")
#         return metadata_cache_channel_pb2.RegisterResponse(success=True)

#     def ProcessQuery(self, request, context):
#         query = request.query
#         query_hash = self.hash_query(query)
#         print(f"MetadataServer: Processing query '{query}' -> Hash: {query_hash}")

#         if query_hash in self.query_map:
#             server_id = self.query_map[query_hash]
#             stub = self.cache_stubs.get(server_id)
#             if not stub:
#                 print(f"MetadataServer: No cache server found with ID {server_id}")
#                 return metadata_cache_channel_pb2.QueryResponse(reply=f"No cache server found with ID {server_id}")
            
#             try:
#                 print(f"MetadataServer: Sending query hash to CacheServer {server_id} (port {self.cache_server_ports[server_id]})")
#                 response = stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
#                 self.query_counts[server_id] += 1  # Increment query count
#                 if response.found:
#                     print(f"MetadataServer: Received result '{response.result}' from CacheServer {server_id}")
#                     return metadata_cache_channel_pb2.QueryResponse(reply=response.result)
#                 else:
#                     print(f"MetadataServer: Cache miss on CacheServer {server_id}")
#             except grpc.RpcError as e:
#                 print(f"MetadataServer: Failed to contact CacheServer {server_id}: {e}")
#                 return metadata_cache_channel_pb2.QueryResponse(reply=f"Failed to contact cache server {server_id}")
#         else:
#             print(f"MetadataServer: Query hash {query_hash} not found in query_map")

#         # Handle cache miss: query file and store result
#         result = self.query_file(query)
#         print(f"MetadataServer: Queried file, got result '{result}'")
        
#         # Assign to least loaded server
#         server_id = self.get_least_loaded_server()
#         if server_id is None:
#             return metadata_cache_channel_pb2.QueryResponse(reply="No cache servers available")
#         stub = self.cache_stubs[server_id]
#         try:
#             stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
#             self.query_map[query_hash] = server_id
#             self.server_loads[server_id] += 1  # Increment load
#             self.query_counts[server_id] += 1  # Increment query count
#             print(f"MetadataServer: Stored result in CacheServer {server_id} and updated query_map")
#             print(f"MetadataServer: Server loads: {self.server_loads}")
#             print(f"MetadataServer: Query counts: {self.query_counts}")
#             return metadata_cache_channel_pb2.QueryResponse(reply=result)
#         except grpc.RpcError as e:
#             print(f"MetadataServer: Failed to store result in CacheServer {server_id}: {e}")
#             return metadata_cache_channel_pb2.QueryResponse(reply="Failed to store in cache")

# def serve_metadata():
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     metadata_cache_channel_pb2_grpc.add_MetadataServiceServicer_to_server(MetadataServer(), server)
#     server.add_insecure_port('[::]:50050')
#     server.start()
#     print("MetadataServer started on port 50050")
#     server.wait_for_termination()

# if __name__ == "__main__":
#     serve_metadata()

# import grpc
# from concurrent import futures
# import sys
# import os
# import hashlib

# # Add the grpc folder to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

# import metadata_cache_channel_pb2
# import metadata_cache_channel_pb2_grpc

# class MetadataServer(metadata_cache_channel_pb2_grpc.MetadataServiceServicer):
#     def __init__(self):
#         self.primary_server_id = None
#         self.primary_stub = None
#         self.primary_port = None
#         self.query_map = {}  # hash -> server_id
#         self.query_counts = {}  # server_id -> number of queries processed

#     def hash_query(self, query):
#         return hashlib.sha256(query.encode()).hexdigest()

#     def query_file(self, query):
#         try:
#             with open('cache_data.txt', 'r') as f:
#                 for line in f:
#                     key, value = line.strip().split(':', 1)
#                     if key == query:
#                         return value
#             return "no data"
#         except FileNotFoundError:
#             print("MetadataServer: cache_data.txt not found")
#             return "file not found"
#         except Exception as e:
#             print(f"MetadataServer: File error: {e}")
#             return "file error"

#     def RegisterServer(self, request, context):
#         server_id = request.server_id
#         port = request.port

#         # Register this server as the primary
#         self.primary_server_id = server_id
#         self.primary_port = port
#         self.primary_stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(
#             grpc.insecure_channel(f'localhost:{port}')
#         )
#         self.query_counts[server_id] = 0

#         print(f"MetadataServer: Registered Primary CacheServer {server_id} on port {port}")
#         return metadata_cache_channel_pb2.RegisterResponse(success=True)

#     def ProcessQuery(self, request, context):
#         query = request.query
#         query_hash = self.hash_query(query)
#         print(f"MetadataServer: Processing query '{query}' -> Hash: {query_hash}")

#         if self.primary_stub is None:
#             print("MetadataServer: No primary cache server registered")
#             return metadata_cache_channel_pb2.QueryResponse(reply="No primary cache server available")

#         try:
#             response = self.primary_stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
#             self.query_counts[self.primary_server_id] += 1  # Increment query count
#             if response.found:
#                 print(f"MetadataServer: Received result '{response.result}' from Primary CacheServer {self.primary_server_id}")
#                 return metadata_cache_channel_pb2.QueryResponse(reply=response.result)
#             else:
#                 print(f"MetadataServer: Cache miss on Primary CacheServer {self.primary_server_id}")
#         except grpc.RpcError as e:
#             print(f"MetadataServer: Failed to contact Primary CacheServer {self.primary_server_id}: {e}")
#             return metadata_cache_channel_pb2.QueryResponse(reply=f"Failed to contact primary cache server")

#         # Handle cache miss: query file and store result
#         result = self.query_file(query)
#         print(f"MetadataServer: Queried file, got result '{result}'")

#         try:
#             self.primary_stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
#             self.query_map[query_hash] = self.primary_server_id
#             self.query_counts[self.primary_server_id] += 1  # Increment query count
#             print(f"MetadataServer: Stored result in Primary CacheServer {self.primary_server_id} and updated query_map")
#             print(f"MetadataServer: Query counts: {self.query_counts}")
#             return metadata_cache_channel_pb2.QueryResponse(reply=result)
#         except grpc.RpcError as e:
#             print(f"MetadataServer: Failed to store result in Primary CacheServer {self.primary_server_id}: {e}")
#             return metadata_cache_channel_pb2.QueryResponse(reply="Failed to store in cache")

# def serve_metadata():
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     metadata_cache_channel_pb2_grpc.add_MetadataServiceServicer_to_server(MetadataServer(), server)
#     server.add_insecure_port('[::]:50050')
#     server.start()
#     print("MetadataServer started on port 50050")
#     server.wait_for_termination()

# if __name__ == "__main__":
#     serve_metadata()


import grpc
from concurrent import futures
import sys
import os
import hashlib
import threading

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc

class MetadataServer(metadata_cache_channel_pb2_grpc.MetadataServiceServicer):
    def __init__(self):
        self.primaries = {}  # cluster_id -> (server_id, stub, port)
        self.query_map = {}  # query_hash -> server_id
        self.query_counts = {}  # server_id -> number of queries processed
        self.lock = threading.Lock()

    def hash_query(self, query):
        return hashlib.sha256(query.encode()).hexdigest()

    def query_file(self, query):
        try:
            with open('cache_data.txt', 'r') as f:
                for line in f:
                    key, value = line.strip().split(':', 1)
                    if key == query:
                        return value
            return "no data"
        except FileNotFoundError:
            print("MetadataServer: cache_data.txt not found")
            return "file not found"
        except Exception as e:
            print(f"MetadataServer: File error: {e}")
            return "file error"

    def RegisterServer(self, request, context):
        server_id = request.server_id
        port = request.port
        cluster_id = request.cluster_id
        print(f"MetadataServer: Attempting to register CacheServer {server_id} (cluster {cluster_id}) on port {port}")

        try:
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(grpc.insecure_channel(f'localhost:{port}'))
            role_response = stub.GetRole(metadata_cache_channel_pb2.EmptyRequest())
            if role_response.is_primary:
                with self.lock:
                    self.primaries[cluster_id] = (server_id, stub, port)
                    self.query_counts[server_id] = 0
                print(f"MetadataServer: Registered PRIMARY CacheServer {server_id} in cluster {cluster_id} on port {port}")
                return metadata_cache_channel_pb2.RegisterResponse(success=True)
            else:
                print(f"MetadataServer: Ignored replica CacheServer {server_id} (not primary)")
                return metadata_cache_channel_pb2.RegisterResponse(success=False)
        except grpc.RpcError as e:
            print(f"MetadataServer: Could not connect to CacheServer {server_id}: {e}")
            return metadata_cache_channel_pb2.RegisterResponse(success=False)

    # def ProcessQuery(self, request, context):
    #     query = request.query
    #     query_hash = self.hash_query(query)
    #     print(f"MetadataServer: Processing query '{query}' -> Hash: {query_hash}")

    #     if not self.primaries:
    #         print("MetadataServer: No primary cache servers available")
    #         return metadata_cache_channel_pb2.QueryResponse(reply="No primary cache server available")

    #     with self.lock:
    #         # Choose the least loaded server overall (or by some policy per cluster if needed)
    #         all_servers = [(server_id, self.query_counts.get(server_id, float('inf')), stub) 
    #                        for (_, (server_id, stub, _)) in self.primaries.items()]

    #         if not all_servers:
    #             print("MetadataServer: No active primary cache servers available")
    #             return metadata_cache_channel_pb2.QueryResponse(reply="No active primary available")

    #         least_loaded_id, _, stub = min(all_servers, key=lambda x: x[1])

    #     try:
    #         response = stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
    #         self.query_counts[least_loaded_id] += 1
    #         if response.found:
    #             print(f"MetadataServer: Received result '{response.result}' from Primary CacheServer {least_loaded_id}")
    #             return metadata_cache_channel_pb2.QueryResponse(reply=response.result)
    #         else:
    #             print(f"MetadataServer: Cache miss on Primary CacheServer {least_loaded_id}")
    #     except grpc.RpcError as e:
    #         print(f"MetadataServer: Failed to contact Primary CacheServer {least_loaded_id}: {e}")
    #         return metadata_cache_channel_pb2.QueryResponse(reply="Failed to contact primary cache server")

    #     result = self.query_file(query)
    #     print(f"MetadataServer: Queried file, got result '{result}'")

    #     try:
    #         stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
    #         self.query_map[query_hash] = least_loaded_id
    #         self.query_counts[least_loaded_id] += 1
    #         print(f"MetadataServer: Stored result in Primary CacheServer {least_loaded_id} and updated query_map")
    #         print(f"MetadataServer: Query counts: {self.query_counts}")
    #         return metadata_cache_channel_pb2.QueryResponse(reply=result)
    #     except grpc.RpcError as e:
    #         print(f"MetadataServer: Failed to store result in Primary CacheServer {least_loaded_id}: {e}")
    #         return metadata_cache_channel_pb2.QueryResponse(reply="Failed to store in cache")
    def ProcessQuery(self, request, context):
        query = request.query
        query_hash = self.hash_query(query)
        print(f"MetadataServer: Processing query '{query}' -> Hash: {query_hash}")

        if not self.primaries:
            print("MetadataServer: No primary cache servers available")
            return metadata_cache_channel_pb2.QueryResponse(reply="No primary cache server available")

        # Check query_map for cache hit
        with self.lock:
            if query_hash in self.query_map:
                server_id = self.query_map[query_hash]
                stub = self.primaries.get(server_id, [None, None])[1]
                if not stub:
                    print(f"MetadataServer: CacheServer {server_id} not available")
                    return metadata_cache_channel_pb2.QueryResponse(reply="Cache server unavailable")
                print(f"MetadataServer: Cache hit for hash '{query_hash}' on Primary CacheServer {server_id}")
            else:
                # Select least loaded server for potential miss
                all_servers = [(server_id, self.query_counts.get(server_id, float('inf')), stub)
                            for (_, (server_id, stub, _)) in self.primaries.items()]
                if not all_servers:
                    print("MetadataServer: No active primary cache servers available")
                    return metadata_cache_channel_pb2.QueryResponse(reply="No active primary available")
                server_id, _, stub = min(all_servers, key=lambda x: x[1])
                print(f"MetadataServer: No cache hit, trying Primary CacheServer {server_id} (least loaded)")

        # Call GetResult
        try:
            response = stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
            with self.lock:
                self.query_counts[server_id] = self.query_counts.get(server_id, 0) + 1
            if response.found:
                print(f"MetadataServer: Received result '{response.result}' from Primary CacheServer {server_id}")
                return metadata_cache_channel_pb2.QueryResponse(reply=response.result)
            else:
                print(f"MetadataServer: Cache miss on Primary CacheServer {server_id}")
                return metadata_cache_channel_pb2.QueryResponse(reply="cache miss")
        except grpc.RpcError as e:
            print(f"MetadataServer: Failed to contact Primary CacheServer {server_id}: {e}")
            return metadata_cache_channel_pb2.QueryResponse(reply="Failed to contact primary cache server")

    def NotifyPromotion(self, request, context):
        promoted_server_id = request.server_id
        promoted_server_port = request.port
        cluster_id = request.cluster_id
        print(f"MetadataServer: Received promotion notification for CacheServer {promoted_server_id} (cluster {cluster_id}) on port {promoted_server_port}")

        with self.lock:
            # Remove the old primary for the same cluster if any
            if cluster_id in self.primaries:
                old_primary_id, _, _ = self.primaries[cluster_id]
                if old_primary_id in self.query_counts:
                    del self.query_counts[old_primary_id]
                print(f"MetadataServer: Removed old PRIMARY {old_primary_id} for cluster {cluster_id}")

            # Set new primary for the cluster
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(grpc.insecure_channel(f'localhost:{promoted_server_port}'))
            self.primaries[cluster_id] = (promoted_server_id, stub, promoted_server_port)
            self.query_counts[promoted_server_id] = 0
            print(f"MetadataServer: Set CacheServer {promoted_server_id} as PRIMARY for cluster {cluster_id}")

        return metadata_cache_channel_pb2.NotifyPromotionResponse(success=True)

def serve_metadata():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    metadata_cache_channel_pb2_grpc.add_MetadataServiceServicer_to_server(MetadataServer(), server)
    server.add_insecure_port('[::]:50050')
    server.start()
    print("MetadataServer started on port 50050")
    server.wait_for_termination()

if __name__ == "__main__":
    serve_metadata()
