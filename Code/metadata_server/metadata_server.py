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

    def ProcessQuery(self, request, context):
        query = request.query
        query_hash = self.hash_query(query)
        print(f"MetadataServer: Processing query '{query}' -> Hash: {query_hash}")

        if not self.primaries:
            print("MetadataServer: No primary cache servers available")
            return metadata_cache_channel_pb2.QueryResponse(reply="No primary cache server available")

        with self.lock:
            print("Inside lock")
            print("Current primaries in MetadataServer:")
            for cluster_id, (server_id, _, port) in self.primaries.items():
                print(f"  Cluster {cluster_id} -> Primary Server {server_id} (Port {port})")

            # Case 1: Cache hit
            if query_hash in self.query_map:
                server_id = self.query_map[query_hash]
                # Find stub for that server
                stub = None
                for (sid, stub_candidate, _) in self.primaries.values():
                    if sid == server_id:
                        stub = stub_candidate
                        break

                if not stub:
                    print(f"MetadataServer: Stub for CacheServer {server_id} not found")
                    return metadata_cache_channel_pb2.QueryResponse(reply="Cache server unavailable")

                print(f"MetadataServer: Cache hit for hash '{query_hash}' on Primary CacheServer {server_id}")

                try:
                    response = stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
                    if response.found:
                        self.query_counts[server_id] = self.query_counts.get(server_id, 0) + 1
                        print(f"MetadataServer: Received result '{response.result}' from Primary CacheServer {server_id}")
                        return metadata_cache_channel_pb2.QueryResponse(reply=response.result)
                    else:
                        print(f"MetadataServer: Cache miss on mapped server")
                        return metadata_cache_channel_pb2.QueryResponse(reply="Cache miss on mapped server")
                except grpc.RpcError as e:
                    print(f"MetadataServer: Failed to contact Primary CacheServer {server_id}: {e}")
                    return metadata_cache_channel_pb2.QueryResponse(reply="Failed to contact primary cache server")

            # Case 2: Cache miss
            else:
                print("came inside else statement!")
                # Dynamically query load from all primaries
                primary_loads = []
                for (server_id, stub, _) in self.primaries.values():
                    print(f"came inside the for loop for server id" , server_id)
                    try:
                        response = stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest())
                        print(f"got the load : {response.load}")
                        primary_loads.append((server_id, response.load, stub))
                    except grpc.RpcError as e:
                        print("failed to load!!")
                        print(f"MetadataServer: Failed to get load from server {server_id}: {e}")

                if not primary_loads:
                    print("MetadataServer: No active primary cache servers available")
                    return metadata_cache_channel_pb2.QueryResponse(reply="No active primary available")
                print("All collected primary loads:")
                for sid, load, _ in primary_loads:
                    print(f"Server ID {sid} -> Load: {load}")
                # Choose the server with the least load
                least_loaded_id, _, stub = min(primary_loads, key=lambda x: x[1])
                result = self.query_file(query)
                print(f"MetadataServer: Queried file, got result '{result}'")

                try:
                    stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
                    self.query_map[query_hash] = least_loaded_id
                    self.query_counts[least_loaded_id] = self.query_counts.get(least_loaded_id, 0) + 1
                    print(f"MetadataServer: Stored result in Primary CacheServer {least_loaded_id} and updated query_map")
                    print(f"MetadataServer: Query counts: {self.query_counts}")
                    return metadata_cache_channel_pb2.QueryResponse(reply=result)
                except grpc.RpcError as e:
                    print(f"MetadataServer: Failed to store result in Primary CacheServer {least_loaded_id}: {e}")
                    return metadata_cache_channel_pb2.QueryResponse(reply="Failed to store in cache")


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
