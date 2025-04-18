# load_balancer.py (renamed from metadata_server.py)
import grpc
from concurrent import futures
import sys
import os
import hashlib
import threading
import consul
import time

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc
import load_balancer_pb2
import load_balancer_pb2_grpc

class LoadBalancerService(load_balancer_pb2_grpc.CacheServiceServicer, metadata_cache_channel_pb2_grpc.MetadataServiceServicer):
    def __init__(self):
        self.primaries = {}  # cluster_id -> (server_id, stub, port)
        self.query_map = {}  # query_hash -> server_id
        self.query_counts = {}  # server_id -> number of queries processed
        self.lock = threading.Lock()

    def hash_query(self, query):
        return hashlib.sha256(query.encode()).hexdigest()

    # Load Balancer proto implementation
    def GetCachedData(self, request, context):
        """Implementation of the load_balancer proto service"""
        key = request.key
        query_hash = self.hash_query(key)
        
        print(f"LoadBalancer: Looking for cached data with key: {key}, hash: {query_hash}")
        
        if not self.primaries:
            print("LoadBalancer: No primary cache servers available")
            return load_balancer_pb2.CacheResponse(found=False)
            
        with self.lock:
            # Try to find which server has this cached data
            if query_hash in self.query_map:
                server_id = self.query_map[query_hash]
                # Find stub for that server
                stub = None
                for (sid, stub_candidate, _) in self.primaries.values():
                    if sid == server_id:
                        stub = stub_candidate
                        break

                if not stub:
                    print(f"LoadBalancer: Stub for CacheServer {server_id} not found")
                    return load_balancer_pb2.CacheResponse(found=False)

                print(f"LoadBalancer: Cache hit for hash '{query_hash}' on Primary CacheServer {server_id}")

                try:
                    # Use the metadata cache protocol to get the actual result
                    response = stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
                    if response.found:
                        self.query_counts[server_id] = self.query_counts.get(server_id, 0) + 1
                        print(f"LoadBalancer: Received result from Primary CacheServer {server_id}")
                        return load_balancer_pb2.CacheResponse(found=True, value=response.result)
                except grpc.RpcError as e:
                    print(f"LoadBalancer: Failed to contact Primary CacheServer {server_id}: {e}")
                    
            # Cache miss
            print("LoadBalancer: Cache miss")
            return load_balancer_pb2.CacheResponse(found=False)

    def SetCachedData(self, request, context):
        """Implementation of the load_balancer proto service"""
        key = request.key
        value = request.value
        query_hash = self.hash_query(key)
        
        print(f"LoadBalancer: Setting cached data for key: {key}, hash: {query_hash}")
        
        if not self.primaries:
            print("LoadBalancer: No primary cache servers available")
            return load_balancer_pb2.CacheSetResponse(success=False)
            
        with self.lock:
            # Choose the server with the least load
            primary_loads = []
            for (server_id, stub, _) in self.primaries.values():
                try:
                    response = stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest())
                    primary_loads.append((server_id, response.load, stub))
                except grpc.RpcError as e:
                    print(f"LoadBalancer: Failed to get load from server {server_id}: {e}")

            if not primary_loads:
                print("LoadBalancer: No active primary cache servers available")
                return load_balancer_pb2.CacheSetResponse(success=False)
                
            # Choose the server with the least load
            least_loaded_id, _, stub = min(primary_loads, key=lambda x: x[1])
            
            try:
                stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=value))
                self.query_map[query_hash] = least_loaded_id
                self.query_counts[least_loaded_id] = self.query_counts.get(least_loaded_id, 0) + 1
                print(f"LoadBalancer: Stored result in Primary CacheServer {least_loaded_id}")
                return load_balancer_pb2.CacheSetResponse(success=True)
            except grpc.RpcError as e:
                print(f"LoadBalancer: Failed to store result in Primary CacheServer {least_loaded_id}: {e}")
                return load_balancer_pb2.CacheSetResponse(success=False)

    # Original MetadataServer proto implementation (keep these for compatibility)
    def RegisterServer(self, request, context):
        server_id = request.server_id
        port = request.port
        cluster_id = request.cluster_id
        print(f"LoadBalancer: Attempting to register CacheServer {server_id} (cluster {cluster_id}) on port {port}")

        try:
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(grpc.insecure_channel(f'localhost:{port}'))
            role_response = stub.GetRole(metadata_cache_channel_pb2.EmptyRequest())
            if role_response.is_primary:
                with self.lock:
                    self.primaries[cluster_id] = (server_id, stub, port)
                    self.query_counts[server_id] = 0
                print(f"LoadBalancer: Registered PRIMARY CacheServer {server_id} in cluster {cluster_id} on port {port}")
                return metadata_cache_channel_pb2.RegisterResponse(success=True)
            else:
                print(f"LoadBalancer: Ignored replica CacheServer {server_id} (not primary)")
                return metadata_cache_channel_pb2.RegisterResponse(success=False)
        except grpc.RpcError as e:
            print(f"LoadBalancer: Could not connect to CacheServer {server_id}: {e}")
            return metadata_cache_channel_pb2.RegisterResponse(success=False)

    def ProcessQuery(self, request, context):
        query = request.query
        query_hash = self.hash_query(query)
        print(f"LoadBalancer: Processing query '{query}' -> Hash: {query_hash}")

        if not self.primaries:
            print("LoadBalancer: No primary cache servers available")
            return metadata_cache_channel_pb2.QueryResponse(reply="No primary cache server available")

        with self.lock:
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
                    print(f"LoadBalancer: Stub for CacheServer {server_id} not found")
                    return metadata_cache_channel_pb2.QueryResponse(reply="Cache server unavailable")

                print(f"LoadBalancer: Cache hit for hash '{query_hash}' on Primary CacheServer {server_id}")

                try:
                    response = stub.GetResult(metadata_cache_channel_pb2.CacheRequest(query_hash=query_hash))
                    if response.found:
                        self.query_counts[server_id] = self.query_counts.get(server_id, 0) + 1
                        print(f"LoadBalancer: Received result from Primary CacheServer {server_id}")
                        return metadata_cache_channel_pb2.QueryResponse(reply=response.result)
                    else:
                        print(f"LoadBalancer: Cache miss on mapped server")
                        return metadata_cache_channel_pb2.QueryResponse(reply="Cache miss on mapped server")
                except grpc.RpcError as e:
                    print(f"LoadBalancer: Failed to contact Primary CacheServer {server_id}: {e}")
                    return metadata_cache_channel_pb2.QueryResponse(reply="Failed to contact primary cache server")

            # Case 2: Cache miss
            return metadata_cache_channel_pb2.QueryResponse(reply="Data not in cache!")

    def NotifyPromotion(self, request, context):
        promoted_server_id = request.server_id
        promoted_server_port = request.port
        cluster_id = request.cluster_id
        print(f"LoadBalancer: Received promotion notification for CacheServer {promoted_server_id} (cluster {cluster_id}) on port {promoted_server_port}")

        try:
            # Connect to the new primary to verify it's available
            channel = grpc.insecure_channel(f'localhost:{promoted_server_port}')
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
            # Verify role and connectivity
            role_response = stub.GetRole(metadata_cache_channel_pb2.EmptyRequest(), timeout=2)
            
            if not role_response.is_primary:
                print(f"LoadBalancer: Warning - Server {promoted_server_id} doesn't think it's primary yet")
                # We'll still update our records since Sentinel says it's primary
                
            with self.lock:
                # Remove the old primary for the same cluster if any
                old_primary_id = None
                if cluster_id in self.primaries:
                    old_primary_id, _, _ = self.primaries[cluster_id]
                    if old_primary_id in self.query_counts:
                        del self.query_counts[old_primary_id]
                    print(f"LoadBalancer: Removed old PRIMARY {old_primary_id} for cluster {cluster_id}")
                
                # Set new primary for the cluster
                self.primaries[cluster_id] = (promoted_server_id, stub, promoted_server_port)
                self.query_counts[promoted_server_id] = 0
                print(f"LoadBalancer: Set CacheServer {promoted_server_id} as PRIMARY for cluster {cluster_id}")
                
                # Update the query_map
                if old_primary_id:
                    remapped = 0
                    for query_hash, server_id in list(self.query_map.items()):
                        if server_id == old_primary_id:
                            self.query_map[query_hash] = promoted_server_id
                            remapped += 1
                    print(f"LoadBalancer: Remapped {remapped} queries from old primary {old_primary_id} to new primary {promoted_server_id}")
                
            return metadata_cache_channel_pb2.NotifyPromotionResponse(success=True)
        
        except Exception as e:
            print(f"LoadBalancer: Failed to connect to new primary: {e}")
            return metadata_cache_channel_pb2.NotifyPromotionResponse(success=False)

def register_with_consul(port):
    """Register this load balancer with Consul"""
    try:
        c = consul.Consul()
        service_id = f"load-balancer-{int(time.time())}"
        c.agent.service.register(
            name="load-balancer",
            service_id=service_id,
            address="127.0.0.1",
            port=port,
            tags=["cache", "loadbalancer"]
        )
        print(f"LoadBalancer registered with Consul as {service_id}")
    except Exception as e:
        print(f"Failed to register with Consul: {e}")

def serve_load_balancer():
    port = 50053  # Set a consistent port for the load balancer
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = LoadBalancerService()
    
    # Register both service interfaces
    metadata_cache_channel_pb2_grpc.add_MetadataServiceServicer_to_server(service, server)
    load_balancer_pb2_grpc.add_CacheServiceServicer_to_server(service, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    # Register with Consul
    register_with_consul(port)
    
    print(f"LoadBalancer started on port {port}")
    server.wait_for_termination()

if __name__ == "__main__":
    serve_load_balancer()