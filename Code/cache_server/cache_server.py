# import grpc
# from concurrent import futures
# import sys
# import os
# import threading

# # Add the grpc folder to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

# import metadata_cache_channel_pb2
# import metadata_cache_channel_pb2_grpc

# class CacheServer(metadata_cache_channel_pb2_grpc.CacheServiceServicer):
#     def __init__(self, server_id):
#         self.server_id = server_id
#         self.cache = {}  # hash -> result
#         self.is_primary = is_primary

#     def GetResult(self, request, context):
#         query_hash = request.query_hash
#         print(f"CacheServer {self.server_id}: Received query hash '{query_hash}'")
#         result = self.cache.get(query_hash, "")
#         found = query_hash in self.cache
#         print(f"CacheServer {self.server_id}: {'Found' if found else 'Missed'} result for hash '{query_hash}'")
#         return metadata_cache_channel_pb2.CacheResponse(result=result, found=found)

#     def SetResult(self, request, context):
#         query_hash = request.query_hash
#         result = request.result
#         self.cache[query_hash] = result
#         print(f"CacheServer {self.server_id}: Stored result '{result}' for hash '{query_hash}'")
#         return metadata_cache_channel_pb2.CacheSetResponse(success=True)

#     def GetLoad(self, request, context):
#         load = len(self.cache)
#         print(f"CacheServer {self.server_id}: Reported load {load}")
#         return metadata_cache_channel_pb2.LoadResponse(load=load)
    
#     def GetRole(self, request, context):
#         return metadata_cache_channel_pb2.RoleResponse(is_primary=self.is_primary)

#     def PromoteToPrimary(self, request, context):
#         with self.lock:
#             self.is_primary = True
#         print(f"CacheServer {self.server_id}: Promoted to PRIMARY")
#         return metadata_cache_channel_pb2.PromotionResponse(success=True)

# def register_with_metadata(server_id, port):
#     try:
#         with grpc.insecure_channel('localhost:50050') as channel:
#             stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
#             response = stub.RegisterServer(metadata_cache_channel_pb2.RegisterRequest(server_id=server_id, port=port))
#             if response.success:
#                 print(f"CacheServer {server_id}: Successfully registered with MetadataServer on port {port}")
#             else:
#                 print(f"CacheServer {server_id}: Failed to register with MetadataServer")
#     except grpc.RpcError as e:
#         print(f"CacheServer {server_id}: Failed to connect to MetadataServer: {e}")

# def serve(server_id, port):
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(CacheServer(server_id), server)
#     server.add_insecure_port(f'[::]:{port}')
#     server.start()
#     print(f"CacheServer {server_id} started on port {port}")
#     # Register with metadata server after starting
#     register_with_metadata(server_id, port)
#     server.wait_for_termination()

# if __name__ == "__main__":
#     server_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
#     port = 50051 + server_id
#     serve(server_id, port)

# import grpc
# from concurrent import futures
# import sys
# import os
# import threading

# # Add the grpc folder to the Python path
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

# import metadata_cache_channel_pb2
# import metadata_cache_channel_pb2_grpc

# class CacheServer(metadata_cache_channel_pb2_grpc.CacheServiceServicer):
#     def __init__(self, server_id, is_primary=False):
#         self.server_id = server_id
#         self.cache = {}  # hash -> result
#         self.is_primary = is_primary
#         self.lock = threading.Lock()

#     def GetResult(self, request, context):
#         query_hash = request.query_hash
#         print(f"CacheServer {self.server_id}: Received query hash '{query_hash}'")
#         result = self.cache.get(query_hash, "")
#         found = query_hash in self.cache
#         print(f"CacheServer {self.server_id}: {'Found' if found else 'Missed'} result for hash '{query_hash}'")
#         return metadata_cache_channel_pb2.CacheResponse(result=result, found=found)

#     def SetResult(self, request, context):
#         query_hash = request.query_hash
#         result = request.result
#         self.cache[query_hash] = result
#         print(f"CacheServer {self.server_id}: Stored result '{result}' for hash '{query_hash}'")
#         return metadata_cache_channel_pb2.CacheSetResponse(success=True)

#     def GetLoad(self, request, context):
#         load = len(self.cache)
#         print(f"CacheServer {self.server_id}: Reported load {load}")
#         return metadata_cache_channel_pb2.LoadResponse(load=load)

#     def GetRole(self, request, context):
#         print(f"CacheServer {self.server_id}: Role check -> {'PRIMARY' if self.is_primary else 'REPLICA'}")
#         return metadata_cache_channel_pb2.RoleResponse(is_primary=self.is_primary)

#     def PromoteToPrimary(self, request, context):
#         with self.lock:
#             self.is_primary = True
#         print(f"CacheServer {self.server_id}: Promoted to PRIMARY")
#         return metadata_cache_channel_pb2.PromotionResponse(success=True)

# def register_with_metadata(server_id, port):
#     try:
#         with grpc.insecure_channel('localhost:50050') as channel:
#             stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
#             response = stub.RegisterServer(metadata_cache_channel_pb2.RegisterRequest(server_id=server_id, port=port))
#             if response.success:
#                 print(f"CacheServer {server_id}: Successfully registered with MetadataServer on port {port}")
#             else:
#                 print(f"CacheServer {server_id}: Failed to register with MetadataServer")
#     except grpc.RpcError as e:
#         print(f"CacheServer {server_id}: Failed to connect to MetadataServer: {e}")

# def serve(server_id, port, is_primary):
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
#     metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(
#         CacheServer(server_id, is_primary), server
#     )
#     server.add_insecure_port(f'[::]:{port}')
#     server.start()
#     print(f"CacheServer {server_id} started on port {port} as {'PRIMARY' if is_primary else 'REPLICA'}")
#     register_with_metadata(server_id, port)
#     server.wait_for_termination()

# if __name__ == "__main__":
#     # Example usage: python cache_server.py 1 1 (means server_id=1, is_primary=True)
#     server_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
#     is_primary_flag = int(sys.argv[2]) if len(sys.argv) > 2 else 0
#     is_primary = bool(is_primary_flag)
#     port = 50051 + server_id
#     serve(server_id, port, is_primary)


import grpc
from concurrent import futures
import sys
import os
import threading

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc

class CacheServer(metadata_cache_channel_pb2_grpc.CacheServiceServicer):
    def __init__(self, server_id, is_primary=False, cluster_id=None):
        self.server_id = server_id
        self.cache = {}  # hash -> result
        self.is_primary = is_primary
        self.lock = threading.Lock()
        self.replica_stubs = [] 
        self.cluster_id = cluster_id

    def GetResult(self, request, context):
        query_hash = request.query_hash
        print(f"CacheServer {self.server_id}: Received query hash '{query_hash}'")
        result = self.cache.get(query_hash, "")
        found = query_hash in self.cache
        print(f"CacheServer {self.server_id}: {'Found' if found else 'Missed'} result for hash '{query_hash}'")
        return metadata_cache_channel_pb2.CacheResponse(result=result, found=found)

    def SetResult(self, request, context):
        query_hash = request.query_hash
        result = request.result
        self.cache[query_hash] = result
        print(f"CacheServer {self.server_id}: Stored result '{result}' for hash '{query_hash}'")

        if self.is_primary:
            for stub in self.replica_stubs:
                try:
                    stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
                    print(f"CacheServer {self.server_id}: Synced result to a replica")
                except grpc.RpcError as e:
                    print(f"CacheServer {self.server_id}: Failed to sync with replica: {e}")

        return metadata_cache_channel_pb2.CacheSetResponse(success=True)

    def GetLoad(self, request, context):
        load = len(self.cache)
        print(f"CacheServer {self.server_id}: Reported load {load}")
        return metadata_cache_channel_pb2.LoadResponse(load=load)

    def GetRole(self, request, context):
        print(f"CacheServer {self.server_id}: Role check -> {'PRIMARY' if self.is_primary else 'REPLICA'}")
        return metadata_cache_channel_pb2.RoleResponse(is_primary=self.is_primary)

    def GetClusterId(self, request, context):
        return metadata_cache_channel_pb2.ClusterIdResponse(cluster_id=self.cluster_id)

    def PromoteToPrimary(self, request, context):
        with self.lock:
            self.is_primary = True
            self.replica_stubs = []
        print(f"CacheServer {self.server_id}: Promoted to PRIMARY")
        return metadata_cache_channel_pb2.PromotionResponse(success=True)

    def RegisterReplica(self, request, context):
        replica_id = request.replica_id
        replica_port = request.replica_port
        try:
            channel = grpc.insecure_channel(f'localhost:{replica_port}')
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
            self.replica_stubs.append(stub)
            print(f"CacheServer {self.server_id}: Registered replica {replica_id} on port {replica_port}")
            return metadata_cache_channel_pb2.ReplicaRegisterResponse(success=True)
        except Exception as e:
            print(f"CacheServer {self.server_id}: Failed to register replica {replica_id}: {e}")
            return metadata_cache_channel_pb2.ReplicaRegisterResponse(success=False)

def register_with_metadata(server_id, port, cluster_id):
    try:
        with grpc.insecure_channel('localhost:50050') as channel:
            stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
            response = stub.RegisterServer(
                metadata_cache_channel_pb2.RegisterRequest(
                    server_id=server_id,
                    port=port,
                    cluster_id=cluster_id
                )
            )
            if response.success:
                print(f"CacheServer {server_id}: Successfully registered with MetadataServer on port {port}")
            else:
                print(f"CacheServer {server_id}: Failed to register with MetadataServer")
    except grpc.RpcError as e:
        print(f"CacheServer {server_id}: Failed to connect to MetadataServer: {e}")

def register_with_primary(replica_id, replica_port, primary_port):
    try:
        with grpc.insecure_channel(f'localhost:{primary_port}') as channel:
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
            response = stub.RegisterReplica(
                metadata_cache_channel_pb2.ReplicaRegisterRequest(
                    replica_id=replica_id,
                    replica_port=replica_port
                )
            )
            if response.success:
                print(f"Replica CacheServer {replica_id}: Successfully registered with primary at port {primary_port}")
            else:
                print(f"Replica CacheServer {replica_id}: Failed to register with primary")
    except grpc.RpcError as e:
        print(f"Replica CacheServer {replica_id}: Could not contact primary: {e}")

def serve(server_id, port, is_primary, cluster_id, primary_port=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_server = CacheServer(server_id, is_primary, cluster_id)
    metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(cache_server, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"CacheServer {server_id} started on port {port} as {'PRIMARY' if is_primary else 'REPLICA'}")

    if is_primary:
        register_with_metadata(server_id, port, cluster_id)
    else:
        register_with_primary(server_id, port, primary_port)

    server.wait_for_termination()

if __name__ == "__main__":
    # Example usage: python cache_server.py <server_id> <is_primary_flag> <cluster_id> [primary_port]
    server_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    is_primary_flag = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    is_primary = bool(is_primary_flag)
    cluster_id = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    port = 50051 + server_id
    primary_port = int(sys.argv[4]) if len(sys.argv) > 4 else None
    serve(server_id, port, is_primary, cluster_id, primary_port)
