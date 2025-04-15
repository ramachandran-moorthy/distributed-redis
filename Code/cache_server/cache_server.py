import grpc
from concurrent import futures
import sys
import os
import threading
import time
import uuid
import zlib

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc

PING_TIMEOUT = 3
MAX_RETRY = 2

class CacheServer(metadata_cache_channel_pb2_grpc.CacheServiceServicer):
    def __init__(self, server_id, is_primary=False, cluster_id=None):
        self.server_id = server_id
        self.cache = {}  # hash -> result
        self.is_primary = is_primary
        self.lock = threading.Lock()
        self.replica_stubs = []
        self.replica_status = {}  # replica_stub -> {offset, failed_pings}
        self.cluster_id = cluster_id
        self.replication_id = str(uuid.uuid4())
        self.offset = 0

    # gRPC Methods
    def GetResult(self, request, context):
        query_hash = request.query_hash
        result = self.cache.get(query_hash, "")
        found = query_hash in self.cache
        return metadata_cache_channel_pb2.CacheResponse(result=result, found=found)

    def SetResult(self, request, context):
        query_hash = request.query_hash
        result = request.result
        print(f"saved hash {query_hash} with value {result}")
        with self.lock:
            self.cache[query_hash] = result
            self.offset += 1
        if self.is_primary:
            # for stub in list(self.replica_stubs):
            # Trigger replication immediately
            self.send_to_replicas(query_hash, result)
        return metadata_cache_channel_pb2.CacheSetResponse(success=True)

    def GetLoad(self, request, context):
        return metadata_cache_channel_pb2.LoadResponse(load=len(self.cache))

    def GetRole(self, request, context):
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
            with self.lock:
                self.replica_stubs.append(stub)
                self.replica_status[stub] = {"offset": 0, "failed_pings": 0}
                print("The data of the replica saved with the primary!!")
                # Trigger initial sync
                try:
                    pong = stub.GetOffset(metadata_cache_channel_pb2.EmptyRequest())
                    replica_offset = pong.offset
                    if replica_offset < self.offset:
                        compressed_data = zlib.compress(str(self.cache).encode())
                        stub.SetSnapshot(metadata_cache_channel_pb2.SnapshotRequest(data=compressed_data))
                except grpc.RpcError:
                    print("Initial ping to replica failed during register")
                
            return metadata_cache_channel_pb2.ReplicaRegisterResponse(success=True)
        except Exception as e:
            return metadata_cache_channel_pb2.ReplicaRegisterResponse(success=False)
    def GetSnapshot(self, request, context):
        with self.lock:
            compressed_data = zlib.compress(str(self.cache).encode())
        return metadata_cache_channel_pb2.SnapshotResponse(data=compressed_data)

    def SetSnapshot(self, request, context):
        data = zlib.decompress(request.data)
        snapshot = eval(data.decode())
        with self.lock:
            for key, value in snapshot.items():
                if key not in self.cache:
                    self.cache[key] = value
            self.offset = len(self.cache)
        print("Replica updated with snapshot")
        return metadata_cache_channel_pb2.SnapshotAck(success=True)
    
    def GetOffset(self, request, context):
        return metadata_cache_channel_pb2.OffsetResponse(offset=self.offset)
    
    def PingWithOffset(self, request, context):
        replica_offset = request.offset
        print(f"[PING DEBUG] Replica pinged with offset: {replica_offset}, primary offset: {self.offset}")
        if replica_offset < self.offset:
            # If the replica is behind, inform it to request the snapshot
            print(f"[PING DEBUG] Replica is behind, asking it to pull the snapshot from primary")
            
            # Instead of sending the snapshot directly, we rely on the replica to pull it.
            return metadata_cache_channel_pb2.PingResponse(up_to_date=False)

        print(f"[PING DEBUG] Replica is up-to-date")
        return metadata_cache_channel_pb2.PingResponse(up_to_date=True)
        

    def request_snapshot(self, primary_port):
        try:
            channel = grpc.insecure_channel(f'localhost:{primary_port}')
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)

            print("[REPLICA DEBUG] Requesting snapshot from primary...")
            compressed_data = stub.GetSnapshot(metadata_cache_channel_pb2.EmptyRequest()).data
            data = zlib.decompress(compressed_data)
            snapshot = eval(data.decode())
            
            with self.lock:
                for key, value in snapshot.items():
                    if key not in self.cache:
                        self.cache[key] = value
                        print(f"value of key : {key} is {value}")
                self.offset = len(self.cache)
            
            print("[REPLICA DEBUG] Snapshot received and applied")
        except grpc.RpcError as e:
            print(f"[REPLICA DEBUG] Failed to get snapshot: {e}")
    # Custom Logic
    def send_to_replicas(self, query_hash, result):
        print("came here to send to the replicas!")
        for stub in list(self.replica_stubs):
            try:
                ack = stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
                if ack.success:
                    self.replica_status[stub]["offset"] += 1
                    self.replica_status[stub]["failed_pings"] = 0
            except grpc.RpcError:
                print("Going to ping the replica")
                self.handle_replica_timeout(stub)

    def handle_replica_timeout(self, stub):
        print("Replica stubs tracked:", list(self.replica_status.keys()))
        print("Current stub:", stub)

        status = self.replica_status.get(stub, None)
        if not status:
            return

        status["failed_pings"] += 1
        print("In between handle_replica_timeout")
        if status["failed_pings"] == 1:
            # First failure → try ping and snapshot
            self.ping_and_snapshot(stub)

        elif status["failed_pings"] <= MAX_RETRY:
            # Second failure → try ping and snapshot again
            self.ping_and_snapshot(stub)

            # If still bad, remove it
            print(f"CacheServer {self.server_id}: Marking replica as DOWN")
            self.replica_stubs.remove(stub)
            del self.replica_status[stub]

    def ping_and_snapshot(self, stub):
        try:
            pong = stub.GetRole(metadata_cache_channel_pb2.EmptyRequest())
            if pong:
                compressed_data = zlib.compress(str(self.cache).encode())
                stub.SetSnapshot(metadata_cache_channel_pb2.SnapshotRequest(data=compressed_data))
        except grpc.RpcError:
            print(f"CacheServer {self.server_id}: Ping failed again")

    def replica_handler(self):
        while True:
            time.sleep(1)  # yield CPU
            if self.is_primary:
                with self.lock:
                    for stub in list(self.replica_stubs):
                        status = self.replica_status.get(stub)
                        if status and status["failed_pings"] > 0:
                            print(f"CacheServer {self.server_id}: Retrying ping to failed replica...")
                            self.handle_replica_timeout(stub)

# Networking Helpers

def register_with_metadata(server_id, port, cluster_id):
    with grpc.insecure_channel('localhost:50050') as channel:
        stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
        stub.RegisterServer(metadata_cache_channel_pb2.RegisterRequest(server_id=server_id, port=port, cluster_id=cluster_id))

def register_with_primary(replica_id, replica_port, primary_port, cache_server):
    print("This replica registered with primary!")
    with grpc.insecure_channel(f'localhost:{primary_port}') as channel:
        stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
        stub.RegisterReplica(
            metadata_cache_channel_pb2.ReplicaRegisterRequest(
                replica_id=replica_id,
                replica_port=replica_port
            )
        )

        try:
            pong = stub.PingWithOffset(metadata_cache_channel_pb2.PingRequest(offset=cache_server.offset))
            if not pong.up_to_date:
                print(f"[REPLICA DEBUG] Replica is behind. Requesting snapshot...")
                cache_server.request_snapshot(primary_port)
            else:
                print(f"[REPLICA DEBUG] Replica is up-to-date")
        except grpc.RpcError as e:
            print(f"[REPLICA DEBUG] Failed to ping primary: {e}")

def serve(server_id, port, is_primary, cluster_id, primary_port=None):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_server = CacheServer(server_id, is_primary, cluster_id)
    metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(cache_server, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    if is_primary:
        register_with_metadata(server_id, port, cluster_id)
        threading.Thread(target=cache_server.replica_handler, daemon=True).start()
    else:
        def delayed_register():
            time.sleep(0.5)
            register_with_primary(server_id, port, primary_port, cache_server)
        threading.Thread(target=delayed_register).start()

    print(f"CacheServer {server_id} started on port {port} as {'PRIMARY' if is_primary else 'REPLICA'}")
    server.wait_for_termination()

if __name__ == "__main__":
    server_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    is_primary_flag = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    is_primary = bool(is_primary_flag)
    cluster_id = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    port = 50051 + server_id
    primary_port = int(sys.argv[4]) if len(sys.argv) > 4 else None
    serve(server_id, port, is_primary, cluster_id, primary_port)
