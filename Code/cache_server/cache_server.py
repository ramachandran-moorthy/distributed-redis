# cache_server.py
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

# Import the sentinel service client stubs (from the new sentinel.proto)
import sentinel_pb2
import sentinel_pb2_grpc

PING_TIMEOUT = 3
MAX_RETRY = 2

class CacheServer(metadata_cache_channel_pb2_grpc.CacheServiceServicer):
    def __init__(self, server_id, is_primary=False, cluster_id=None, ack_policy=0):
        self.server_id = server_id
        self.cache = {}  # hash -> result
        self.is_primary = is_primary
        self.lock = threading.Lock()
        self.replica_stubs = []
        self.replica_status = {}  # replica_stub -> {offset, failed_pings}
        self.cluster_id = cluster_id
        self.replication_id = str(uuid.uuid4())
        self.offset = 0
        self.replication_backlog = []  # List of (offset, query_hash, result) tuples
        self.backlog_max_size = 100
        self.ack_policy = ack_policy  # 0: no wait, 1-N: wait for N acks
        self.replication_timeout_ms = 1000

    def GetResult(self, request, context):
        query_hash = request.query_hash
        with self.lock:
            result = self.cache.get(query_hash, "")
            found = query_hash in self.cache
        print(f"CacheServer {self.server_id}: GetResult for hash {query_hash}, found: {found}")
        return metadata_cache_channel_pb2.CacheResponse(result=result, found=found)
    
    def wait_for_replicas(self, query_hash, result, num_replicas, timeout_ms):
        """Wait for at least num_replicas to acknowledge receiving the data"""
        if not self.replica_stubs or len(self.replica_stubs) < num_replicas:
            # Still do async replication but return false if we can't satisfy the policy
            self.send_to_replicas(query_hash, result)
            return False
        
        start_time = time.time()
        timeout_sec = timeout_ms / 1000.0
        
        # Create sets to track replica acknowledgments
        pending_replicas = set(self.replica_stubs)
        acknowledged = set()
        
        # Send to all replicas
        for stub in list(self.replica_stubs):
            try:
                ack = stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
                if ack.success:
                    with self.lock:
                        self.replica_status[stub]["offset"] += 1
                        self.replica_status[stub]["failed_pings"] = 0
                    acknowledged.add(stub)
                    pending_replicas.remove(stub)
            except grpc.RpcError:
                self.handle_replica_timeout(stub)
        
        # Wait for acknowledgments or timeout
        while time.time() - start_time < timeout_sec and len(acknowledged) < num_replicas:
            # Try any remaining replicas that haven't acknowledged yet
            for stub in list(pending_replicas):
                try:
                    ack = stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
                    if ack.success:
                        with self.lock:
                            self.replica_status[stub]["offset"] += 1
                            self.replica_status[stub]["failed_pings"] = 0
                        acknowledged.add(stub)
                        pending_replicas.remove(stub)
                        if len(acknowledged) >= num_replicas:
                            break
                except grpc.RpcError:
                    self.handle_replica_timeout(stub)
                    pending_replicas.remove(stub)
            
            if len(acknowledged) < num_replicas and pending_replicas:
                time.sleep(0.05)  # Small sleep to prevent CPU spinning
        
        print(f"CacheServer {self.server_id}: Waited for replication, got {len(acknowledged)}/{num_replicas} acks")
        return len(acknowledged) >= num_replicas

    def _send_and_track_ack(self, stub, query_hash, result, pending_replicas, acknowledged):
        try:
            ack = stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(
                query_hash=query_hash, result=result))
            if ack.success:
                with self.lock:
                    self.replica_status[stub]["offset"] += 1
                    self.replica_status[stub]["failed_pings"] = 0
                    pending_replicas.remove(stub)
                    acknowledged.add(stub)
        except grpc.RpcError:
            self.handle_replica_timeout(stub)

    def SetResult(self, request, context):
        query_hash = request.query_hash
        result = request.result
        print(f"CacheServer {self.server_id}: saving hash {query_hash} with value {result}")
        
        with self.lock:
            self.cache[query_hash] = result
            self.offset += 1
            
            # Add to replication backlog (change #6)
            self.replication_backlog.append((self.offset, query_hash, result))
            
            # Trim backlog if needed
            if len(self.replication_backlog) > self.backlog_max_size:
                self.replication_backlog.pop(0)
        
        if self.is_primary:
            if self.ack_policy > 0:
                # Wait for replication based on ack_policy
                success = self.wait_for_replicas(query_hash, result, self.ack_policy, self.replication_timeout_ms)
                return metadata_cache_channel_pb2.CacheSetResponse(success=success)
            else:
                # Default async behavior
                self.send_to_replicas(query_hash, result)
        
        return metadata_cache_channel_pb2.CacheSetResponse(success=True)

    def GetLoad(self, request, context):
        return metadata_cache_channel_pb2.LoadResponse(load=len(self.cache))

    def GetRole(self, request, context):
        return metadata_cache_channel_pb2.RoleResponse(is_primary=self.is_primary)

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
            print(f"CacheServer {self.server_id}: Registered replica {replica_id}")
            # Return a response indicating success.
            return metadata_cache_channel_pb2.ReplicaRegisterResponse(success=True)
        except Exception as e:
            print(f"CacheServer {self.server_id}: Error during replica registration: {e}")
            return metadata_cache_channel_pb2.ReplicaRegisterResponse(success=False)


    def GetSnapshot(self, request, context):
        with self.lock:
            compressed_data = zlib.compress(str(self.cache).encode())
        return metadata_cache_channel_pb2.SnapshotResponse(data=compressed_data, success=True)

    def SetSnapshot(self, request, context):
        data = zlib.decompress(request.data)
        snapshot = eval(data.decode())
        with self.lock:
            for key, value in snapshot.items():
                if key not in self.cache:
                    self.cache[key] = value
            self.offset = len(self.cache)
        print(f"CacheServer {self.server_id}: Replica updated with snapshot")
        return metadata_cache_channel_pb2.SnapshotResponse(success=True, data=request.data)
    
    def GetOffset(self, request, context):
        return metadata_cache_channel_pb2.OffsetResponse(offset=self.offset)
    
    def PingWithOffset(self, request, context):
        replica_offset = request.offset
        if replica_offset < self.offset:
            # Calculate what data needs to be sent to catch up the replica
            # Instead of always sending a full snapshot
            missing_commands = []
            for cmd_offset, query_hash, result in self.replication_backlog:
                if cmd_offset > replica_offset:
                    missing_commands.append((query_hash, result))
            
            # If the replica is too far behind, send full snapshot
            if not missing_commands or replica_offset < self.offset - len(self.replication_backlog):
                return self.send_snapshot(context.peer())
            else:
                # Catch up replica with just the missing commands
                for query_hash, result in missing_commands:
                    try:
                        self.send_command_to_replica(context.peer(), query_hash, result)
                    except:
                        return metadata_cache_channel_pb2.PingResponse(up_to_date=False)
        
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
                        print(f"Replica {self.server_id}: key {key} updated")
                self.offset = len(self.cache)
            print("[REPLICA DEBUG] Snapshot received and applied")
        except grpc.RpcError as e:
            print(f"[REPLICA DEBUG] Failed to get snapshot: {e}")

    def send_to_replicas(self, query_hash, result):
        print(f"CacheServer {self.server_id}: Sending result for hash {query_hash} to replicas")
        for stub in list(self.replica_stubs):
            try:
                ack = stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(query_hash=query_hash, result=result))
                if ack.success:
                    self.replica_status[stub]["offset"] += 1
                    self.replica_status[stub]["failed_pings"] = 0
            except grpc.RpcError:
                self.handle_replica_timeout(stub)

    def handle_replica_timeout(self, stub):
        status = self.replica_status.get(stub, None)
        if not status:
            return
        status["failed_pings"] += 1
        if status["failed_pings"] == 1:
            self.ping_and_snapshot(stub)
        elif status["failed_pings"] <= MAX_RETRY:
            self.ping_and_snapshot(stub)
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
            print(f"CacheServer {self.server_id}: Ping failed for replica")

    def replica_handler(self):
        while True:
            time.sleep(1)
            if self.is_primary:
                with self.lock:
                    for stub in list(self.replica_stubs):
                        status = self.replica_status.get(stub)
                        if status and status["failed_pings"] > 0:
                            print(f"CacheServer {self.server_id}: Retrying ping to replica...")
                            self.handle_replica_timeout(stub)

def register_with_sentinel(server_id, port, cluster_id, sentinel_address="localhost", sentinel_port=50060):
    """
    Register with the sentinel service to determine role
    """
    try:
        channel = grpc.insecure_channel(f"{sentinel_address}:{sentinel_port}")
        stub = sentinel_pb2_grpc.SentinelServiceStub(channel)
        request = sentinel_pb2.CacheServerRegistrationRequest(server_id=server_id, port=port, cluster_id=cluster_id)
        response = stub.RegisterServer(request)
        print(f"CacheServer {server_id}: Registered with Sentinel. Is primary? {response.is_primary}")
        return response.is_primary, response.primary_port
    except Exception as e:
        print(f"CacheServer {server_id}: Error registering with Sentinel: {e}")
        return False, None
    
def register_with_primary(replica_id, replica_port, primary_port, cache_server):
    """
    Registers this replica cache server with the primary cache server.
    
    Parameters:
      replica_id (int): The unique identifier for this replica cache server.
      replica_port (int): The port on which this replica is running.
      primary_port (int): The port of the primary cache server.
      cache_server: Instance of the local CacheServer class (to access state, e.g., offset).
    """
    print(f"CacheServer {replica_id}: Registering as replica with primary on port {primary_port}...")
    try:
        # Connect to the primary cache server
        with grpc.insecure_channel(f'localhost:{primary_port}') as channel:
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
            # Prepare and send the registration request
            reg_request = metadata_cache_channel_pb2.ReplicaRegisterRequest(
                replica_id=replica_id,
                replica_port=replica_port
            )
            reg_response = stub.RegisterReplica(reg_request)
            print(f"Debug: Received registration response: {reg_response}")
            if reg_response.success:
                print(f"CacheServer {replica_id}: Successfully registered with primary on port {primary_port}.")
            else:
                print(f"CacheServer {replica_id}: Registration with primary failed.")
            
            # Perform an initial ping to check synchronization status.
            try:
                ping_request = metadata_cache_channel_pb2.PingRequest(offset=cache_server.offset)
                pong = stub.PingWithOffset(ping_request)
                if not pong.up_to_date:
                    print(f"CacheServer {replica_id}: Replica is behind. Requesting snapshot from primary...")
                    # Request snapshot if replica is behind in the cache.
                    cache_server.request_snapshot(primary_port)
                else:
                    print(f"CacheServer {replica_id}: Replica is up-to-date with primary.")
            except grpc.RpcError as e:
                print(f"CacheServer {replica_id}: Failed to ping primary: {e}")
    except Exception as e:
        print(f"CacheServer {replica_id}: Error during registration with primary: {e}")
    
def register_with_load_balancer(server_id, port, cluster_id, load_balancer_address="localhost", load_balancer_port=50053):
    try:
        channel = grpc.insecure_channel(f"{load_balancer_address}:{load_balancer_port}")
        stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
        # We use the NotifyPromotion endpoint here as a way to register the primary.
        request = metadata_cache_channel_pb2.NotifyPromotionRequest(
            server_id=server_id,
            port=port,
            cluster_id=cluster_id
        )
        response = stub.NotifyPromotion(request)
        if response.success:
            print(f"CacheServer {server_id}: Successfully registered as PRIMARY with the load balancer.")
        else:
            print(f"CacheServer {server_id}: Registration with load balancer failed.")
    except Exception as e:
        print(f"CacheServer {server_id}: Error registering with load balancer: {e}")

def get_primary_port(cluster_id, sentinel_address="localhost", sentinel_port=50060):
    try:
        channel = grpc.insecure_channel(f"{sentinel_address}:{sentinel_port}")
        stub = sentinel_pb2_grpc.SentinelServiceStub(channel)
        request = sentinel_pb2.PrimaryForClusterRequest(cluster_id=cluster_id)
        response = stub.GetPrimaryForCluster(request)
        if response.found:
            return response.port
    except Exception as e:
        print(f"Error querying sentinel for primary: {e}")
    return None

def serve(server_id, port, cluster_id, primary_port=None, ack_policy=0):
    # Register with the sentinel to determine role
    assigned_as_primary, sentinel_primary_port = register_with_sentinel(server_id, port, cluster_id)
    
    # If we're a replica and primary_port wasn't specified, use the one from sentinel
    if not assigned_as_primary and primary_port is None:
        primary_port = sentinel_primary_port
        if primary_port == 0 or primary_port is None:
            print(f"CacheServer {server_id}: No primary port provided by Sentinel. Cannot register as replica.")
            return

    cache_server = CacheServer(server_id, is_primary=assigned_as_primary, cluster_id=cluster_id, ack_policy=ack_policy)
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(cache_server, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    if assigned_as_primary:
        threading.Thread(target=cache_server.replica_handler, daemon=True).start()
        ack_description = {
            0: "async (no waiting)",
            1: "wait for 1 replica",
            2: "wait for 2 replicas",
            3: "wait for 3 replicas"
        }
        print(f"CacheServer {server_id}: Started as PRIMARY with replication policy: {ack_description.get(ack_policy, str(ack_policy))}")
    else:
        print(f"CacheServer {server_id}: Operating as REPLICA. Registering with current primary on port {primary_port}...")
        register_with_primary(server_id, port, primary_port, cache_server)
    
    print(f"CacheServer {server_id} started on port {port}.")
    server.wait_for_termination()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python cache_server.py <server_id> [ack_policy]")
        sys.exit(1)

    server_id = int(sys.argv[1])
    cluster_id = server_id // 3
    port = 50051 + server_id

    ack_policy = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    if ack_policy < 0 or ack_policy > 3 or len(sys.argv) < 3:
        print(f"Using default (0) for acknowledgement policy.")
        ack_policy = 0

    serve(server_id, port, cluster_id, ack_policy=ack_policy)