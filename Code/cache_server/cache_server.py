# cache_server.py
import grpc
from concurrent import futures
import sys
import os
import threading
import time
import uuid
import zlib
import threading
import signal
import pickle

import collections
from collections import OrderedDict

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
        self.cache = OrderedDict()
        self.entity_index = {}  # (entity_type, entity_id) -> set of query_hash
        self.is_primary = is_primary
        self.lock = threading.Lock()
        self.replica_stubs = []
        self.replica_status = {}  # replica_stub -> {offset, failed_pings}
        self.cluster_id = cluster_id
        self.replication_id = str(uuid.uuid4())
        self.offset = 0
        self.replication_backlog = []  # List of (offset, query_hash, result) tuples
        self.backlog_max_size = 10
        self.ack_policy = ack_policy  # 0: no wait, 1-N: wait for N acks
        self.replication_timeout_ms = 1000
        self.server_port = None
        self.max_entries = 20

        self.persistence_dir = "cache_data"
        os.makedirs(self.persistence_dir, exist_ok=True)
        self.persistence_file = f"{self.persistence_dir}/cache_server_{server_id}.pickle"
        
        # Load data from file if it exists
        self.load_from_file()
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        self.health_lock = threading.Lock()
        self.health_check_running = True
        self.health_check_thread = threading.Thread(target=self.health_check_handler, daemon=True)
        self.health_check_thread.start()
    
    def save_to_file(self):
        """Save cache data to a persistence file"""
        print(f"CacheServer {self.server_id}: Saving cache data to file...")
        
        try:
            with self.lock:
                # Create a dictionary with all the necessary data
                data_to_save = {
                    'cache': self.cache,
                    'entity_index': self.entity_index,
                    'offset': self.offset
                }
                
                # Save data to a temporary file first
                temp_file = f"{self.persistence_file}.temp"
                with open(temp_file, 'wb') as f:
                    pickle.dump(data_to_save, f)
                    
                # Atomically replace the old file with the new one
                os.replace(temp_file, self.persistence_file)
                print(f"CacheServer {self.server_id}: Successfully saved cache data to {self.persistence_file}")
                
        except Exception as e:
            print(f"CacheServer {self.server_id}: Error saving cache data: {e}")

    def load_from_file(self):
        """Load cache data from a persistence file if it exists"""
        if not os.path.exists(self.persistence_file):
            print(f"CacheServer {self.server_id}: No persistence file found at {self.persistence_file}")
            return False
        
        try:
            print(f"CacheServer {self.server_id}: Loading cache data from {self.persistence_file}")
            with open(self.persistence_file, 'rb') as f:
                data = pickle.load(f)
            
            # Apply loaded data to the cache server
            with self.lock:
                self.cache = data['cache']
                self.entity_index = data['entity_index'] 
                self.offset = data['offset']
                
            print(f"CacheServer {self.server_id}: Successfully loaded cache with {len(self.cache)} entries")
            return True
        except Exception as e:
            print(f"CacheServer {self.server_id}: Error loading cache data: {e}")
            return False

    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown when SIGINT/SIGTERM is received"""
        signal_names = {signal.SIGINT: "SIGINT (Ctrl+C)", signal.SIGTERM: "SIGTERM"}
        signal_name = signal_names.get(signum, str(signum))
        
        print(f"\nCacheServer {self.server_id}: Received {signal_name}. Performing graceful shutdown...")
        
        # Save cache to file
        self.save_to_file()
        
        # Clean up resources
        self.health_check_running = False
        if hasattr(self, "health_check_thread") and self.health_check_thread.is_alive():
            self.health_check_thread.join(timeout=1.0)
        
        print(f"CacheServer {self.server_id}: Shutdown complete. Exiting.")
        sys.exit(0)

    def health_check_handler(self):
        """Dedicated thread for handling health check requests from sentinel"""
        print(f"CacheServer {self.server_id}: Started dedicated health check thread")
        while self.health_check_running:
            time.sleep(0.1)

    def replicate_async(self, stub, request):
        thread = threading.Thread(target=self._do_replicate, args=(stub, request))
        thread.daemon = True
        thread.start()

    def _do_replicate(self, stub, request):
        try:
            stub.SetResult(request, timeout=2.0)
        except Exception as e:
            print(f"Replication error: {e}")
            self.handle_replica_timeout(stub)

    def GetResult(self, request, context):
        query_hash = request.query_hash
        
        with self.lock:
            if query_hash in self.cache:
                # LRU behavior: Move accessed item to the end (most recently used position)
                result = self.cache[query_hash]
                # Remove and reinsert to move to the end
                self.cache.move_to_end(query_hash)
                found = True
                print(f"CacheServer {self.server_id}: GetResult for hash {query_hash}, found: {found}")
            else:
                result = ""
                found = False
                
            return metadata_cache_channel_pb2.CacheResponse(result=result, found=found)
    
    def wait_for_replicas(self, query_hash, result, num_replicas, timeout_ms, entity="", operation="read"):
        """Wait for at least num_replicas to acknowledge receiving the data"""
        if not self.replica_stubs or len(self.replica_stubs) < num_replicas:
            # Still do async replication but return false if we can't satisfy the policy
            self.send_to_replicas(query_hash, result, entity, operation)
            return False

        start_time = time.time()
        timeout_sec = timeout_ms / 1000.0
        
        # Track successful replications
        successful_replications = 0  # Start with 1 for the primary server
        
        # Create a lock and condition variable for thread synchronization
        ack_lock = threading.Lock()
        ack_condition = threading.Condition(ack_lock)
        
        # Extract stubs properly
        replicas_to_contact = []
        for item in self.replica_stubs:
            if isinstance(item, tuple):
                # If tuple (stub, info), extract the stub
                replicas_to_contact.append(item[0])
            else:
                # If already a stub
                replicas_to_contact.append(item)
        
        # Define a callback function to track acknowledgments
        def ack_callback(stub, success):
            nonlocal successful_replications
            with ack_lock:
                if success:
                    successful_replications += 1
                    print(f"Received acknowledgment ({successful_replications}/{num_replicas})")
                    if successful_replications >= num_replicas:
                        ack_condition.notify_all()
        
        # Create a specialized _do_replicate with callback
        def _do_replicate_with_callback(stub, request):
            try:
                response = stub.SetResult(request, timeout=2.0)
                if response.success:
                    with self.lock:
                        self.replica_status[stub]["offset"] += 1
                        self.replica_status[stub]["failed_pings"] = 0
                    ack_callback(stub, True)
                else:
                    ack_callback(stub, False)
            except Exception as e:
                print(f"Replication error: {e}")
                self.handle_replica_timeout(stub)
                ack_callback(stub, False)
        
        # Launch async replication threads for all replicas
        for stub in replicas_to_contact:
            request = metadata_cache_channel_pb2.CacheSetRequest(
                query_hash=query_hash,
                result=result,
                entity=entity,
                operation=operation
            )
            thread = threading.Thread(
                target=_do_replicate_with_callback,
                args=(stub, request)
            )
            thread.daemon = True
            thread.start()
        
        # Wait for enough acknowledgments or timeout
        with ack_lock:
            # Check if we already have enough acks
            if successful_replications >= num_replicas:
                return True
            
            # Wait for condition with timeout
            wait_result = ack_condition.wait(timeout_sec)
            
            # Return true if we got enough acks, false otherwise
            return successful_replications >= num_replicas

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
        entity = request.entity
        operation = request.operation if hasattr(request, 'operation') else "read"
        
        print(f"CacheServer {self.server_id}: Processing {operation} operation for entity {entity}")
        
        with self.lock:
            # For write operations, invalidate the cache for this entity
            if operation in ["create", "update", "delete"]:
                # Existing invalidation code remains the same...
                print(f"CacheServer {self.server_id}: Invalidating cache for entity {entity} due to {operation} operation")
                # Collect the keys to invalidate
                keys_to_invalidate = []
                if entity in self.entity_index:
                    keys_to_invalidate = list(self.entity_index[entity])
                    
                # Remove these keys from the cache
                for query_hash in keys_to_invalidate:
                    if query_hash in self.cache:
                        del self.cache[query_hash]
                        
                # Clear the entity index for this entity
                self.entity_index[entity] = set()
                
                # Propagate invalidation to replicas if this is a primary
                if self.is_primary:
                    self.propagate_entity_invalidation_to_replicas(entity)
                    
                return metadata_cache_channel_pb2.CacheSetResponse(success=True)
            
            # For read operations, cache the data with LRU eviction if needed
            else:
                print(f"CacheServer {self.server_id}: Caching result for hash {query_hash} (entity: {entity})")
                
                # LRU eviction: If we're at capacity and this is a new key, remove oldest entry
                if len(self.cache) >= self.max_entries and query_hash not in self.cache:
                    # popitem with last=False removes the first (oldest) item
                    oldest_key, _ = self.cache.popitem(last=False)
                    print(f"CacheServer {self.server_id}: LRU eviction removing oldest entry: {oldest_key}")
                    
                    # Also clean up entity_index for the evicted item
                    for entity_key, hash_set in self.entity_index.items():
                        if oldest_key in hash_set:
                            hash_set.remove(oldest_key)
                
                # Add or update cache entry (if update, it moves to the end automatically)
                self.cache[query_hash] = result
                self.offset += 1
                
                # Track this key for the given entity
                if entity not in self.entity_index:
                    self.entity_index[entity] = set()
                self.entity_index[entity].add(query_hash)
                
                # Add to replication backlog
                self.replication_backlog.append((self.offset, query_hash, result, entity, operation))
                
                # Trim backlog if needed
                if len(self.replication_backlog) > self.backlog_max_size:
                    self.replication_backlog.pop(0)
                    
                # Rest of the replication code remains the same...
                if self.is_primary:
                    if self.ack_policy > 0:
                        # Wait for replication based on ack_policy
                        success = self.wait_for_replicas(query_hash, result, self.ack_policy, self.replication_timeout_ms, entity, operation)
                        return metadata_cache_channel_pb2.CacheSetResponse(success=success)
                    else:
                        # Default async behavior
                        self.send_to_replicas(query_hash, result, entity, operation)
                        
                return metadata_cache_channel_pb2.CacheSetResponse(success=True)

    def GetLoad(self, request, context):
        """Handle health check requests and report cache statistics"""
        with self.health_lock:
            load = len(self.cache)
            capacity_percentage = (load / self.max_entries) * 100 if self.max_entries > 0 else 0
            print(f"CacheServer {self.server_id}: Current load: {load}/{self.max_entries} ({capacity_percentage:.1f}%)")
            return metadata_cache_channel_pb2.LoadResponse(load=load)



    def GetRole(self, request, context):
        return metadata_cache_channel_pb2.RoleResponse(is_primary=self.is_primary)

    def PromoteToPrimary(self, request, context):
        """Handle promotion from replica to primary status"""
        print(f"CacheServer {self.server_id}: Received promotion to PRIMARY")
        
        with self.lock:
            was_replica = not self.is_primary
            self.is_primary = True
            # Clear old replica list as we'll rebuild it
            self.replica_stubs = []
            self.replica_status = {}
        
        if was_replica:
            # Register with load balancer as the new primary
            register_with_load_balancer(self.server_id, self.server_port, self.cluster_id)
            
            # Discover replicas from Sentinel
            threading.Thread(target=self.discover_replicas, daemon=True).start()
            
            # Start the replica handler thread if not already running
            threading.Thread(target=self.replica_handler, daemon=True).start()
            
            print(f"CacheServer {self.server_id}: Successfully promoted to PRIMARY")
        
        return metadata_cache_channel_pb2.PromotionResponse(success=True)

    def discover_replicas(self, sentinel_port=70100):
        """Discover and connect to all replicas in the cluster"""
        try:
            # Connect to sentinel and get replica information
            channel = grpc.insecure_channel(f"localhost:{sentinel_port}")
            stub = sentinel_pb2_grpc.SentinelServiceStub(channel)
            request = sentinel_pb2.GetClusterReplicasRequest(
                cluster_id=self.cluster_id,
                server_id=self.server_id
            )
            response = stub.GetClusterReplicas(request)
            
            if response.success:
                # Connect to each replica
                for replica in response.replicas:
                    try:
                        replica_channel = grpc.insecure_channel(f"localhost:{replica.port}")
                        replica_stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(replica_channel)
                        
                        # Add to our replica list
                        with self.lock:
                            self.replica_stubs.append(replica_stub)
                            self.replica_status[replica_stub] = {"offset": 0, "failed_pings": 0}
                        
                        print(f"CacheServer {self.server_id}: Connected to replica {replica.server_id}")
                        
                        # Send our current snapshot to the replica to ensure consistency
                        self.send_snapshot_to_replica(replica_stub)
                        
                    except Exception as e:
                        print(f"CacheServer {self.server_id}: Failed to connect to replica {replica.server_id}: {e}")
                        
                print(f"CacheServer {self.server_id}: Successfully discovered {len(response.replicas)} replicas")
            else:
                print(f"CacheServer {self.server_id}: Sentinel returned failure for replica discovery")
                
        except Exception as e:
            print(f"CacheServer {self.server_id}: Error in replica discovery: {e}")

    def RegisterReplica(self, request, context):
        replica_id = request.replica_id
        replica_port = request.replica_port
        
        try:
            target_address = f'localhost:{replica_port}'
            channel = grpc.insecure_channel(target_address)
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
            
            with self.lock:
                # Store both stub and its target address
                self.replica_stubs.append((stub, {
                    'target_address': f'localhost:{replica_port}',
                    'server_id': replica_id
                }))
                self.replica_status[stub] = {"offset": 0, "failed_pings": 0}
            
            print(f"CacheServer {self.server_id}: Registered replica {replica_id}")
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
        replica_id = request.server_id if hasattr(request, 'server_id') else None
        
        if replica_offset < self.offset:
            print(f"CacheServer {self.server_id}: Replica {replica_id} is behind (offset {replica_offset} vs {self.offset})")
            
            # Calculate missing commands
            missing_commands = []
            for cmd_offset, query_hash, result, entity, operation in self.replication_backlog:
                if cmd_offset > replica_offset:
                    missing_commands.append((query_hash, result, entity, operation))
            
            # Check if replica needs full snapshot
            if not missing_commands or replica_offset < self.offset - len(self.replication_backlog):
                print(f"CacheServer {self.server_id}: Replica too far behind, needs full snapshot")
                return metadata_cache_channel_pb2.PingResponse(up_to_date=False)
            else:
                # Send incremental updates
                print(f"CacheServer {self.server_id}: Sending {len(missing_commands)} incremental updates to replica")
                success = True
                
                for query_hash, result, entity, operation in missing_commands:
                    try:
                        # Find matching stub by address
                        matching_stub = None
                        for stub, stub_info in self.replica_stubs:
                            if stub_info.get('server_id') == replica_id:
                                matching_stub = stub
                                break
                        
                        if matching_stub:
                            response = matching_stub.SetResult(metadata_cache_channel_pb2.CacheSetRequest(
                                query_hash=query_hash,
                                result=result,
                                entity=entity,
                                operation=operation
                            ))
                            if not response.success:
                                success = False
                                break
                        else:
                            print(f"CacheServer {self.server_id}: No matching stub found for {replica_id}")
                            success = False
                    except Exception as e:
                        print(f"CacheServer {self.server_id}: Error sending incremental update: {e}")
                        success = False
                        break
                
                return metadata_cache_channel_pb2.PingResponse(up_to_date=success)
        
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

    def send_snapshot_to_replica(self, replica_stub):
        """Send the current cache snapshot to a replica"""
        try:
            with self.lock:
                compressed_data = zlib.compress(str(self.cache).encode())
            response = replica_stub.SetSnapshot(metadata_cache_channel_pb2.SnapshotRequest(data=compressed_data))
            if response.success:
                print(f"CacheServer {self.server_id}: Sent snapshot to replica successfully")
            else:
                print(f"CacheServer {self.server_id}: Failed to send snapshot to replica")
        except Exception as e:
            print(f"CacheServer {self.server_id}: Error sending snapshot to replica: {e}")
    

    def send_to_replicas(self, query_hash, result, entity="", operation="read"):
        """Send updates to all replicas asynchronously"""
        if not self.replica_stubs:
            return
        
        for item in list(self.replica_stubs):
            # Extract stub properly regardless of format
            stub = item[0] if isinstance(item, tuple) else item
            
            request = metadata_cache_channel_pb2.CacheSetRequest(
                query_hash=query_hash,
                result=result,
                entity=entity,
                operation=operation
            )
            # Use the async method instead of blocking call
            self.replicate_async(stub, request)



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
            for i, (s, _) in enumerate(self.replica_stubs):
                if s == stub:
                    self.replica_stubs.pop(i)
                    break
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
                    for item in list(self.replica_stubs):
                        # Extract stub properly regardless of format
                        stub = item[0] if isinstance(item, tuple) else item
                        
                        status = self.replica_status.get(stub)
                        if status and status["failed_pings"] > 0:
                            print(f"CacheServer {self.server_id}: Retrying ping to replica...")
                            self.handle_replica_timeout(stub)

    
    def propagate_entity_invalidation_to_replicas(self, entity):
        """Propagate entity cache invalidation to all replicas"""
        print(f"CacheServer {self.server_id}: Propagating invalidation for entity {entity} to replicas")
        for item in list(self.replica_stubs):
            # Extract stub properly regardless of format
            stub = item[0] if isinstance(item, tuple) else item
            try:
                _ = stub.InvalidateEntityCache(metadata_cache_channel_pb2.InvalidateEntityRequest(entity=entity))
            except grpc.RpcError:
                self.handle_replica_timeout(stub)

    def InvalidateEntityCache(self, request, context):
        """Handle cache invalidation for a specific entity"""
        entity = request.entity
        print(f"CacheServer {self.server_id}: Invalidating cache for entity {entity}")
        
        keys_to_invalidate = []
        with self.lock:
            if entity in self.entity_index:
                keys_to_invalidate = list(self.entity_index[entity])
                
                # Remove these keys from the cache
                for query_hash in keys_to_invalidate:
                    if query_hash in self.cache:
                        del self.cache[query_hash]
                
                # Clear the entity index for this entity
                self.entity_index[entity] = set()
        
        # Propagate invalidation to replicas if this is a primary
        if self.is_primary:
            self.propagate_entity_invalidation_to_replicas(entity)
        
        return metadata_cache_channel_pb2.InvalidateEntityResponse(success=True)
    
    def start_health_check_thread(self):
        def health_check_handler():
            while True:
                # Process any pending health check requests with priority
                time.sleep(0.1)  # Small sleep to prevent CPU spinning
        
        health_thread = threading.Thread(target=health_check_handler, daemon=True)
        health_thread.start()
        print(f"CacheServer {self.server_id}: Health check thread started")



def register_with_sentinel(server_id, port, cluster_id, sentinel_address="localhost", sentinel_port=70100):
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
    Efficiently synchronizes data using incremental updates when possible.
    
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
            
            # Step 1: Register with the primary
            reg_request = metadata_cache_channel_pb2.ReplicaRegisterRequest(
                replica_id=replica_id,
                replica_port=replica_port
            )
            reg_response = stub.RegisterReplica(reg_request)
            
            if not reg_response.success:
                print(f"CacheServer {replica_id}: Registration with primary failed.")
                return
                
            print(f"CacheServer {replica_id}: Successfully registered with primary on port {primary_port}.")
            
            # Step 2: Synchronize data
            current_offset = cache_server.offset
            print(f"CacheServer {replica_id}: Current offset: {current_offset}")
            
            # Try to get incremental updates first
            try:
                # Send our current offset to the primary
                ping_request = metadata_cache_channel_pb2.PingRequest(
                    offset=current_offset,
                    server_id=replica_id  # Add replica's ID
                )
                pong = stub.PingWithOffset(ping_request)
                
                if pong.up_to_date:
                    print(f"CacheServer {replica_id}: Replica is already up-to-date with primary.")
                else:
                    # The primary should have sent incremental updates if possible
                    # If we're too far behind, we need to request a full snapshot
                    
                    # Check if replica is still behind after PingWithOffset
                    # PingWithOffset should have tried to catch us up if possible
                    verify_request = metadata_cache_channel_pb2.PingRequest(
                        offset=cache_server.offset,
                        server_id=replica_id
                    )
                    verify_pong = stub.PingWithOffset(verify_request)
                    
                    if not verify_pong.up_to_date:
                        print(f"CacheServer {replica_id}: Incremental update not possible or incomplete. Requesting full snapshot...")
                        cache_server.request_snapshot(primary_port)
                    else:
                        print(f"CacheServer {replica_id}: Successfully synchronized using incremental updates.")
            except grpc.RpcError as e:
                print(f"CacheServer {replica_id}: Error during synchronization: {e}")
                print(f"CacheServer {replica_id}: Falling back to full snapshot...")
                cache_server.request_snapshot(primary_port)
                
    except Exception as e:
        print(f"CacheServer {replica_id}: Error during registration with primary: {e}")

def register_with_load_balancer(server_id, port, cluster_id, load_balancer_address="localhost", load_balancer_port=70000):
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

def get_primary_port(cluster_id, sentinel_address="localhost", sentinel_port=70100):
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

        if port == primary_port:
            print(f"CacheServer {server_id}: Cannot register with self as primary. Port conflict detected.")
            print(f"CacheServer {server_id}: Switching to PRIMARY mode.")
            assigned_as_primary = True

        if primary_port == 0 or primary_port is None:
            print(f"CacheServer {server_id}: No primary port provided by Sentinel. Cannot register as replica.")
            return

    cache_server = CacheServer(server_id, is_primary=assigned_as_primary, cluster_id=cluster_id, ack_policy=ack_policy)
    cache_server.server_port = port
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(cache_server, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()

    if assigned_as_primary:
        register_with_load_balancer(server_id, port, cluster_id)

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
    cluster_id = server_id // 4
    port = 50051 + server_id

    ack_policy = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    if ack_policy < 0 or ack_policy > 3 or len(sys.argv) < 3:
        print(f"Using default (0) for acknowledgement policy.")
        ack_policy = 0

    serve(server_id, port, cluster_id, ack_policy=ack_policy)