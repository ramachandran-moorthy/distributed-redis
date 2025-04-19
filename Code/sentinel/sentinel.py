# sentinel.py
import grpc
import time
import sys
import os
import threading
import concurrent.futures
import consul
import time

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

# Import sentinel proto generated files
import sentinel_pb2
import sentinel_pb2_grpc
import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc

# Sentinel configuration
SENTINEL_PORT = 70100
CHECK_INTERVAL = 5  # seconds

def notify_load_balancer(new_primary, cluster_id, load_balancer_address="localhost", load_balancer_port=70000, max_retries=3):
    """
    Notifies the load balancer that the primary has changed.
    new_primary is a tuple: (server_id, port)
    """
    for attempt in range(max_retries):
        try:
            channel = grpc.insecure_channel(f"{load_balancer_address}:{load_balancer_port}")
            stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
            request = metadata_cache_channel_pb2.NotifyPromotionRequest(
                server_id=new_primary[0],
                port=new_primary[1],
                cluster_id=cluster_id
            )
            response = stub.NotifyPromotion(request)
            if response.success:
                print(f"Sentinel: Notified load balancer about new primary {new_primary[0]} in cluster {cluster_id}.")
                return True
            else:
                print("Sentinel: Load balancer failed to acknowledge promotion.")
        except Exception as e:
            print(f"Sentinel: Error notifying load balancer (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retrying
    
    print(f"Sentinel: Failed to notify load balancer after {max_retries} attempts")
    return False

class SentinelServiceServicer(sentinel_pb2_grpc.SentinelServiceServicer):
    def __init__(self):
        # self.clusters stores the state per cluster:
        # Key: cluster_id, Value: { 'primary': (server_id, port), 'replicas': [ (server_id, port), ... ] }
        self.clusters = {}
        self.lock = threading.Lock()
        # Track server health: {port: {'failed_checks': count, 'last_check': timestamp}}
        self.server_health = {}
        self.HEALTH_THRESHOLD = 3  # Mark as down after 3 failed checks

    def RegisterServer(self, request, context):
        """
        RPC for cache servers to register.
        If no primary exists in the cluster or if the current primary is unreachable,
        assign this server as primary; otherwise, register as a replica.
        """
        cluster_id = request.cluster_id
        server_id = request.server_id
        port = request.port
        is_primary = False

        with self.lock:
            if cluster_id not in self.clusters:
                self.clusters[cluster_id] = {'primary': None, 'replicas': [], 'all_servers': []}
            cluster = self.clusters[cluster_id]
            
            # Add to all_servers list if not already there
            server_info = (server_id, port)
            if server_info not in cluster['all_servers']:
                cluster['all_servers'].append(server_info)
            
            # Initialize health tracking for this server
            self.server_health[port] = {
                'failed_checks': 0, 
                'last_check': time.time(),
                'offset': 0  # Add offset tracking
            }
            
            if cluster['primary'] is None:
                # No primary exists: assign the new server as primary.
                cluster['primary'] = (server_id, port)
                is_primary = True
                print(f"Sentinel: CacheServer {server_id} becomes PRIMARY for cluster {cluster_id}")
            else:
                # Primary exists - check if it's healthy according to our health tracking
                primary_port = cluster['primary'][1]
                if primary_port in self.server_health and self.server_health[primary_port]['failed_checks'] >= self.HEALTH_THRESHOLD:
                    # Existing primary is marked as down, promote this server
                    old_primary = cluster['primary']
                    cluster['primary'] = (server_id, port)
                    is_primary = True
                    print(f"Sentinel: Previous primary {old_primary[0]} in cluster {cluster_id} is down. CacheServer {server_id} becomes new PRIMARY.")
                    # Only notify if the primary was actually down
                    notify_load_balancer((server_id, port), cluster_id)
                else:
                    # Primary exists and is healthy, register as replica
                    cluster['replicas'].append((server_id, port))
                    print(f"Sentinel: CacheServer {server_id} registered as REPLICA for cluster {cluster_id}")
            
            primary_port = cluster['primary'][1] if cluster['primary'] else 0
        
        return sentinel_pb2.CacheServerRegistrationResponse(success=True, is_primary=is_primary, primary_port=primary_port)

    def GetClusterReplicas(self, request, context):
        """RPC to get all replicas for a cluster (excluding the requesting server)"""
        cluster_id = request.cluster_id
        requester_id = request.server_id
        replicas = []
        
        with self.lock:
            if cluster_id in self.clusters:
                cluster = self.clusters[cluster_id]
                # Convert all servers except the requester and current primary to replicas
                primary_id = cluster['primary'][0] if cluster['primary'] else None
                
                for server_id, port in cluster['all_servers']:
                    # Skip the requesting server and current primary
                    if server_id != requester_id and server_id != primary_id:
                        # Only include healthy servers
                        if port not in self.server_health or self.server_health[port]['failed_checks'] < self.HEALTH_THRESHOLD:
                            replicas.append({'server_id': server_id, 'port': port})
        
        return sentinel_pb2.GetClusterReplicasResponse(
            success=True,
            replicas=[sentinel_pb2.ReplicaInfo(server_id=r['server_id'], port=r['port']) for r in replicas]
        )

    def _check_server_health(self, port):
        """Performs a health check on a server and updates its health status."""
        try:
            channel = grpc.insecure_channel(f"localhost:{port}")
            stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
            
            # Check health with short timeout
            _ = stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest(), timeout=2)
            
            # Get replication offset information
            try:
                offset_response = stub.GetOffset(metadata_cache_channel_pb2.EmptyRequest(), timeout=1)
                offset = offset_response.offset
            except grpc.RpcError:
                offset = 0  # Default if offset call fails
                
            # Reset failed checks and update offset information
            with self.lock:
                if port in self.server_health:
                    self.server_health[port]['failed_checks'] = 0
                    self.server_health[port]['last_check'] = time.time()
                    self.server_health[port]['offset'] = offset
            return True
        except Exception:
            # Handle failure case (existing code)
            with self.lock:
                if port in self.server_health:
                    self.server_health[port]['failed_checks'] += 1
                    self.server_health[port]['last_check'] = time.time()
                    # Rest of existing code...
            return False

        
def run_sentinel_monitor(servicer):
    """
    Periodically checks the health of registered primary and replica servers.
    Promotes replicas if the primary fails health checks and notifies the load balancer.
    """
    print("Sentinel monitor started.")
    while True:
        time.sleep(CHECK_INTERVAL)
        
        # First perform health checks on all registered servers
        with servicer.lock:
            # Get a list of all servers to check (both primaries and replicas)
            servers_to_check = []
            for cluster_id, cluster in servicer.clusters.items():
                if cluster['primary']:
                    servers_to_check.append((cluster_id, *cluster['primary']))  # (cluster_id, server_id, port)
                for replica in cluster['replicas']:
                    servers_to_check.append((cluster_id, *replica))  # (cluster_id, server_id, port)
            
        # Check each server's health (outside the lock to avoid blocking)
        for cluster_id, server_id, port in servers_to_check:
            servicer._check_server_health(port)
            
        # Now handle any necessary failovers
        with servicer.lock:
            for cluster_id, cluster in list(servicer.clusters.items()):
                primary = cluster.get('primary')
                if primary:
                    primary_id, primary_port = primary
                    
                    # Check if primary is marked as down
                    if (primary_port in servicer.server_health and 
                        servicer.server_health[primary_port]['failed_checks'] >= servicer.HEALTH_THRESHOLD):
                        print(f"Sentinel Monitor: Primary {primary_id} in cluster {cluster_id} is down.")
                        
                        # Find all healthy replicas first
                        healthy_replicas = []
                        for i, replica in enumerate(list(cluster['replicas'])):
                            replica_id, replica_port = replica
                            if (replica_port not in servicer.server_health or
                                servicer.server_health[replica_port]['failed_checks'] < servicer.HEALTH_THRESHOLD):
                                # Get current offset (defaults to 0 if not available)
                                offset = servicer.server_health.get(replica_port, {}).get('offset', 0)
                                healthy_replicas.append((i, replica, offset))

                        new_primary = None
                        if healthy_replicas:
                            # Sort by offset in descending order (highest offset first)
                            healthy_replicas.sort(key=lambda x: x[2], reverse=True)
                            
                            # Select the replica with highest offset
                            idx, best_replica, highest_offset = healthy_replicas[0]
                            new_primary = best_replica
                            
                            print(f"Sentinel Monitor: Selected replica {new_primary[0]} with offset {highest_offset} as new PRIMARY")
                            # Remove the selected replica from the replicas list
                            cluster['replicas'].pop(idx)

                        
                        if new_primary:
                            # Promote the healthy replica
                            cluster['primary'] = new_primary
                            print(f"Sentinel Monitor: Promoted replica {new_primary[0]} to PRIMARY in cluster {cluster_id}.")
                            
                            # Notify the load balancer about the new primary
                            notify_load_balancer(new_primary, cluster_id)
                            
                            # Attempt to notify the new primary about its promotion
                            try:
                                channel = grpc.insecure_channel(f"localhost:{new_primary[1]}")
                                stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
                                stub.PromoteToPrimary(metadata_cache_channel_pb2.EmptyRequest())
                                print("Sentinel Monitor: Notified new primary about promotion!")
                            except Exception as e:
                                print(f"Sentinel Monitor: Failed to notify new primary about promotion: {e}")
                        else:
                            print(f"Sentinel Monitor: No healthy replicas available in cluster {cluster_id}. Primary remains marked as down.")

def register_with_consul(service_name="sentinel", service_port=SENTINEL_PORT):
    try:
        c = consul.Consul()
        service_id = f"{service_name}-{int(time.time())}"
        c.agent.service.register(
            name=service_name,
            service_id=service_id,
            address="127.0.0.1",
            port=service_port,
            tags=["sentinel"]
        )
        print(f"Sentinel registered with Consul as {service_id}")
    except Exception as e:
        print(f"Sentinel Consul registration failed: {e}")

def serve_sentinel_service(servicer):
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    sentinel_pb2_grpc.add_SentinelServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{SENTINEL_PORT}")
    server.start()
    print(f"Sentinel gRPC service started on port {SENTINEL_PORT}")
    server.wait_for_termination()

if __name__ == "__main__":
    register_with_consul()
    servicer = SentinelServiceServicer()
    # Start the gRPC server in a daemon thread.
    threading.Thread(target=serve_sentinel_service, args=(servicer,), daemon=True).start()
    # Run the monitor loop in the main thread.
    run_sentinel_monitor(servicer)