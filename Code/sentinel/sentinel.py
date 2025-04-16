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

# List of cache servers by cluster
# We'll use a dictionary mapping cluster_id to a structure with 'primary' and a list of 'replicas'
registered_cache_servers = {}  # { cluster_id: { "primary": {server_id, port}, "replicas": [{server_id, port}, ...] } }

CHECK_INTERVAL = 5  # seconds
SENTINEL_PORT = 50060  # gRPC port for the SentinelService

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

class SentinelServiceServicer(sentinel_pb2_grpc.SentinelServiceServicer):
    def RegisterCacheServer(self, request, context):
        cluster_id = request.cluster_id
        server_info = {"server_id": request.server_id, "port": request.port}
        is_primary = False

        # Check if there's an existing primary and if it's alive.
        primary_exists = cluster_id in registered_cache_servers and "primary" in registered_cache_servers[cluster_id]
        if primary_exists:
            existing_primary = registered_cache_servers[cluster_id]["primary"]
            try:
                with grpc.insecure_channel(f"localhost:{existing_primary['port']}") as channel:
                    stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
                    # Use a short timeout health-check call.
                    _ = stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest(), timeout=2)
                # If the call succeeds, keep existing primary.
                registered_cache_servers[cluster_id]["replicas"].append(server_info)
                print(f"Sentinel: CacheServer {request.server_id} registered as REPLICA in cluster {cluster_id}.")
            except Exception as e:
                # Existing primary is down; assign this new one as primary.
                registered_cache_servers[cluster_id] = {"primary": server_info, "replicas": []}
                is_primary = True
                print(f"Sentinel: Existing primary unreachable; CacheServer {request.server_id} becomes PRIMARY in cluster {cluster_id}.")
        else:
            # No primary registered yet for this cluster.
            registered_cache_servers[cluster_id] = {"primary": server_info, "replicas": []}
            is_primary = True
            print(f"Sentinel: CacheServer {request.server_id} becomes PRIMARY in cluster {cluster_id}.")

        return sentinel_pb2.CacheServerRegistrationResponse(success=True, is_primary=is_primary)

    def GetPrimaryForCluster(self, request, context):
        cluster_id = request.cluster_id
        if cluster_id in registered_cache_servers and "primary" in registered_cache_servers[cluster_id]:
            primary = registered_cache_servers[cluster_id]["primary"]
            print(f"Sentinel: Primary for cluster {cluster_id} is {primary['server_id']} on port {primary['port']}")
            return sentinel_pb2.PrimaryForClusterResponse(found=True, server_id=primary["server_id"], port=primary["port"])
        else:
            print(f"Sentinel: No primary found for cluster {cluster_id}")
            return sentinel_pb2.PrimaryForClusterResponse(found=False, server_id=0, port=0)
        

def notify_load_balancer(new_primary, cluster_id, load_balancer_address="localhost", load_balancer_port=50053):
    try:
        channel = grpc.insecure_channel(f"{load_balancer_address}:{load_balancer_port}")
        stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
        request = metadata_cache_channel_pb2.NotifyPromotionRequest(
            server_id=new_primary["server_id"],
            port=new_primary["port"],
            cluster_id=cluster_id
        )
        response = stub.NotifyPromotion(request)
        if response.success:
            print(f"Sentinel: Notified load balancer about new primary {new_primary['server_id']} in cluster {cluster_id}.")
        else:
            print("Sentinel: Load balancer failed to acknowledge promotion.")
    except Exception as e:
        print(f"Sentinel: Error notifying load balancer: {e}")


def run_sentinel_monitor():
    print("Sentinel monitor started.")
    while True:
        time.sleep(CHECK_INTERVAL)
        for cluster_id, servers in list(registered_cache_servers.items()):
            primary = servers.get("primary")
            if primary:
                try:
                    # Attempt a simple health check RPC, for example, GetLoad.
                    with grpc.insecure_channel(f"localhost:{primary['port']}") as channel:
                        stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
                        # We'll use GetLoad as a proxy for health; you can choose a dedicated health check method.
                        load_response = stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest(), timeout=2)
                        # If the call succeeds, we assume the primary is alive.
                        print(f"Sentinel: Primary CacheServer {primary['server_id']} in cluster {cluster_id} is alive (load: {load_response.load}).")
                except Exception as e:
                    print(f"Sentinel: Primary CacheServer {primary['server_id']} in cluster {cluster_id} is unreachable or down: {e}")
                    # Mark primary as stale; if replicas exist, promote one.
                    if servers.get("replicas"):
                        new_primary = servers["replicas"].pop(0)
                        servers["primary"] = new_primary
                        print(f"Sentinel: Promoted CacheServer {new_primary['server_id']} to PRIMARY in cluster {cluster_id}.")
                        notify_load_balancer(new_primary, cluster_id)
                    else:
                        print(f"Sentinel: No replicas available in cluster {cluster_id}. Removing stale primary.")
                        # Remove the stale registration
                        del registered_cache_servers[cluster_id]
            else:
                print(f"Sentinel: No primary registered for cluster {cluster_id}.")

def serve_sentinel_service():
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
    sentinel_pb2_grpc.add_SentinelServiceServicer_to_server(SentinelServiceServicer(), server)
    server.add_insecure_port(f"[::]:{SENTINEL_PORT}")
    server.start()
    print(f"Sentinel gRPC service started on port {SENTINEL_PORT}")
    server.wait_for_termination()

if __name__ == "__main__":
    # Register sentinel with Consul immediately on start.
    register_with_consul()
    
    # Start the sentinel gRPC service in a separate thread.
    threading.Thread(target=serve_sentinel_service, daemon=True).start()
    
    # Run the existing sentinel monitoring logic.
    run_sentinel_monitor()