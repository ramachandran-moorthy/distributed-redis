import grpc
import time
import sys
import os

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc

# List of cache servers by cluster
CLUSTERS = {
    0: {0: 50051, 1: 50052, 2: 50053},
    1: {3: 50054, 4: 50055, 5: 50056}
}

CHECK_INTERVAL = 5  # seconds
METADATA_SERVER_ADDRESS = 'localhost:50050'

def is_alive(stub):
    try:
        stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest())
        return True
    except grpc.RpcError:
        return False

def get_primary(cluster):
    for server_id, port in cluster.items():
        try:
            with grpc.insecure_channel(f'localhost:{port}') as channel:
                stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
                response = stub.GetRole(metadata_cache_channel_pb2.EmptyRequest())
                if response.is_primary:
                    print(f"Sentinel: CacheServer {server_id} is currently PRIMARY in its cluster.")
                    return server_id
        except grpc.RpcError:
            print(f"Sentinel: CacheServer {server_id} (port {port}) is DOWN.")
    return None

def get_most_updated_replica(cluster):
    most_keys = -1
    selected_server = None
    for server_id, port in cluster.items():
        try:
            with grpc.insecure_channel(f'localhost:{port}') as channel:
                stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
                role = stub.GetRole(metadata_cache_channel_pb2.EmptyRequest())
                if not role.is_primary:
                    load_response = stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest())
                    if load_response.load > most_keys:
                        most_keys = load_response.load
                        selected_server = (server_id, port)
        except grpc.RpcError:
            print(f"Sentinel: Could not reach CacheServer {server_id} for load check.")
    return selected_server

def notify_metadata_of_promotion(server_id, port, cluster_id):
    try:
        with grpc.insecure_channel(METADATA_SERVER_ADDRESS) as channel:
            stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
            response = stub.NotifyPromotion(
                metadata_cache_channel_pb2.NotifyPromotionRequest(
                    server_id=server_id, port=port, cluster_id=cluster_id
                )
            )
            if response.success:
                print(f"Sentinel: Metadata server notified of promotion for CacheServer {server_id} in cluster {cluster_id}.")
            else:
                print(f"Sentinel: Metadata server failed to register promotion for CacheServer {server_id}.")
    except grpc.RpcError as e:
        print(f"Sentinel: Failed to notify metadata server: {e}")

def promote(server_id, server_port, cluster_id):
    try:
        channel = grpc.insecure_channel(f'localhost:{server_port}')
        stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
        response = stub.PromoteToPrimary(metadata_cache_channel_pb2.EmptyRequest())
        if response.success:
            print(f"Sentinel: CacheServer {server_id} promoted to PRIMARY")
            notify_metadata_of_promotion(server_id, server_port, cluster_id)
        else:
            print(f"Sentinel: Promotion of CacheServer {server_id} failed")
    except grpc.RpcError as e:
        print(f"Sentinel: Error promoting CacheServer {server_id}: {e}")

def run_sentinel():
    print("Sentinel started.")
    current_primaries = {}

    for cluster_id, cluster in CLUSTERS.items():
        current_primary = get_primary(cluster)
        current_primaries[cluster_id] = current_primary

    while True:
        time.sleep(CHECK_INTERVAL)

        for cluster_id, cluster in CLUSTERS.items():
            current_primary = current_primaries.get(cluster_id)
            still_alive = False

            if current_primary is not None:
                try:
                    with grpc.insecure_channel(f'localhost:{cluster[current_primary]}') as channel:
                        stub = metadata_cache_channel_pb2_grpc.CacheServiceStub(channel)
                        stub.GetLoad(metadata_cache_channel_pb2.EmptyRequest())
                        still_alive = True
                except grpc.RpcError:
                    print(f"Sentinel: Primary CacheServer {current_primary} in cluster {cluster_id} is DOWN!")

            if not still_alive:
                print(f"Sentinel: Electing new primary for cluster {cluster_id}...")
                new_primary = get_most_updated_replica(cluster)
                if new_primary:
                    server_id, server_port = new_primary
                    promote(server_id, server_port, cluster_id)
                    current_primaries[cluster_id] = server_id
                else:
                    print(f"Sentinel: No replicas available to promote in cluster {cluster_id}!")

if __name__ == "__main__":
    run_sentinel()
