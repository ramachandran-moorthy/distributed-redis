import threading
import time
import sys
import os
import json
import random
import importlib.util
from concurrent.futures import ThreadPoolExecutor
import subprocess
import signal
import statistics

# Global flag for graceful shutdown
running = True

def signal_handler(sig, frame):
    global running
    print("\nReceived shutdown signal. Preparing to exit...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

# Dynamic module importing
def import_module(file_path, module_name):
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None:
            print(f"Could not find module {module_name} at {file_path}")
            return None
        
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        print(f"Error importing {module_name} from {file_path}: {e}")
        return None

# Thread functions for each component
def run_sentinel(sentinel_module):
    print("Starting Sentinel thread...")
    try:
        # Create a servicer
        servicer = sentinel_module.SentinelServiceServicer()
        # Register with consul
        sentinel_module.register_with_consul()
        # Start the gRPC server
        server = sentinel_module.grpc.server(sentinel_module.concurrent.futures.ThreadPoolExecutor(max_workers=10))
        sentinel_module.sentinel_pb2_grpc.add_SentinelServiceServicer_to_server(servicer, server)
        server.add_insecure_port(f"[::]:{sentinel_module.SENTINEL_PORT}")
        server.start()
        print(f"Sentinel server started on port {sentinel_module.SENTINEL_PORT}")
        
        # Run the monitor thread
        monitor_thread = threading.Thread(target=sentinel_module.run_sentinel_monitor, args=(servicer,))
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # Wait for termination
        while running:
            time.sleep(1)
    except Exception as e:
        print(f"Error in sentinel thread: {e}")

def run_load_balancer(lb_module):
    print("Starting Load Balancer thread...")
    try:
        # Create and start the server
        server = lb_module.grpc.server(lb_module.futures.ThreadPoolExecutor(max_workers=10))
        service = lb_module.LoadBalancerService()
        lb_module.metadata_cache_channel_pb2_grpc.add_MetadataServiceServicer_to_server(service, server)
        lb_module.load_balancer_pb2_grpc.add_CacheServiceServicer_to_server(service, server)
        port = 70000
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        
        # Register with Consul
        lb_module.register_with_consul(port)
        print(f"LoadBalancer started on port {port}")
        
        # Wait for termination
        while running:
            time.sleep(1)
    except Exception as e:
        print(f"Error in load balancer thread: {e}")

def run_gateway(gateway_module):
    print("Starting Gateway thread...")
    try:
        # Create and start the server
        server = gateway_module.grpc.server(gateway_module.futures.ThreadPoolExecutor(max_workers=10))
        gateway_module.gateway_pb2_grpc.add_GatewayServiceServicer_to_server(
            gateway_module.GatewayService(), server)
        port = 60000
        server.add_insecure_port(f"[::]:{port}")
        
        # Try to discover load balancer
        if not gateway_module.discover_load_balancer():
            print("Gateway: Using default load balancer address")
        
        # Register with Consul
        try:
            gateway_module.register_with_consul(service_name="gateway-server", service_port=port)
        except Exception as e:
            print(f"Gateway: Warning: Consul registration failed: {e}")
        
        # Start health check thread
        health_thread = threading.Thread(target=gateway_module.check_backend_health, daemon=True)
        health_thread.start()
        
        server.start()
        print(f"Gateway server started on port {port}")
        
        # Wait for termination
        while running:
            time.sleep(1)
        
        with gateway_module.latency_lock:
            if gateway_module.request_latencies:
                avg_latency = statistics.mean(gateway_module.request_latencies)
                print(f"\nGateway: Average request latency: {avg_latency:.6f} seconds")
                print(f"Gateway: Total requests processed: {len(gateway_module.request_latencies)}")
            else:
                print("\nGateway: No requests were processed")
            
            print(f"Gateway: Total cache hits: {gateway_module.cache_hits}")
        
    except Exception as e:
        print(f"Error in gateway thread: {e}")

def run_backend(backend_module):
    print("Starting Backend thread...")
    try:
        # Create and start the server
        server = backend_module.grpc.server(backend_module.futures.ThreadPoolExecutor(max_workers=20))
        backend_module.backend_pb2_grpc.add_BackendServiceServicer_to_server(
            backend_module.BackendService(), server)
        backend_module.heartbeat_pb2_grpc.add_HeartbeatServiceServicer_to_server(
            backend_module.HeartbeatService(), server)
        server.add_insecure_port('[::]:70200')
        server.start()
        print(f"Backend server {backend_module.BACKEND_ID} listening on port 70200")
        
        # Wait for termination
        while running:
            time.sleep(1)
    except Exception as e:
        print(f"Error in backend thread: {e}")

def run_cache_server(cache_module, server_id, ack_policy=0):
    print(f"Starting Cache Server {server_id} thread...")
    try:
        # Determine server parameters
        cluster_id = server_id // 4
        port = 50051 + server_id
        
        # Register with the sentinel to determine role
        assigned_as_primary, sentinel_primary_port = cache_module.register_with_sentinel(
            server_id, port, cluster_id)
        
        # If we're a replica and primary_port wasn't specified, use the one from sentinel
        primary_port = None
        if not assigned_as_primary:
            primary_port = sentinel_primary_port
            if primary_port == 0 or primary_port is None:
                print(f"CacheServer {server_id}: No primary port provided by Sentinel.")
                return
        
        # Create and configure the cache server instance
        cache_server = cache_module.CacheServer(
            server_id, is_primary=assigned_as_primary, cluster_id=cluster_id, ack_policy=ack_policy)
        cache_server.server_port = port
        
        # Create and start the gRPC server
        server = cache_module.grpc.server(cache_module.futures.ThreadPoolExecutor(max_workers=10))
        cache_module.metadata_cache_channel_pb2_grpc.add_CacheServiceServicer_to_server(cache_server, server)
        server.add_insecure_port(f'[::]:{port}')
        server.start()
        
        # Register with appropriate services based on role
        if assigned_as_primary:
            cache_module.register_with_load_balancer(server_id, port, cluster_id)
            threading.Thread(target=cache_server.replica_handler, daemon=True).start()
            print(f"CacheServer {server_id}: Started as PRIMARY")
        else:
            print(f"CacheServer {server_id}: Operating as REPLICA. Registering with primary...")
            cache_module.register_with_primary(server_id, port, primary_port, cache_server)
        
        print(f"CacheServer {server_id} started on port {port}")
        
        # Wait for termination
        while running:
            time.sleep(1)
    except Exception as e:
        print(f"Error in cache server {server_id} thread: {e}")

def run_client(client_id, operation_type="read"):
    """
    Run a client with the specified operation type
    
    Args:
        client_id: Unique identifier for this client
        operation_type: One of "read", "create", "update", "delete"
    """
    print(f"Client {client_id} sending {operation_type} query...")
    
    # Generate a student ID based on client_id to ensure consistency
    student_id = client_id % 100 + 1  # Cycle through 100 student IDs
    
    # Prepare query data based on operation type
    if operation_type == "read":
        query_data = json.dumps({"student_id": student_id})
    elif operation_type == "create":
        query_data = json.dumps({
            "student_id": student_id,
            "first_name": f"Student_{client_id}",
            "program": f"Dept_{random.randint(1, 5)}",
            "admission_year": random.randint(1, 4)
        })
    elif operation_type == "update":
        query_data = json.dumps({
            "student_id": student_id,
            "first_name": f"Updated_Student_{client_id}",
            "program": f"Dept_{random.randint(1, 5)}",
            "admission_year": random.randint(1, 4)
        })
    elif operation_type == "delete":
        query_data = json.dumps({"student_id": student_id})
    else:
        print(f"Unknown operation type: {operation_type}")
        return None
    
    try:
        result = subprocess.run(
            [sys.executable, "Code/client/client.py", "student", operation_type, query_data],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            print(f"Client {client_id} completed {operation_type} successfully")
        else:
            print(f"Client {client_id} failed {operation_type} with error: {result.stderr}")
        return result
    except subprocess.TimeoutExpired:
        print(f"Client {client_id} {operation_type} timed out")
        return None

def main():
    # Redirect all output to a file
    log_file = open("test_all_ops.log", "w")
    original_stdout = sys.stdout
    sys.stdout = log_file
    
    print("=== Distributed System Test Script ===")
    print(f"Test started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    print("=== Distributed System Test Script ===")
    
    # Load modules
    print("\nLoading modules...")
    sentinel_module = import_module("Code/sentinel/sentinel.py", "sentinel")
    lb_module = import_module("Code/load_balancer/load_balancer.py", "load_balancer")
    gateway_module = import_module("Code/gateway_server/gateway_server.py", "gateway_server")
    backend_module = import_module("Code/backend_server/backend_server.py", "backend_server")
    cache_module = import_module("Code/cache_server/cache_server.py", "cache_server")
    
    if None in [sentinel_module, lb_module, gateway_module, backend_module, cache_module]:
        print("Failed to load all required modules. Exiting.")
        return
    
    # Start components in threads
    threads = []
    
    # Start Sentinel
    print("\n=== Starting Components ===")
    sentinel_thread = threading.Thread(target=run_sentinel, args=(sentinel_module,))
    sentinel_thread.daemon = True
    sentinel_thread.start()
    threads.append(("Sentinel", sentinel_thread))
    time.sleep(2)
    
    # Start Load Balancer
    lb_thread = threading.Thread(target=run_load_balancer, args=(lb_module,))
    lb_thread.daemon = True
    lb_thread.start()
    threads.append(("Load Balancer", lb_thread))
    time.sleep(2)
    
    # Start Gateway
    # gateway_thread = threading.Thread(target=run_gateway, args=(gateway_module,))
    # gateway_thread.daemon = True
    # gateway_thread.start()
    # threads.append(("Gateway", gateway_thread))
    # time.sleep(2)
    
    # Start Backend
    backend_thread = threading.Thread(target=run_backend, args=(backend_module,))
    backend_thread.daemon = True
    backend_thread.start()
    threads.append(("Backend", backend_thread))
    time.sleep(2)
    
    # Start Cache Servers
    for i in range(12):  # Cache servers 0-11
        cache_thread = threading.Thread(target=run_cache_server, args=(cache_module, i, 0))
        cache_thread.daemon = True
        cache_thread.start()
        threads.append((f"Cache Server {i}", cache_thread))
        time.sleep(1)
    
    print("\n=== System Initialization Complete ===")
    print("Waiting 10 seconds for system stabilization...\n")
    time.sleep(10)
    
    # Create a distribution of operations
    total_clients = 400
    operations = ["read"] * total_clients  # Start with all reads
    
    # Generate random indices for create, update, and delete operations
    indices = list(range(total_clients))
    random.shuffle(indices)
    
    # Assign 10 create, 10 update, and 10 delete operations
    create_indices = indices[:10]
    update_indices = indices[10:20]
    delete_indices = indices[20:30]
    
    for idx in create_indices:
        operations[idx] = "create"
    
    for idx in update_indices:
        operations[idx] = "update"
    
    for idx in delete_indices:
        operations[idx] = "delete"
    
    # Print operation distribution statistics
    op_count = {"create": 0, "read": 0, "update": 0, "delete": 0}
    for op in operations:
        op_count[op] += 1
    
    print("=== Operation Distribution ===")
    for op, count in op_count.items():
        print(f"  {op.upper()} operations: {count}")
    print()
    
    # Spawn 400 clients, 100 at a time with 5-second gaps
    print("\n=== Running Client Load Test ===")
    total_clients_processed = 0
    batch_size = 100
    
    for batch in range(4):  # 4 batches of 100 = 400 clients
        print(f"\nSpawning client batch {batch+1}/4 (clients {total_clients_processed+1}-{total_clients_processed+batch_size})...")
        
        # Count operations in this batch
        batch_ops = operations[total_clients_processed:total_clients_processed+batch_size]
        batch_op_count = {"create": 0, "read": 0, "update": 0, "delete": 0}
        for op in batch_ops:
            batch_op_count[op] += 1
        
        print(f"  This batch contains: CREATE: {batch_op_count['create']}, UPDATE: {batch_op_count['update']}, DELETE: {batch_op_count['delete']}, READ: {batch_op_count['read']}")
        
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # Submit client tasks
            futures = []
            for i in range(batch_size):
                client_id = total_clients_processed + i
                operation = operations[client_id]
                futures.append(executor.submit(run_client, client_id, operation))
            
            # Wait for all clients in this batch to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Client error: {e}")
        
        total_clients_processed += batch_size
        print(f"Batch {batch+1}/4 completed. Total clients processed: {total_clients_processed}")
        
        # Wait 5 seconds before next batch (except after the last batch)
        if batch < 3:
            print(f"Waiting 5 seconds before spawning batch {batch+2}...")
            time.sleep(5)
    
    print("\n=== Client Load Test Complete ===")
    print(f"All {total_clients_processed} client operations completed!")
    print(f"Final operation count: CREATE: {op_count['create']}, UPDATE: {op_count['update']}, DELETE: {op_count['delete']}, READ: {op_count['read']}")

    # Signal all components to shut down by setting the running flag to False
    print("\n=== Shutting Down Components ===")
    global running
    running = False
    
    # Give time for the components to print their stats and shut down gracefully
    print("Waiting for components to shut down gracefully...")
    time.sleep(5)

    sys.stdout = original_stdout
    log_file.close()
    print("\nShutdown complete. Results written to log file")

if __name__ == "__main__":
    main()
