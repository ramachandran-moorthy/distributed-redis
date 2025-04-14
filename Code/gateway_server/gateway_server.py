import sys
from os import path

import json
import grpc
from concurrent import futures
import time
import consul
import threading
from datetime import datetime

sys.path.append(path.abspath(path.join(path.dirname(__file__), '../grpc')))

import gateway_pb2, gateway_pb2_grpc
import load_balancer_pb2, load_balancer_pb2_grpc
import backend_pb2, backend_pb2_grpc
import heartbeat_pb2, heartbeat_pb2_grpc

# Global variable to track backend health status
backend_health = {
    "last_check": datetime.now(),
    "is_healthy": True
}
health_lock = threading.Lock()

def build_sql_from_query(query_data):
    entity = query_data.get("entity")
    operation = query_data.get("operation")
    data = query_data.get("data", {})

    if operation == "create":
        columns = list(data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {entity} ({', '.join(columns)}) VALUES ({placeholders})"
        params = tuple(data.values())
        return sql, params
    elif operation == "read":
        conditions = " AND ".join([f"{k} = %s" for k in data])
        sql = f"SELECT * FROM {entity}"
        params = tuple()
        if conditions:
            sql += f" WHERE {conditions}"
            params = tuple(data.values())
        return sql, params
    elif operation == "update":
        identifier = data.pop("id", None)
        if not identifier:
            raise ValueError("Update operation requires an 'id' field")
        set_clause = ", ".join([f"{k} = %s" for k in data])
        sql = f"UPDATE {entity} SET {set_clause} WHERE id = %s"
        params = tuple(data.values()) + (identifier,)
        return sql, params
    elif operation == "delete":
        conditions = " AND ".join([f"{k} = %s" for k in data])
        if not conditions:
            raise ValueError("Delete operation requires at least one condition")
        sql = f"DELETE FROM {entity} WHERE {conditions}"
        params = tuple(data.values())
        return sql, params
    else:
        raise ValueError(f"Unsupported operation: {operation}")

class GatewayService(gateway_pb2_grpc.GatewayServiceServicer):
    def ProcessQuery(self, request, context):
        try:
            query_data = json.loads(request.json_query)
        except Exception as e:
            err = {"error": "Invalid JSON", "details": str(e)}
            return gateway_pb2.GatewayResponse(response=json.dumps(err))
        
        # Check backend health status before proceeding
        global backend_health
        with health_lock:
            is_backend_healthy = backend_health["is_healthy"]
        
        # Build a cache key using the entity, operation, and data.
        cache_key = f"{query_data.get('entity')}:{query_data.get('operation')}:" + json.dumps(query_data.get("data", {}))
        print(f"Gateway: Using cache key {cache_key}")

        try:
            # Check cache via load balancer.
            with grpc.insecure_channel("localhost:50053") as cache_channel:
                cache_stub = load_balancer_pb2_grpc.CacheServiceStub(cache_channel)
                cache_resp = cache_stub.GetCachedData(load_balancer_pb2.CacheRequest(key=cache_key))
        except grpc.RpcError as e:
            print(f"Cache service error: {e}")
            if not is_backend_healthy:
                return gateway_pb2.GatewayResponse(response=json.dumps({
                    "error": "Service unavailable",
                    "details": "Cache service is down and backend server is unhealthy"
                }))
            # Continue to backend if cache is down but backend is healthy
            cache_resp = type('obj', (object,), {'found': False})
        
        if cache_resp.found:
            print("Gateway: Cache hit.")
            return gateway_pb2.GatewayResponse(response=cache_resp.value)
        else:
            print("Gateway: Cache miss. Building SQL query.")
            
            # If cache miss and backend is down, return error
            if not is_backend_healthy:
                return gateway_pb2.GatewayResponse(response=json.dumps({
                    "error": "Service unavailable", 
                    "details": "Backend server is down and data is not in cache"
                }))
                
            try:
                sql_query, params = build_sql_from_query(query_data)
            except ValueError as ve:
                return gateway_pb2.GatewayResponse(response=json.dumps({"error": str(ve)}))
            
            # Call backend server via gRPC.
            try:
                with grpc.insecure_channel("localhost:50055") as backend_channel:
                    backend_stub = backend_pb2_grpc.BackendServiceStub(backend_channel)
                    backend_resp = backend_stub.ExecuteSQL(
                        backend_pb2.BackendRequest(sql_query=sql_query, params=json.dumps({"params": params}))
                    )
            except grpc.RpcError as e:
                # Mark backend as unhealthy if request fails
                with health_lock:
                    backend_health["is_healthy"] = False
                return gateway_pb2.GatewayResponse(response=json.dumps({
                    "error": "Backend service error", 
                    "details": str(e)
                }))
            
            # Try to cache the result, but don't fail if cache is down
            try:
                with grpc.insecure_channel("localhost:50053") as cache_channel:
                    cache_stub = load_balancer_pb2_grpc.CacheServiceStub(cache_channel)
                    _ = cache_stub.SetCachedData(load_balancer_pb2.CacheSetRequest(key=cache_key, value=backend_resp.result))
            except grpc.RpcError as e:
                print(f"Warning: Failed to cache result: {e}")
            
            return gateway_pb2.GatewayResponse(response=backend_resp.result)

def check_backend_health():
    """Thread to check backend health by sending requests every 0.5 seconds"""
    global backend_health
    
    while True:
        try:
            with grpc.insecure_channel("localhost:50055") as channel:
                stub = heartbeat_pb2_grpc.HeartbeatServiceStub(channel)
                request = heartbeat_pb2.HeartbeatRequest(
                    gateway_id=f"gateway-{int(time.time())}"
                )
                
                # Set a shorter timeout for the health check (200ms)
                response = stub.CheckHealth(request, timeout=0.2)
                
                # Update health status to healthy
                with health_lock:
                    backend_health["last_check"] = datetime.now()
                    if not backend_health["is_healthy"]:
                        print("Backend is now healthy!")
                    backend_health["is_healthy"] = True
                    
        except grpc.RpcError as e:
            # Update health status to unhealthy
            with health_lock:
                now = datetime.now()
                last_check = backend_health["last_check"]
                time_diff = (now - last_check).total_seconds()
                
                if backend_health["is_healthy"]:
                    print(f"Backend is down! Health check failed with error: {e}")
                backend_health["is_healthy"] = False
                
        except Exception as e:
            print(f"Unexpected error in health check: {e}")
        
        time.sleep(0.5)

def register_with_consul(service_name="gateway-server", service_port=50051):
    try:
        c = consul.Consul()
        service_id = f"{service_name}-{int(time.time())}"
        c.agent.service.register(
            name=service_name,
            service_id=service_id,
            address="127.0.0.1",
            port=service_port,
            tags=["gateway"]
        )
        print(f"Gateway registered with Consul as {service_id}")
    except Exception as e:
        print(f"Failed to register with Consul: {e}")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    gateway_pb2_grpc.add_GatewayServiceServicer_to_server(GatewayService(), server)
    
    port = 50051
    server.add_insecure_port(f"[::]:{port}")
    
    try:
        register_with_consul(service_name="gateway-server", service_port=port)
    except Exception as e:
        print(f"Warning: Consul registration failed: {e}")
    
    # Start health check thread
    health_thread = threading.Thread(target=check_backend_health, daemon=True)
    health_thread.start()
    
    server.start()
    print(f"Gateway server started on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()