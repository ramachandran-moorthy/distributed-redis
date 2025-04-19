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

# Global variable for load balancer address
load_balancer_address = "localhost:70000"  # Default, will be updated from Consul

def discover_load_balancer():
    """Discover load balancer from Consul"""
    global load_balancer_address
    try:
        c = consul.Consul()
        services = c.agent.services()
        for service in services.values():
            if service.get("Service") == "load-balancer":
                address = service.get("Address", "localhost")
                port = service.get("Port", 70000)
                load_balancer_address = f"{address}:{port}"
                print(f"Gateway: Discovered load balancer at {load_balancer_address}")
                return True
        print("Gateway: No load balancer found in Consul registry")
        return False
    except Exception as e:
        print(f"Gateway: Error connecting to Consul: {e}")
        return False
    
def create_consistent_cache_key(entity, operation, data):
    """Create a consistent cache key by sorting dictionary keys"""
    # Sort the data dictionary keys to ensure consistent ordering
    if isinstance(data, dict):
        serialized_data = json.dumps(data, sort_keys=True)
    else:
        serialized_data = json.dumps(data)
    return f"{entity}:{operation}:{serialized_data}"

def build_sql_from_query(query_data):
    entity = query_data.get("entity")
    operation = query_data.get("operation")
    data = query_data.get("data", {})
    
    # Additional query options
    filters = query_data.get("filters", {})
    order_by = query_data.get("order_by", None)
    select_fields = query_data.get("select", "*")
    
    if operation == "create":
        if not data:
            raise ValueError("Create operation requires data")
            
        columns = list(data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {entity} ({', '.join(columns)}) VALUES ({placeholders})"
        params = tuple(data.values())
            
        return sql, params
        
    elif operation == "read":
        # Support for selecting specific fields
        if isinstance(select_fields, list):
            fields_str = ", ".join(select_fields)
        else:
            fields_str = select_fields
            
        sql = f"SELECT {fields_str} FROM {entity}"
        params = []
        
        # Build WHERE clause from both data and filters
        where_conditions = []
        
        # Process basic equality conditions from data
        for k, v in data.items():
            where_conditions.append(f"{k} = %s")
            params.append(v)
        
        # Process advanced filters
        if filters:
            for field, conditions in filters.items():
                if isinstance(conditions, dict):
                    for op, value in conditions.items():
                        operator_map = {
                            "eq": "=",
                            "neq": "!=",
                            "gt": ">",
                            "gte": ">=",
                            "lt": "<",
                            "lte": "<=",
                            "in": "IN",
                            "not_in": "NOT IN",
                            "like": "LIKE",
                            "ilike": "ILIKE",  # Case insensitive LIKE (PostgreSQL)
                            "contains": "LIKE",
                            "starts_with": "LIKE",
                            "ends_with": "LIKE",
                            "is_null": "IS NULL",
                            "is_not_null": "IS NOT NULL"
                        }
                        
                        sql_op = operator_map.get(op)
                        if not sql_op:
                            raise ValueError(f"Unsupported filter operator: {op}")
                            
                        # Special handling for various operators
                        if op == "in" or op == "not_in":
                            placeholders = ", ".join(["%s"] * len(value))
                            where_conditions.append(f"{field} {sql_op} ({placeholders})")
                            params.extend(value)
                        elif op == "contains":
                            where_conditions.append(f"{field} {sql_op} %s")
                            params.append(f"%{value}%")
                        elif op == "starts_with":
                            where_conditions.append(f"{field} {sql_op} %s")
                            params.append(f"{value}%")
                        elif op == "ends_with":
                            where_conditions.append(f"{field} {sql_op} %s")
                            params.append(f"%{value}")
                        elif op == "is_null" or op == "is_not_null":
                            where_conditions.append(f"{field} {sql_op}")  # No parameter needed
                        else:
                            where_conditions.append(f"{field} {sql_op} %s")
                            params.append(value)
                else:
                    # Simple equality
                    where_conditions.append(f"{field} = %s")
                    params.append(conditions)
        
        # Add WHERE clause if we have conditions
        if where_conditions:
            sql += f" WHERE {' AND '.join(where_conditions)}"
        
        # Add ORDER BY if specified
        if order_by:
            if isinstance(order_by, list):
                # Handle multiple order by fields
                order_clauses = []
                for field in order_by:
                    if isinstance(field, dict):
                        # Format: {"field": "name", "direction": "DESC"}
                        field_name = field.get("field")
                        direction = field.get("direction", "ASC").upper()
                        order_clauses.append(f"{field_name} {direction}")
                    else:
                        # Simple field name assumes ASC
                        order_clauses.append(f"{field} ASC")
                sql += f" ORDER BY {', '.join(order_clauses)}"
            else:
                # Simple string
                sql += f" ORDER BY {order_by}"
        
        return sql, tuple(params)
        
    elif operation == "update":
        if "id" in data:
            identifier = data.pop("id")
        elif query_data.get("where"):
            # Allow custom WHERE conditions for updates
            where_clause = query_data.get("where")
            where_params = query_data.get("where_params", [])
            
            if not data:
                raise ValueError("Update operation requires data to update")
                
            set_clause = ", ".join([f"{k} = %s" for k in data])
            sql = f"UPDATE {entity} SET {set_clause} WHERE {where_clause}"
            params = tuple(data.values()) + tuple(where_params)
            return sql, params
        else:
            raise ValueError("Update operation requires either an 'id' field or a 'where' clause")
        
        set_clause = ", ".join([f"{k} = %s" for k in data])
        sql = f"UPDATE {entity} SET {set_clause} WHERE id = %s"
        params = tuple(data.values()) + (identifier,)
        return sql, params
        
    elif operation == "delete":
        if data:
            conditions = " AND ".join([f"{k} = %s" for k in data])
            params = tuple(data.values())
        elif query_data.get("where"):
            # Allow custom WHERE conditions for deletes
            conditions = query_data.get("where")
            params = tuple(query_data.get("where_params", []))
        else:
            raise ValueError("Delete operation requires conditions")
            
        sql = f"DELETE FROM {entity} WHERE {conditions}"
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
        cache_key = create_consistent_cache_key(query_data.get('entity'), query_data.get('operation'), query_data.get("data", {}))
        print(f"Gateway: Using cache key {cache_key}")

        # Attempt to discover load balancer if address isn't set
        global load_balancer_address
        if load_balancer_address is None:
            discover_load_balancer()

        # Check cache via load balancer
        try:
            with grpc.insecure_channel(load_balancer_address) as cache_channel:
                cache_stub = load_balancer_pb2_grpc.CacheServiceStub(cache_channel)
                cache_resp = cache_stub.GetCachedData(load_balancer_pb2.CacheRequest(key=cache_key))
        except grpc.RpcError as e:
            print(f"Gateway: Cache service error: {e}")
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
                with grpc.insecure_channel("localhost:70200") as backend_channel:
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
            
            # After processing the query in the gateway
            operation = query_data.get('operation', 'read')  # Default to read if not specified
            entity = query_data.get('entity', '')

            # For all operations, call SetCachedData with operation type
            try:
                with grpc.insecure_channel(load_balancer_address) as cache_channel:
                    cache_stub = load_balancer_pb2_grpc.CacheServiceStub(cache_channel)
                    _ = cache_stub.SetCachedData(load_balancer_pb2.CacheSetRequest(
                        key=cache_key, 
                        value=backend_resp.result, 
                        entity=entity,
                        operation=operation))  # Include operation
            except grpc.RpcError as e:
                print(f"Gateway: Warning: Failed to communicate with cache: {e}")

            
            return gateway_pb2.GatewayResponse(response=backend_resp.result)
        
def check_backend_health():
    """Thread to check backend health by sending requests every 0.5 seconds"""
    global backend_health
    
    while True:
        try:
            with grpc.insecure_channel("localhost:70200") as channel:
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

def register_with_consul(service_name="gateway-server", service_port=60000):
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
    
    port = 60000
    server.add_insecure_port(f"[::]:{port}")
    
    # Try to discover load balancer from Consul
    global load_balancer_address
    if not discover_load_balancer():
        print("Gateway: Using default load balancer address:", load_balancer_address)
    
    try:
        register_with_consul(service_name="gateway-server", service_port=port)
    except Exception as e:
        print(f"Gateway: Warning: Consul registration failed: {e}")
    
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