import sys
from os import path

import consul
import grpc
import json
import sys
import time

sys.path.append(path.abspath(path.join(path.dirname(__file__), '../grpc')))

import gateway_pb2, gateway_pb2_grpc

def discover_gateway_server(consul_host="127.0.0.1", consul_port=8500):
    """
    Discover the gateway server registered in Consul.
    Returns (address, port) if found, otherwise (None, None).
    """
    try:
        c = consul.Consul(host=consul_host, port=consul_port)
        services = c.agent.services()
        for service in services.values():
            # Look for the service with name "gateway-server"
            if service.get("Service") == "gateway-server":
                address = service.get("Address")
                port = service.get("Port")
                print(f"Discovered gateway server at {address}:{port}")
                return address, port
        print("No gateway server found in Consul registry")
        return {"error": "No gateway server found in Consul registry"}
    except Exception as e:
        print(f"Error connecting to Consul: {e}")
        return {"error": f"Error connecting to Consul: {e}"}

def build_query(entity, operation, **kwargs):
    return json.dumps({
        "entity": entity,
        "operation": operation,
        "data": kwargs
    })

def run(entity, operation, **kwargs):
    address, port = discover_gateway_server()
    if not address:
        print("Gateway server not found in Consul.")
        return
    
    try:
        target = f"{address}:{port}"
        with grpc.insecure_channel(target) as channel:
            stub = gateway_pb2_grpc.GatewayServiceStub(channel)
            
            json_query = build_query(entity, operation, **kwargs)
            print(f"Sending query: {json_query}")
            
            request = gateway_pb2.GatewayRequest(json_query=json_query)
            response = stub.ProcessQuery(request)
            
            try:
                # Try to prettify JSON response
                result = json.loads(response.response)
                print("\nResponse from gateway server:")
                print(json.dumps(result, indent=2))
                return result
            except json.JSONDecodeError:
                # If not valid JSON, print as-is
                print("\nResponse from gateway server:", response.response)
                return response.response
                
    except grpc.RpcError as e:
        print(f"Failed to connect to gateway server with gRPC error: {e}")
        return {"error": f"Failed to connect to gateway server: {str(e)}"}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"error": f"Unexpected error: {str(e)}"}

def print_usage():
    print("""
    Client Usage Guide:
    ------------------
    This client interfaces with the distributed database system through the gateway server.
    
    Basic Operations:
    ----------------
    1. Create a record:
       python client.py create <entity> '{"field1": "value1", "field2": "value2", ...}'
       
    2. Read records:
       python client.py read <entity> '{"field1": "value1"}'
       python client.py read <entity> '{"data": {"status": "active"}, "filters": {"age": {"gt": 21}}}'
       python client.py read <entity> '{"select": ["id", "name", "email"]}'
       python client.py read <entity> '{"order_by": [{"field": "last_name", "direction": "DESC"}]}'

       
    3. Update a record:
       python client.py update <entity> '{"id": 1, "field1": "new_value"}'
       python client.py update <entity> '{"data": {"status": "inactive"}, "where": "last_login < %s", "where_params": ["2023-01-01"]}'
       
    4. Delete a record:
       python client.py delete <entity> '{"id": 1}'
        
    Filter Operators:
    ---------------
    - "eq": Equal to (=)
    - "neq": Not equal to (!=)
    - "gt": Greater than (>)
    - "gte": Greater than or equal to (>=)
    - "lt": Less than (<)
    - "lte": Less than or equal to (<=)
    - "in": In a list of values (IN)
    - "not_in": Not in a list of values (NOT IN)
    - "like": Pattern matching with wildcards (LIKE)
    - "ilike": Case-insensitive pattern matching
    - "contains": Contains substring (LIKE %value%)
    - "starts_with": Starts with (LIKE value%)
    - "ends_with": Ends with (LIKE %value)
    - "is_null": Is NULL
    - "is_not_null": Is NOT NULL
    
    Example filter usage:
    python client.py read users '{"filters": {"name": {"like": "%John%"}, "age": {"gt": 25, "lt": 65}}}'
    """)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)
    
    entity = sys.argv[1]
    operation = sys.argv[2]
    data = {}
    
    # Parse key=value arguments
    for arg in sys.argv[3:]:
        if "=" in arg:
            key, value = arg.split("=", 1)
            # Try to convert to appropriate type
            if value.isdigit():
                value = int(value)
            elif value.lower() == "true":
                value = True
            elif value.lower() == "false":
                value = False
            # Remove quotes if present
            elif value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            data[key] = value
    
    run(entity, operation, **data)