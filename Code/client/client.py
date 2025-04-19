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

def build_query(entity, operation, data):
    return json.dumps({
        "entity": entity,
        "operation": operation,
        "data": data
    })

def run(entity, operation, data=None):
    address, port = discover_gateway_server()
    if not address:
        print("Gateway server not found in Consul.")
        return
    
    if data is None:
        data = {}
    
    try:
        target = f"{address}:{port}"
        with grpc.insecure_channel(target) as channel:
            stub = gateway_pb2_grpc.GatewayServiceStub(channel)
            
            json_query = build_query(entity, operation, data)
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
    """Print usage information for the client"""
    print("Database Client Usage:")
    print("\npython client.py <entity> <operation> [data]")
    print("\nOperations:")
    print("  create - Create a new record")
    print("    Example: python client.py student create '{\"first_name\": \"John\", \"last_name\": \"Doe\", \"program\": \"Computer Science\"}'")
    print("\n  read - Read records (with optional filters)")
    print("    Example: python client.py student read '{\"program\": \"Chemistry\", \"last_name\": \"Chong\"}'")
    print("    Example: python client.py student read  # Returns all records")
    print("\n  update - Update records (first field is the filter)")
    print("    Example: python client.py student update '{\"id\": 5, \"last_name\": \"Smith\", \"program\": \"Physics\"}'")
    print("    Note: The first field (id in this example) is used as the filter, all other fields will be updated")
    print("\n  delete - Delete records")
    print("    Example: python client.py student delete '{\"id\": 5}'")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(1)
    
    entity = sys.argv[1]
    operation = sys.argv[2]
    data = {}
    
    # Try to parse data as JSON if provided
    if len(sys.argv) >= 4:
        try:
            data = json.loads(sys.argv[3])
        except json.JSONDecodeError:
            print("Error: Unable to parse data as JSON. Make sure it's properly formatted.")
            print("Example: '{\"id\": 5, \"name\": \"John\"}'")
            sys.exit(1)
    
    run(entity, operation, data)
