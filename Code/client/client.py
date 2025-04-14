import grpc
import sys
import os

# Add the grpc folder to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../grpc')))

import metadata_cache_channel_pb2
import metadata_cache_channel_pb2_grpc

def send_query(query):
    try:
        with grpc.insecure_channel('localhost:50050') as channel:
            print(f"Client: Creating channel to MetadataServer at localhost:50050")
            stub = metadata_cache_channel_pb2_grpc.MetadataServiceStub(channel)
            response = stub.ProcessQuery(metadata_cache_channel_pb2.QueryRequest(query=query))
            print(f"Client: Sent query '{query}', received response '{response.reply}'")
    except grpc.RpcError as e:
        print(f"Client: Failed to connect to MetadataServer: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python client.py <query>")
        sys.exit(1)
    
    query = sys.argv[1]
    send_query(query)