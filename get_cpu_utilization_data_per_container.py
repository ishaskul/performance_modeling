import json
import csv
import os
import glob
from datetime import datetime

# Input and output directories
input_dir = '1/system_cpu_data'
output_base_dir = 'cpu_util_data_per_container'

# Ensure the base output directory exists
os.makedirs(output_base_dir, exist_ok=True)

# Services to extract
relevant_services = [
    "account-service", "billing-service", "catalog-service",
    "payment-service", "order-service", "gateway-server"
]

# List of servers
servers = ["gl2", "gl5", "gl6"]

# Iterate through each server and match corresponding files
for server in servers:
    json_file_pattern = os.path.join(input_dir, f'per_container_cpu_usage_{server}_*.json')
    json_files = glob.glob(json_file_pattern)
    
    for json_file_path in json_files:
        # Create output directory for the server if it doesn't exist
        output_dir = os.path.join(output_base_dir, server)
        os.makedirs(output_dir, exist_ok=True)
        
        # Load the JSON data
        with open(json_file_path, 'r') as f:
            data = json.load(f)

        # Prepare to store data per service
        service_data = {}
        gateway_server_counter = 1  # Counter for naming gateway-server replicas

        for result in data["data"]["result"]:
            cmdline = result["metric"].get("cmdline", "")
            
            # Check for relevant services in cmdline
            for service in relevant_services:
                if service in cmdline:
                    # Special handling for gateway-server
                    if service == "gateway-server":
                        container_name = f"gateway-server_replica_{gateway_server_counter}"
                        gateway_server_counter += 1
                    else:
                        container_name = service
                    
                    # Initialize service data if not already
                    if container_name not in service_data:
                        service_data[container_name] = []
                    
                    # Add data points
                    for timestamp, value in result["values"]:
                        # Convert timestamp (epoch) to the desired format
                        formatted_timestamp = datetime.utcfromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')
                        service_data[container_name].append([formatted_timestamp, container_name, float(value), result["metric"]["pid"]])
                    break

        # Write the service data to CSV files
        for service, records in service_data.items():
            csv_file_path = os.path.join(output_dir, f"{service}.csv")
            with open(csv_file_path, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(["timestamp", "container", "cpu_usage", "pid"])
                csv_writer.writerows(records)

print("CSV files created successfully for all servers.")