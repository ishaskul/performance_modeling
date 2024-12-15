import os
import json
import glob
import csv
data_dir_name = "buy_books_model_refinement_test_run"

source_dir_path = f"../{data_dir_name}/1/service_time_data"
with open("buy-books-scenario-per-service-job-details.json", 'r') as json_file:
    jobs_per_service_data = json.load(json_file)

with open("./avg_service_time_details.csv", mode='w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["server", "service_name", "job", "avg_service_time"])

def process_scraped_data(scraped_data, service_name, job, server, csv_file_path):
    result = scraped_data.get("data", {}).get("result", [])
    values = result[0]["values"]
    
    # Filter out the "NaN" values and calculate the average of the valid numbers
    valid_values = [float(value[1]) for value in values if value[1] != "NaN" and value[1] != "+Inf"]
    
    if valid_values:
        avg_service_time = sum(valid_values) / len(valid_values)
    else:
        avg_service_time = 0 
    
    avg_service_time = round(avg_service_time, 4)

    with open(csv_file_path, mode='a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([server, service_name, job, avg_service_time])

servers = ["gl2", "gl5", "gl6"]
for server in servers:
    server_dir_path = os.path.join(source_dir_path, server)
                        
    # Iterate over services in service_details JSON
    for service_name, service_info in jobs_per_service_data.items():
        port_number = service_info.get("port_number")
        jobs = service_info.get("jobs", [])
                            
        # print(f"\nserver: {server}")
        # print(f"\nService: {service_name}")
        # print(f"Port: {port_number}")
        # print("Jobs:") 
        for job in jobs:
            #print(f"  - {job}")
            scraped_job_data_path = os.path.join(server_dir_path, f"{service_name}_{job}_*.json")
            scraped_job_data_for_specific_job = glob.glob(scraped_job_data_path)
            file_path = scraped_job_data_for_specific_job[0]
            with open(file_path, 'r') as file:
                 scraped_data = json.load(file)
                 process_scraped_data(scraped_data, service_name, job, server, "./avg_service_time_details.csv")
