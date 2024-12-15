import pandas as pd
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np
import os

# Define the paths for the data
cpu_data_path = './cpu_util_data_per_container'
estimated_data_path = './estimated_cpu_data'  # This should be a path to the directory
output_dir = 'cpu_comparison_plots'  # Directory to save the plots

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# List of servers and containers to process (example)
servers = ['gl2', 'gl5', 'gl6']  # Use correct server names
containers = [
    'account-service', 'billing-service', 'catalog-service', 'payment-service',
    'order-service', 'gateway-server_replica_1', 'gateway-server_replica_2', 'gateway-server_replica_3'
]

# Loop through each server and container
for server in servers:
    for container in containers:
        try:
            # Skip gateway server replicas for servers other than gl2
            if 'gateway-server_replica' in container and server != 'gl2':
                continue

            # Load actual CPU data
            actual_data_path = f"{cpu_data_path}/{server}/{container}.csv"
            if not os.path.exists(actual_data_path):
                print(f"Warning: Actual data file for {server}/{container} not found at {actual_data_path}")
                continue  # Skip if file does not exist

            cpu_data = pd.read_csv(actual_data_path)

            # Ensure 'timestamp' column is datetime
            cpu_data['timestamp'] = pd.to_datetime(cpu_data['timestamp'], errors='coerce')

            # If there's any invalid date after coercion, drop those rows
            cpu_data = cpu_data.dropna(subset=['timestamp'])
            
            cpu_data['time_sec'] = (cpu_data['timestamp'] - cpu_data['timestamp'].iloc[0]).dt.total_seconds()
            cpu_data = cpu_data[(cpu_data['time_sec'] > 60) & (cpu_data['time_sec'] <= 330)]
            cpu_data['interval'] = (cpu_data['time_sec'] // 15) * 15
            grouped = cpu_data.groupby('interval')['cpu_usage'].mean().reset_index()
            grouped.columns = ['Time (s)', 'CPU Utilization (%)']
            grouped['Time (s)'] -= 60

            # Load estimated CPU data (fixed path construction)
            estimated_data_path_container = f"{estimated_data_path}/{server}/{container}.csv"  # Fix the path
            if not os.path.exists(estimated_data_path_container):
                print(f"Warning: Estimated data file for {server}/{container} not found at {estimated_data_path_container}")
                continue  # Skip if file does not exist

            estimated_cpu_data = pd.read_csv(estimated_data_path_container)
            estimated_cpu_data = estimated_cpu_data[estimated_cpu_data['Time (s)'] <= 270]

            # Merge the actual and estimated data
            merged_data = pd.merge(grouped, estimated_cpu_data, on='Time (s)', suffixes=('_actual', '_estimated'))

            # Calculate metrics
            mse = mean_squared_error(merged_data['CPU Utilization (%)_actual'], merged_data['CPU Utilization (%)_estimated'])
            rmse = np.sqrt(mse)
            mae = mean_absolute_error(merged_data['CPU Utilization (%)_actual'], merged_data['CPU Utilization (%)_estimated'])

            # Plot the comparison
            plt.figure(figsize=(10, 6))
            plt.plot(grouped['Time (s)'], grouped['CPU Utilization (%)'], label='Actual CPU Utilization', marker='o')
            plt.plot(estimated_cpu_data['Time (s)'], estimated_cpu_data['CPU Utilization (%)'], label='Estimated CPU Utilization', color='red')
            plt.xlabel('Time (s)')
            plt.ylabel('CPU Utilization (%)')
            plt.title(f'CPU Utilization: Actual vs. Estimated {server} - {container}')
            plt.legend()
            plt.grid(True)

            # Save the plot
            plot_file_name = f"{output_dir}/{server}_{container}_cpu_comparison.png"
            plt.savefig(plot_file_name)
            plt.close()  # Close the plot to free memory

            # Print the error metrics
            print(f"Metrics for {server} - {container}:")
            print(f'Mean Squared Error: {mse}')
            print(f'Root Mean Squared Error: {rmse}')
            print(f'Mean Absolute Error: {mae}')
            print("-" * 50)

        except Exception as e:
            print(f"Error processing {server}/{container}: {e}")
