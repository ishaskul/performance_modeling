import numpy as np
import matplotlib.pyplot as plt
import csv
import json
import os
from collections import deque

class CPUSimulation:
    def __init__(self, duration, idle_time, ramp_up_duration, burst_duration, burst_arrival_rate, steady_arrival_rate, service_time_range, num_cores, time_step=1):
        self.duration = duration
        self.idle_time = idle_time
        self.ramp_up_duration = ramp_up_duration
        self.burst_duration = burst_duration
        self.burst_arrival_rate = burst_arrival_rate
        self.steady_arrival_rate = steady_arrival_rate
        self.service_time_range = service_time_range
        self.num_cores = num_cores
        self.time_step = time_step
        self.time_points = int(duration / time_step)
        self.time = np.arange(0, duration, time_step)
        self.cpu_utilization = np.zeros(self.time_points)
        self.queue = deque()

    def simulate(self):
        """
        Simulate CPU utilization for a Docker container with explicit queuing behavior.
        """
        for t in range(self.time_points):
            # Set current arrival rate based on time phases
            if t < self.idle_time:
                current_arrival_rate = 0
            elif t < self.idle_time + self.burst_duration:
                current_arrival_rate = self.burst_arrival_rate
            elif t < self.idle_time + self.burst_duration + self.ramp_up_duration:
                current_arrival_rate = self.steady_arrival_rate
            else:
                # Gradually taper off arrival rate instead of abruptly cutting to zero
                current_arrival_rate = max(
                    0,
                    self.steady_arrival_rate * (1 - (t - (self.idle_time + self.burst_duration + self.ramp_up_duration)) / self.duration)
                )

            # Generate arrivals using Poisson distribution
            arrivals = np.random.poisson(current_arrival_rate * self.time_step)
            for _ in range(arrivals):
                service_time = np.random.uniform(*self.service_time_range)
                self.queue.append(service_time)

            # Process jobs in the queue
            for _ in range(self.num_cores):
                if self.queue:
                    job = self.queue.popleft()
                    if job > self.time_step:
                        remaining_service_time = job - self.time_step
                        self.queue.appendleft(remaining_service_time)
                        self.cpu_utilization[t] += self.time_step / self.num_cores
                    else:
                        self.cpu_utilization[t] += job / self.num_cores

        # Clip CPU utilization to range [0, 100] to avoid unrealistic values
        self.cpu_utilization = np.clip(self.cpu_utilization * 100, 0, 100)
        return self.time, self.cpu_utilization

    def aggregate_and_save_to_csv(self, time, data, interval, file_name):
        """
        Aggregate the data and save it to a CSV file.
        """
        aggregated_time = []
        aggregated_data = []

        for i in range(0, len(time), interval):
            aggregated_time.append(time[i])
            aggregated_data.append(np.mean(data[i:i + interval]))

        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_name), exist_ok=True)

        with open(file_name, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Time (s)', 'CPU Utilization (%)'])
            writer.writerows(zip(aggregated_time, aggregated_data))

        return np.array(aggregated_time), np.array(aggregated_data)

    def plot(self, aggregated_time, aggregated_cpu_utilization, plot_file_name):
        """
        Plot the CPU utilization over time and save to the file.
        """
        plt.figure(figsize=(10, 5))
        plt.plot(aggregated_time, aggregated_cpu_utilization, label='CPU Utilization', color='blue')
        plt.axhline(100, color='red', linestyle='--', label='Max Utilization (100%)')
        plt.title('CPU Utilization of a Docker Container (With Idle Time, Burst Mode, and Cooldown)')
        plt.xlabel('Time (s)')
        plt.ylabel('CPU Utilization (%)')
        plt.ylim(0, max(aggregated_cpu_utilization) * 1.2)
        plt.legend()
        plt.grid()
        plt.tight_layout()

        # Save the plot to the file
        plt.savefig(plot_file_name)
        plt.close()


# Load container data from JSON file
with open('deployment-config.json', 'r') as f:
    containers_config = json.load(f)

# Create a dictionary to store simulations for multiple containers
simulations = {}

# Loop through containers in the config
for server, containers in containers_config.items():
    for container, container_data in containers.items():
        # Extract simulation parameters from the JSON file
        duration = container_data['duration']
        idle_time = container_data['idle_time']
        ramp_up_duration = container_data['ramp_up_duration']
        burst_duration = container_data['burst_duration']
        burst_arrival_rate = container_data['burst_arrival_rate']
        steady_arrival_rate = container_data['steady_arrival_rate']
        service_time_range = container_data['service_time_range']
        num_cores = container_data['num_cores']
        time_step = container_data.get('time_step', 1)  # Use time_step from JSON or default to 1

        # Initialize the simulation with the parameters
        simulations[container] = CPUSimulation(
            duration, idle_time, ramp_up_duration, burst_duration, burst_arrival_rate, steady_arrival_rate,
            service_time_range, num_cores, time_step
        )

        # Run the simulation for the container
        time, cpu_utilization = simulations[container].simulate()

        # Aggregate data and save to CSV
        aggregation_interval = 15
        csv_file_name = f"estimated_cpu_data/{server}/{container}.csv"
        aggregated_time, aggregated_cpu_utilization = simulations[container].aggregate_and_save_to_csv(time, cpu_utilization, aggregation_interval, csv_file_name)

        # Plot the results
        plot_file_name = f"estimated_cpu_data/{server}/{container}_cpu_utilization.png"
        simulations[container].plot(aggregated_time, aggregated_cpu_utilization, plot_file_name)
