import sys
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def find_csv_file(directory):
    # List all files in the directory
    files = os.listdir(directory)

    # Filter only CSV files
    csv_files = [file for file in files if file.lower().endswith('.csv')]

    if len(csv_files) == 1:
        # If there is exactly one CSV file, return its full path
        return os.path.join(directory, csv_files[0])
    elif len(csv_files) == 0:
        print(f"No CSV files found in the directory: {directory}")
    else:
        print(f"Multiple CSV files found in the directory: {directory}")
        print("Please specify the file name as a command-line argument.")
    
    sys.exit(1)

def plot_heatmap(csv_file_path, output_image_path):
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Pivot the DataFrame to get a 2D matrix for the heatmap
    heatmap_data = df.pivot(index='weekday', columns='hour', values='count')

    # Define the order of weekdays
    weekdays_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    # Plot the heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(
        heatmap_data,
        cmap='viridis',
        cbar_kws={'label': 'Number of Trips'},
        xticklabels=range(24),  # Set the x-axis ticks to represent hours
        yticklabels=weekdays_order  # Set the y-axis ticks to represent weekdays
    )
    plt.title('Shared Trip Counts by Hour and Weekday')

    # Save the heatmap image to the specified path
    plt.savefig(output_image_path)
    print(f"Heatmap image saved to: {output_image_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <directory_path> <output_image_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    output_image_path = sys.argv[2]

    # Find the CSV file in the specified directory
    csv_file_path = find_csv_file(directory_path)

    plot_heatmap(csv_file_path, output_image_path)
