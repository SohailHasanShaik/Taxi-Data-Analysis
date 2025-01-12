import os
import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

def plot_and_save(input_folder, output_folder):
    # List all files in the input folder
    files = os.listdir(input_folder)

    # Filter files to get only CSV files
    csv_files = [file for file in files if file.endswith('.csv')]

    # Check if there is exactly one CSV file
    if len(csv_files) != 1:
        print("Error: There should be exactly one CSV file in the input folder.")
        sys.exit(1)

    # Get the CSV file name
    data_file = csv_files[0]

    # Create a DataFrame from the data
    df = pd.read_csv(os.path.join(input_folder, data_file))

    # Set the seaborn style
    sns.set(style="whitegrid")

    # Calculate total pickup count for each company, year, and month
    total_count_df = df.groupby(['company', 'year', 'month'])['pickup_count'].sum().reset_index()

    # Convert 'Year-Month' to datetime format and sort the DataFrame
    total_count_df['Year-Month'] = pd.to_datetime(total_count_df['year'].astype(str) + '-' + total_count_df['month'].astype(str))
    total_count_df = total_count_df.sort_values(['company', 'Year-Month'])

    # Plotting
    plt.figure(figsize=(12, 6))

    # Create a line graph for each company
    for company, company_group in total_count_df.groupby('company'):
        plt.plot(company_group['Year-Month'], company_group['pickup_count'], label=company)

    plt.xlabel('Year-Month')
    plt.ylabel('Total Pickup Count (in thousands)')
    plt.title('Total Pickup Count vs Year-Month for Each Company')
    plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better visibility

    # Format y-axis labels in thousands
    plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, _: int(x / 1000)))

    plt.legend(title='Company', bbox_to_anchor=(1, 1), loc='upper left')

    # Save the plot in the output folder
    output_plot_path = os.path.join(output_folder, 'total_pickup_count_vs_year_month.png')
    plt.savefig(output_plot_path, bbox_inches='tight')
    plt.close()

    print("Plot saved successfully.")

if __name__ == "__main__":
    # Check if the correct number of command line arguments are provided
    if len(sys.argv) != 3:
        print("Usage: python script.py input_folder output_folder")
        sys.exit(1)

    # Get input and output folder paths from command line arguments
    input_folder = sys.argv[1]
    output_folder = sys.argv[2]

    # Call the function to plot and save
    plot_and_save(input_folder, output_folder)
