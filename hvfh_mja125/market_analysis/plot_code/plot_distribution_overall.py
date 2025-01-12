import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import sys

output_path = ""
def find_csv_file(input_directory):
    # Search for CSV files in the input directory
    csv_files = glob.glob(os.path.join(input_directory, '*.csv'))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in the specified directory: {input_directory}")

    # Assuming there is only one CSV file, return its path
    csv_file_path = csv_files[0]
    print(f"Found CSV file: {csv_file_path}")
    return csv_file_path


def read_data(csv_file_path):
    # Read CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)
    return df

def total_pickup_counts_over_time(df):
    # Total Pickup Counts Over Time
    df['pickup_datetime'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str))
    df_time_series = df.groupby('pickup_datetime')['pickup_count'].sum()

    # Convert total pickup counts to millions
    df_time_series = df_time_series / 1_000_000  # Dividing by 1 million

    plt.figure(figsize=(12, 6))
    plt.plot(df_time_series.index, df_time_series.values, marker='o')
    plt.title('Total Pickup Counts Over Time')
    plt.xlabel('Time')
    plt.ylabel('Total Pickup Counts (Millions)')  # Update y-axis label
    plt.grid(True)
    output_path1 = os.path.join(output_path, f'total_pickup_counts_over_time.png')
    plt.savefig(output_path1)
    plt.show()

def monthly_pickup_counts_by_year(df):
    # Monthly Pickup Counts by Year
    df['month_year'] = df['year'].astype(str) + '-' + df['month'].astype(str)
    
    # Map numerical month to month names with correct order
    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    month_names = {i+1: month_order[i] for i in range(12)}
    df['month_name'] = df['month'].map(month_names)

    df_pivot = df.pivot_table(values='pickup_count', index='month_name', columns='year', aggfunc='sum', fill_value=0)

    # Convert monthly pickup counts to millions
    df_pivot = df_pivot / 1_000_000  # Dividing by 1 million

    plt.figure(figsize=(12, 8))
    sns.heatmap(df_pivot, cmap='YlGnBu', annot=False, fmt='.2f', cbar_kws={'label': 'Monthly Pickup Counts (Millions)'})
    plt.title('Monthly Pickup Counts by Year')
    plt.xlabel('Year')
    plt.ylabel('Month')
    output_path1 = os.path.join(output_path, f'monthly_pickup_counts_by_year.png')
    plt.savefig(output_path1)
    plt.show()





def top_pickup_zones(df, zone_info_path):
    # Read zone information from the provided CSV file
    zone_info = pd.read_csv(zone_info_path)

    # Merge the main DataFrame with the zone information based on the 'start_zone' column
    df_merged = pd.merge(df, zone_info, left_on='start_zone', right_on='LocationId', how='left')

    # If there are missing values in the 'zone' column after the merge, fill them with 'Unknown' or handle as needed
    df_merged['zone'].fillna('Unknown', inplace=True)

    # Top Pickup Zones
    top_zones = df_merged.groupby('zone')['pickup_count'].sum().sort_values(ascending=False).head(10)

    # Convert pickup counts to millions
    top_zones_millions = top_zones / 1_000_000

    plt.figure(figsize=(12, 6))
    ax = top_zones_millions.plot(kind='bar', color='green')

    # Set y-axis ticks in millions
    ax.set_yticklabels(['{:,.0f}M'.format(y) for y in ax.get_yticks()])

    # Set x-axis ticks and rotate labels for better readability
    ax.set_xticks(range(len(top_zones)))
    ax.set_xticklabels(top_zones.index, rotation=45, ha='right')

    # Adjust layout for better visibility of labels
    plt.tight_layout()

    plt.title('Top Pickup Zones')
    plt.ylabel('Total Pickup Counts (Millions)')
    output_path1 = os.path.join(output_path, f'top_pickup_zones.png')
    plt.savefig(output_path1)
    plt.show()



def vendor_comparison(df):
    # Mapping vendor IDs to names
    vendor_mapping = {'HVOOO2': 'juno', 'HV0003': 'uber', 'HV0004': 'via', 'HV0005': 'lyft'}

    # Replace vendor IDs with names
    df['vendor_name'] = df['vendor_id'].map(vendor_mapping)

    # Convert 'month_year' to datetime
    df['month_year'] = pd.to_datetime(df['month_year'])

    # Vendor Comparison
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='month_year', y='pickup_count', hue='vendor_name', data=df)
    plt.title('Uber, Via and Lyft Comparison')
    plt.xlabel('Month-Year')
    plt.ylabel('Pickup Counts')
    plt.legend(title='Service Provider')

    # Format x-axis to show only years
    plt.gca().xaxis.set_major_locator(mdates.YearLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y'))  # Adjust the format as needed
    
    output_path1 = os.path.join(output_path, 'vendor_comparison.png')
    plt.savefig(output_path1)
    plt.show()



def seasonal_patterns(df):
    # Seasonal Patterns
    df['month_name'] = pd.to_datetime(df['month'], format='%m').dt.strftime('%B')
    
    # Convert pickup counts to millions
    df['pickup_count_millions'] = df['pickup_count'] / 1_000_000

    plt.figure(figsize=(12, 6))
    ax = sns.boxplot(x='month_name', y='pickup_count_millions', data=df, order=sorted(df['month_name'].unique()))

    # Adjust layout for better visibility of x-axis labels
    plt.tight_layout()

    # Rotate x-axis labels for better readability
    ax.set_xticks(ax.get_xticks())
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')

    plt.title('Seasonal Patterns')
    plt.xlabel('Month')
    plt.ylabel('Pickup Counts (Millions)')
    output_path1 = os.path.join(output_path, f'seasonal_patterns.png')
    plt.savefig(output_path1)
    plt.show()



def yearly_growth(df):
    # Yearly Growth
    df['yearly_growth'] = df.groupby('year')['pickup_count'].pct_change() * 100

    plt.figure(figsize=(12, 6))
    sns.lineplot(x='month_year', y='yearly_growth', data=df)
    plt.title('Yearly Growth Rate in Pickup Counts')
    plt.xlabel('Month-Year')
    plt.ylabel('Yearly Growth Rate (%)')
    output_path1 = os.path.join(output_path, f'yearly_growth.png')
    plt.savefig(output_path1)
    plt.show()

if __name__ == "__main__":
    # Check if both input and output directories are provided
    if len(sys.argv) != 4:
        print("Usage: python script.py <input_directory> <output_directory> <zone_info_file>")
        sys.exit(1)

    # Extract input, output directory paths, and zone info file from command line arguments
    input_directory_path = sys.argv[1]
    output_path = sys.argv[2]
    zone_info_file = sys.argv[3]

    try:
        # Find the CSV file in the input directory
        csv_file_path = find_csv_file(input_directory_path)

        # Read data
        data_frame = read_data(csv_file_path)

        # Perform individual analyses
        total_pickup_counts_over_time(data_frame)
        monthly_pickup_counts_by_year(data_frame)
        top_pickup_zones(data_frame, zone_info_file)
        vendor_comparison(data_frame)
        seasonal_patterns(data_frame)
        yearly_growth(data_frame)

        # Call other analysis functions...

    except FileNotFoundError as e:
        print(e)
    except Exception as e:
        print(f"An error occurred: {e}")

