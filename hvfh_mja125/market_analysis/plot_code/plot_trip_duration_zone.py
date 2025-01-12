import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

# Mapping of vendor_id to vendor names
Vendor_names = {'HVOOO2': 'Juno', 'HV0003': 'Uber', 'HV0004': 'Via', 'HV0005': 'Lyft'}

def generate_trip_time_map(input_folder, output_folder):
    # Load GeoJSON file
    geojson_path = "path_to_geojson_file.geojson"
    nyc_geo = gpd.read_file("/mnt/e/big_data_1/ploting_graphs/NYC_Taxi_Zones.geojson")

    # Find the CSV file in the specified input folder
    csv_file = None
    for file in os.listdir(input_folder):
        if file.endswith(".csv"):
            csv_file = os.path.join(input_folder, file)
            break

    if csv_file is None:
        print("No CSV file found in the specified input directory.")
        return

    # Load the dataset with trip time information
    trip_time_data = pd.read_csv(csv_file)

    # Ensure the columns are of correct data types
    nyc_geo['location_id'] = nyc_geo['location_id'].astype(str)
    trip_time_data['start_zone'] = trip_time_data['start_zone'].astype(str)

    # Convert time from seconds to minutes
    trip_time_data['trip_time'] = trip_time_data['trip_time'] / 60.0

    # Merge trip time data with GeoJSON based on start_zone (location_id)
    merged_data = nyc_geo.merge(trip_time_data, how='left', left_on='location_id', right_on='start_zone')

    # Plot the map for each vendor_id
    vendors = trip_time_data['vendor_id'].unique()

    for vendor in vendors:
        vendor_name = Vendor_names.get(vendor, f'Unknown Vendor ({vendor})')
        vendor_data = merged_data[merged_data['vendor_id'] == vendor]

        # Plotting with a different colormap (e.g., 'Blues')
        fig, ax = plt.subplots(figsize=(10, 10))
        vendor_data.plot(ax=ax, column='trip_time', cmap='plasma', legend=True, legend_kwds={'label': "Trip Time (minutes)"})
        ax.set_title(f'Average Trip Time Distribution for {vendor_name}', fontsize=14, fontweight='bold', color='black')
        
        ax.set_axis_off()

        # Find the zone with the highest trip time
        max_zone = vendor_data.loc[vendor_data['trip_time'].idxmax()]['zone']
        max_trip_time = vendor_data['trip_time'].max()

        # Add the name of the zone with the highest trip time under the map
        ax.annotate(f'Zone with Highest Trio Duration: {max_zone}\nTime: {max_trip_time:.2f} minutes',
                    xy=(0.5, -0.1), xycoords='axes fraction',
                    ha='center', va='center', fontsize=12, fontweight='bold', color='red')

        # Save the figure
        output_path = os.path.join(output_folder, f'output_trip_time_map_{vendor}.png')
        plt.savefig(output_path, bbox_inches='tight')
        plt.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py input_directory output_directory")
        sys.exit(1)

    input_directory = sys.argv[1]
    output_directory = sys.argv[2]

    generate_trip_time_map(input_directory, output_directory)
