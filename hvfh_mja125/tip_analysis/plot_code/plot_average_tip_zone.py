import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

# Mapping of vendor_id to vendor names
Vendor_names = {'HVOOO2': 'Juno', 'HV0003': 'Uber', 'HV0004': 'Via', 'HV0005': 'Lyft'}

def generate_tips_map(input_folder, output_folder):
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

    # Load the dataset with tips information
    tips_data = pd.read_csv(csv_file)

    # Ensure the columns are of correct data types
    nyc_geo['location_id'] = nyc_geo['location_id'].astype(str)
    tips_data['drop_zone'] = tips_data['drop_zone'].astype(str)

    # Merge tips data with GeoJSON based on drop_zone (location_id)
    merged_data = nyc_geo.merge(tips_data, how='left', left_on='location_id', right_on='drop_zone')

    # Plot the map for each vendor_id
    vendors = tips_data['vendor_id'].unique()

    for vendor in vendors:
        vendor_name = Vendor_names.get(vendor, f'Unknown Vendor ({vendor})')
        vendor_data = merged_data[merged_data['vendor_id'] == vendor]

        # Find the zone with the highest tip for the current vendor
        max_tip_zone = vendor_data.loc[vendor_data['tips'].idxmax()]['zone']

        # Plotting
        fig, ax = plt.subplots(figsize=(10, 10))
        vendor_data.plot(ax=ax, column='tips', cmap='cividis', legend=True, legend_kwds={'label': "Tips"})
        ax.set_title(f'Tips Distribution for {vendor_name}', fontsize=14, fontweight='bold', color='black')
        
        # Add the name of the zone with the highest tip
        ax.annotate(f'Highest Tip Zone: {max_tip_zone}', xy=(0.5, -0.1), xycoords="axes fraction", ha="center",
                    fontsize=10, fontweight='bold', color='green')

        ax.set_axis_off()

        # Save the figure
        output_path = os.path.join(output_folder, f'output_tips_map_{vendor}.png')
        plt.savefig(output_path, bbox_inches='tight')
        plt.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py input_directory output_directory")
        sys.exit(1)

    input_directory = sys.argv[1]
    output_directory = sys.argv[2]

    generate_tips_map(input_directory, output_directory)
