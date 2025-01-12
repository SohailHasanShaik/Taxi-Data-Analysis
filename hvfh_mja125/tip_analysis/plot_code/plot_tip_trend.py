import os
import argparse
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from tabulate import tabulate


def save_ratio_table_image(input_dir, output_dir):
    # Assuming only one CSV file in the input directory
    csv_file = [file for file in os.listdir(input_dir) if file.endswith('.csv')][0]
    csv_path = os.path.join(input_dir, csv_file)

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Group the data by 'year' and calculate the sum of 'tip_count' and 'without_tip_count' for each group
    grouped_df = df.groupby(['year']).agg({'tip_count': 'sum', 'without_tip_count': 'sum'}).reset_index()

    # Calculate the ratio of trips with tips to trips without tips
    grouped_df['tip_ratio'] = grouped_df['tip_count'] / grouped_df['without_tip_count']

    # Prepare the data for tabulation
    table_data = grouped_df[['year', 'tip_ratio']].values.tolist()
    headers = ['Year', 'Tip Ratio']

    # Generate the table as a string
    table_str = tabulate(table_data, headers, tablefmt='fancy_grid')

    # Plotting the table as an image
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('off')
    ax.text(0, 0.5, table_str, fontsize=10, family='monospace')

    # Save the table as an image to the output directory
    output_path = os.path.join(output_dir, 'ratio_table.png')
    plt.savefig(output_path, bbox_inches='tight', pad_inches=0.1)
    print(f"Table saved as an image: {output_path}")


def plot_tip_counts(input_dir, output_dir):
    # Assuming only one CSV file in the input directory
    csv_file = [file for file in os.listdir(input_dir) if file.endswith('.csv')][0]
    csv_path = os.path.join(input_dir, csv_file)

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Group the data by 'year' and 'month' and sum the 'tip_count' and 'without_tip_count' for each group
    grouped_df = df.groupby(['year', 'month']).agg({'tip_count': 'sum', 'without_tip_count': 'sum'}).reset_index()

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))

    bar_width = 0.35
    bar_positions = range(len(grouped_df))

    # Plot 'tip_count' bars
    ax.bar(bar_positions, grouped_df['tip_count'], width=bar_width, label='With Tip')

    # Plot 'without_tip_count' bars next to 'tip_count' bars
    ax.bar([pos + bar_width for pos in bar_positions], grouped_df['without_tip_count'], width=bar_width, label='Without Tip')

    # Set labels and title
    ax.set_xticks([pos + bar_width / 2 for pos in bar_positions])
    ax.set_xticklabels([f"{row['year']}-{row['month']}" for _, row in grouped_df.iterrows()], rotation=45, ha='right')
    ax.set_xlabel('Month of the Year')
    ax.set_ylabel('Count (in thousands)')
    ax.set_title('Number of Trips with and without tips in New York')

    # Add legend
    ax.legend()

    # Format y-axis ticks to represent thousands
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{int(x / 1000):,}K'))

    # Save the plot to the output directory
    output_path = os.path.join(output_dir, 'tip_counts_plot.png')
    plt.tight_layout()
    plt.savefig(output_path)
    print(f"Plot saved to: {output_path}")

    # Show the plot
    plt.show()

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Plot tip counts by month')
    parser.add_argument('input_dir', help='Input directory containing the CSV file')
    parser.add_argument('output_dir', help='Output directory to save the plot')
    args = parser.parse_args()

    # Call the function with input and output directory paths
    plot_tip_counts(args.input_dir, args.output_dir)
    save_ratio_table_image(args.input_dir, args.output_dir)
