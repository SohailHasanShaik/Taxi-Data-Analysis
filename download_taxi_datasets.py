import requests
import os

def download_file(url, file_path):
    """Download the file from the given URL to the specified file path."""
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192): # Increase the chunk size to increase the download speed.
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
        return True
    return False

def generate_dataset_urls(start_year, end_year):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    types = ["yellow", "green", "fhvhv"]  # fhvhv stands for For-Hire Vehicle High Volume
    folders = {
        "yellow": "yellow_taxi_data",
        "green": "green_taxi_data",
        "fhvhv": "fhvhv_taxi_data"
    }
    urls = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for t in types:
                filename = f"{t}_tripdata_{year}-{month:02}.parquet"
                folder = folders[t]
                url = f"{base_url}/{filename}"
                file_path = f"{folder}/{filename}"
                urls.append((url, file_path))

    return urls

def download_datasets(dataset_urls_and_paths):
    """Download each dataset to its respective folder."""
    for url, file_path in dataset_urls_and_paths:
        success = download_file(url, file_path)
        if success:
            print(f"Downloaded: {file_path}")
        else:
            print(f"Failed to download: {url}")

# Generate the URLs and file paths for the datasets from 2017 to 2023
dataset_urls_and_paths = generate_dataset_urls(2017, 2023)

# Download the datasets
download_datasets(dataset_urls_and_paths)
