import os
import requests
from concurrent.futures import ThreadPoolExecutor


def download_file(url, save_dir):
    filename = url.split("/")[-1]
    save_path = os.path.join(save_dir, filename)

    if os.path.exists(save_path):
        print(f"[SKIPPED] {filename} already exists.")
        return

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            with open(save_path, "wb") as file:
                file.write(response.content)
            print(f"[DOWNLOADED] {filename}")
        else:
            print(f"[FAILED] {filename} - Status: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] {filename} - {e}")


def download_all_files(file_urls, save_dir, max_threads=10):
    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        for url in file_urls:
            executor.submit(download_file, url, save_dir)
