import os
from multiprocessing import Pool


def convert_file_encoding(input_path, output_path):
    try:
        with open(input_path, "r", encoding="iso-8859-1") as infile:
            content = infile.read()

        with open(output_path, "w", encoding="utf-8") as outfile:
            outfile.write(content)

        print(f"[CONVERTED] {os.path.basename(input_path)} -> UTF-8")

    except Exception as e:
        print(f"[ERROR] Failed to convert {os.path.basename(input_path)}: {e}")


def convert_all_files(input_dir, output_dir, max_processes=4):
    tasks = []
    for filename in os.listdir(input_dir):
        if filename.endswith(".dat"):
            input_path = os.path.join(input_dir, filename)
            output_path = os.path.join(output_dir, filename)

            if os.path.exists(output_path):
                print(f"[SKIPPED] {filename} already converted.")
                continue

            tasks.append((input_path, output_path))

    with Pool(processes=max_processes) as pool:
        pool.starmap(convert_file_encoding, tasks)
