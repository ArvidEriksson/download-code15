# Extract exams_part0.zip to exams_part17.zip

import os
import zipfile
import argparse
import tqdm

def extract_files(path, destination):
    """
    Extract all files in a directory.
    
    Args:
        path (str): Path to the directory containing the zip files.
        destination (str): Directory to extract the files.
    """
    zip_files = [file for file in os.listdir(path) if file.endswith('.zip')]
    for file in tqdm.tqdm(zip_files, desc="Extracting files"):
        with zipfile.ZipFile(os.path.join(path, file), 'r') as zip_ref:
            zip_ref.extractall(destination)
        
def main():
    parser = argparse.ArgumentParser(description='Extract files from a directory')
    parser.add_argument('path', type=str, nargs='?', default=os.getcwd(), help='Path to the directory containing the zip files (default: current working directory)')
    parser.add_argument('destination', type=str, nargs='?', default=os.getcwd(), help='Directory to extract the files (default: current working directory)')

    args = parser.parse_args()
    
    print(f"Extracting files from: {args.path}")
    print(f"Destination directory: {args.destination}")
    extract_files(args.path, args.destination)
    print("\nExtraction completed successfully!")

if __name__ == '__main__':
    main()