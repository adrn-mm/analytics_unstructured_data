from PIL import Image
import json
import os
import subprocess
import sys

def image_metadata(image_path):
    with Image.open(image_path) as img:
        image_info = {
            "filename": os.path.basename(image_path),
            "width": img.width,
            "height": img.height,
            "format": img.format
        }
        return json.dumps(image_info)  # Tanpa indentasi untuk format single-line

# Memeriksa apakah argumen telah diberikan
if len(sys.argv) != 2:
    print("Usage: python image_to_json_hdfs.py <local_image_path>")
    sys.exit(1)

local_file_path = sys.argv[1]  # Mengambil path file gambar dari argumen command line
formatted_file_path = local_file_path.split('.')[0] + '_image_metadata.json'  # Membuat nama file JSON

# Lokasi target di HDFS
hdfs_directory_path = '/user/admin/image_metadata'
hdfs_file_name = os.path.basename(formatted_file_path)

image_metadata_json = image_metadata(local_file_path)

try:
    with open(formatted_file_path, 'w') as formatted_file:
        formatted_file.write(image_metadata_json + '\n')  # Tambahkan newline setelah objek JSON
    
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', hdfs_directory_path])
    subprocess.call(['hadoop', 'fs', '-put', '-f', formatted_file_path, f"{hdfs_directory_path}/{hdfs_file_name}"])
    print("File JSON berhasil di-upload ke HDFS.")

    hive_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS jpg_metadata (
        filename STRING,
        width INT,
        height INT,
        format STRING
    )
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE
    LOCATION '{hdfs_directory_path}';
    """
    subprocess.run(['hive', '-e', hive_query], check=True)
    print("External table berhasil dibuat di Hive.")

except Exception as e:
    print(f"Terjadi kesalahan: {e}")