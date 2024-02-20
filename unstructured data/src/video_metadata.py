from moviepy.editor import VideoFileClip
import json
import subprocess
import sys
import os

def video_to_json(video_path):
    with VideoFileClip(video_path) as clip:
        video_info = {
            "filename": video_path.split("/")[-1],
            "duration": clip.duration,
            "width": clip.size[0],
            "height": clip.size[1],
            "fps": clip.fps
        }
        return json.dumps(video_info)

# Memeriksa apakah argumen telah diberikan
if len(sys.argv) != 2:
    print("Usage: python video_to_json_hdfs.py <local_video_path>")
    sys.exit(1)
    
local_file_path = sys.argv[1]  # Mengambil path file video dari argumen command line
formatted_file_path = os.path.splitext(local_file_path)[0] + '_video_metadata.json'  # Membuat nama file JSON yang sesuai

# Lokasi target di HDFS dan nama file
hdfs_directory_path = '/user/admin/videp_metadata'
hdfs_file_name = os.path.basename(formatted_file_path)

video_json = video_to_json(local_file_path)

try:
    with open(formatted_file_path, 'w') as formatted_file:
        formatted_file.write(video_json + '\n')  # Menulis JSON ke file

    # Mengunggah file JSON ke HDFS
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', hdfs_directory_path])
    subprocess.call(['hadoop', 'fs', '-put', '-f', formatted_file_path, f"{hdfs_directory_path}/{hdfs_file_name}"])
    print("File JSON berhasil di-upload ke HDFS.")

    # Membuat external table di Hive
    hive_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS video_metadata (
        filename STRING,
        duration FLOAT,
        width INT,
        height INT,
        fps FLOAT
    )
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE
    LOCATION '{hdfs_directory_path}';
    """
    subprocess.run(['hive', '-e', hive_query], check=True)
    print("External table berhasil dibuat di Hive.")

except FileNotFoundError as e:
    print(f"Error: {e}")
except subprocess.CalledProcessError as e:
    print(f"Error saat menjalankan subprocess: {e}")
except Exception as e:
    print(f"Terjadi kesalahan tak terduga: {e}")