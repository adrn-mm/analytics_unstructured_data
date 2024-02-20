import subprocess
import json

# Lokasi file JSON lokal
local_file_path = 'data/dummy_semistructured_data.json'

# Lokasi file JSON yang sudah diformat
formatted_file_path = 'formatted_dummy_semistructured_data.json'

# Lokasi target di HDFS (pastikan ini adalah direktori, bukan file)
hdfs_directory_path = '/user/admin/json_data'
# Nama file di HDFS, dapat menggunakan nama file asli atau yang sudah diformat
hdfs_file_name = 'data.json'

try:
    # Langkah 1 & 2: Membaca dan mengubah format file JSON
    with open(local_file_path, 'r') as json_file:
        json_data = json.load(json_file)
    
    with open(formatted_file_path, 'w') as formatted_file:
        for entry in json_data:
            json.dump(entry, formatted_file)
            formatted_file.write('\n')
    
    # Langkah 3: Mengunggah file yang sudah diformat ke HDFS
    # Pastikan direktori target sudah ada atau buat menggunakan 'hdfs dfs -mkdir'
    subprocess.call(['hadoop', 'fs', '-mkdir', '-p', hdfs_directory_path])
    subprocess.call(['hadoop', 'fs', '-put', '-f', formatted_file_path, f"{hdfs_directory_path}/{hdfs_file_name}"])
    
    print("File JSON yang sudah diformat berhasil di-upload ke HDFS.")

    # Langkah 4: Membuat external table di Hive
    hive_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS json_table (
        name STRING,
        language STRING,
        id STRING,
        bio STRING,
        version FLOAT
    )
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
    STORED AS TEXTFILE
    LOCATION '{hdfs_directory_path}';
    """
    hive_cmd = ['hive', '-e', hive_query]

    subprocess.run(hive_cmd, check=True)
    print("External table berhasil dibuat di Hive.")

except FileNotFoundError as e:
    print(f"Error: {e}")
except json.JSONDecodeError as e:
    print(f"Error: File JSON tidak valid - {e}")
except subprocess.CalledProcessError as e:
    print(f"Error saat menjalankan subprocess: {e}")
except Exception as e:
    print(f"Terjadi kesalahan tak terduga: {e}")