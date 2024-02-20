import subprocess
import json
import xmltodict

# Lokasi file JSON lokal
xml_file_path = 'data/books.xml'

# Lokasi file JSON yang akan dihasilkan setelah konversi
converted_json_path = 'converted_data.json'

# Lokasi target di HDFS untuk file XML
hdfs_directory_path_xml = '/user/admin/xml_data'
# Lokasi target di HDFS untuk file JSON
hdfs_directory_path_json = '/user/admin/converted_json_data'

# Nama file di HDFS, dapat menggunakan nama file asli atau yang sudah dikonversi
hdfs_file_name_xml = 'data.xml'
hdfs_file_name_json = 'converted_data.json'

# Langkah 1: Mengunggah file XML ke HDFS
subprocess.call(['hadoop', 'fs', '-mkdir', '-p', hdfs_directory_path_xml])
subprocess.call(['hadoop', 'fs', '-put', '-f', xml_file_path, f"{hdfs_directory_path_xml}/{hdfs_file_name_xml}"])
print("File XML berhasil di-upload ke HDFS.")

with open(xml_file_path, 'r') as xml_file:
    xml_content = xml_file.read()
    json_data = xmltodict.parse(xml_content, attr_prefix='')  # Menghilangkan attr_prefix jika ada

    # Misalkan struktur XML memiliki root <catalog> yang berisi banyak <book>
    books = json_data.get('catalog', {}).get('book', [])
    
    # Pastikan 'books' adalah list untuk kasus hanya satu <book>
    if isinstance(books, dict):
        books = [books]

with open(converted_json_path, 'w') as json_file:
    for book in books:
        # Menulis setiap buku sebagai satu objek JSON per baris
        json_file.write(json.dumps(book) + '\n')

# Langkah 4: Mengunggah file JSON yang sudah dikonversi ke HDFS
subprocess.call(['hadoop', 'fs', '-mkdir', '-p', hdfs_directory_path_json])
subprocess.call(['hadoop', 'fs', '-put', '-f', converted_json_path, f"{hdfs_directory_path_json}/{hdfs_file_name_json}"])
print("File JSON yang sudah dikonversi berhasil di-upload ke HDFS.")

 # Langkah 5: Membuat external table di Hive menggunakan file JSON
hive_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS books_converted_json (
    id STRING,
    author STRING,
    title STRING,
    genre STRING,
    price STRING,
    publish_date STRING,
    description STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{hdfs_directory_path_json}'
TBLPROPERTIES ('json.paths'='$.books[*]');
"""
hive_cmd = ['hive', '-e', hive_query]

subprocess.run(hive_cmd, check=True)
print("External table berhasil dibuat di Hive berdasarkan file JSON.")

subprocess.run(hive_cmd, check=True)
print("External table berhasil dibuat di Hive.")