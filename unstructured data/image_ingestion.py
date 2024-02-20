import subprocess
import sys

# Fungsi untuk mengunggah file ke HDFS
def upload_to_hdfs(local_file_path, hdfs_target_path):
    """
    Mengunggah file dari sistem lokal ke HDFS.

    :param local_file_path: Jalur lengkap ke file lokal yang akan diunggah.
    :param hdfs_target_path: Jalur target di HDFS tempat file akan disimpan.
    """
    try:
        # Membuat direktori target di HDFS jika belum ada
        subprocess.run(['hadoop', 'fs', '-mkdir', '-p', hdfs_target_path], check=True)

        # Mengunggah file ke HDFS
        subprocess.run(['hadoop', 'fs', '-put', '-f', local_file_path, hdfs_target_path], check=True)

        print(f"File {local_file_path} berhasil diunggah ke HDFS di {hdfs_target_path}")
    except subprocess.CalledProcessError as e:
        print(f"Terjadi kesalahan saat mengunggah file ke HDFS: {e}")

# Memeriksa apakah argumen telah diberikan
if len(sys.argv) != 2:
    print("Usage: python image_ingestion.py <local_image_path>")
    sys.exit(1)

local_image_path = sys.argv[1]  # Mengambil nama file dari argumen command line
hdfs_image_path = '/user/admin/images/'  # Ganti dengan path target di HDFS

# Memanggil fungsi untuk mengunggah gambar
upload_to_hdfs(local_image_path, hdfs_image_path)