import subprocess
import sys

# Fungsi untuk mengunggah file ke HDFS
def upload_video_to_hdfs(local_video_path, hdfs_target_path):
    """
    Mengunggah file video dari sistem lokal ke HDFS.

    :param local_video_path: Jalur lengkap ke file video lokal yang akan diunggah.
    :param hdfs_target_path: Jalur target di HDFS tempat file akan disimpan.
    """
    try:
        # Membuat direktori target di HDFS jika belum ada
        subprocess.run(['hadoop', 'fs', '-mkdir', '-p', hdfs_target_path], check=True)

        # Mengunggah file video ke HDFS
        subprocess.run(['hadoop', 'fs', '-put', '-f', local_video_path, hdfs_target_path], check=True)

        print(f"File {local_video_path} berhasil diunggah ke HDFS di {hdfs_target_path}")
    except subprocess.CalledProcessError as e:
        print(f"Terjadi kesalahan saat mengunggah file video ke HDFS: {e}")

# Memeriksa apakah argumen telah diberikan
if len(sys.argv) != 2:
    print("Usage: python video_ingestion.py <local_video_path>")
    sys.exit(1)
  
local_video_path = sys.argv[1]  # Mengambil nama file video dari argumen command line
hdfs_video_path = '/user/admin/videos/'  # Ganti dengan path target di HDFS

# Memanggil fungsi untuk mengunggah video
upload_video_to_hdfs(local_video_path, hdfs_video_path)
