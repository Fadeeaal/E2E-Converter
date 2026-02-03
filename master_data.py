import pandas as pd
from sqlalchemy import create_engine
import toml
import os
import numpy as np

# 1. Load Konfigurasi dari secret.toml
secret_path = ".streamlit/secrets.toml"

try:
    config = toml.load(secret_path)
    pg = config['postgres']
    
    # Menyusun Connection String
    # Format: postgresql://user:password@host:port/database?sslmode=require
    conn_string = f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}?sslmode=require"
    print("✅ Konfigurasi database berhasil dimuat.")
except Exception as e:
    print(f"❌ Gagal membaca file secret.toml: {e}")
    exit()

# 2. Baca File Excel
file_path = 'src/master_data/master_data_fg.xlsx'

try:
    # Membaca excel (pastikan nama sheet benar jika ada banyak sheet)
    df = pd.read_excel(file_path)
    
    # Bersihkan nama kolom (lowercase dan hapus spasi) agar cocok dengan database
    df.columns = df.columns.str.strip().str.lower()
    
    print(f"✅ Berhasil membaca {len(df)} baris dari Excel.")
except Exception as e:
    print(f"❌ Gagal membaca file Excel: {e}")
    exit()

# 3. Proses Push ke NeonDB
engine = create_engine(conn_string)

try:
    # Push ke tabel 'master_data'
    # index=False karena kita sudah punya kolom 'material' sebagai index/PK
    df.to_sql('master_data', engine, if_exists='append', index=False)
    print("🚀 SUKSES! Data berhasil di-push ke tabel 'master_data' di NeonDB.")
    
except Exception as e:
    print(f"❌ Terjadi kesalahan saat push ke database: {e}")
    print("\n💡 Saran Perbaikan:")
    print("- Pastikan nama kolom di Excel SAMA dengan nama kolom di tabel SQL.")
    print("- Pastikan tidak ada SKU (material) yang duplikat dengan yang sudah ada di database.")