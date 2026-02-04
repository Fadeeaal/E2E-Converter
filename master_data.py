import pandas as pd
import numpy as np
import toml
from sqlalchemy import create_engine

# --- KONFIGURASI ---
EXCEL_FILE = "FG Master Data.xlsx"
SHEET_NAME = "Database FG"
TABLE_NAME = "fg_master_data"

def push_to_neon():
    try:
        # 1. Load Koneksi
        print("🔐 Menghubungkan ke Database...")
        secrets = toml.load(".streamlit/secrets.toml")
        pg = secrets["postgres"]
        conn_string = f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}?sslmode=require"
        engine = create_engine(conn_string)

        # 2. Baca Excel
        print(f"📖 Membaca file {EXCEL_FILE}...")
        df = pd.read_excel(EXCEL_FILE, sheet_name=SHEET_NAME)

        # 3. Pembersihan Nama Kolom
        df.columns = df.columns.str.strip().str.lower()
        
        # 4. Merapikan SKU_CODE (PENTING: Menghilangkan '.0' jika ada)
        # Seringkali SKU terbaca sebagai float (123.0), kita ubah jadi string '123'
        def clean_sku(x):
            if pd.isna(x): return 'nan'
            try:
                # Jika angka, ubah ke int dulu baru str (untuk hapus .0)
                return str(int(float(x)))
            except:
                return str(x).strip()

        df['sku_code'] = df['sku_code'].apply(clean_sku)
        df['line'] = df['line'].astype(str).str.strip().replace(['nan', 'None', '', 'NaN'], 'N/A')

        # 5. Konversi Kolom Angka (Menghapus 'TBC' atau teks lainnya)
        numeric_cols = ['pcs_cb', 'kg_cb', 'speed']
        for col in numeric_cols:
            if col in df.columns:
                # errors='coerce' akan mengubah teks seperti 'TBC' menjadi NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 6. Hapus Baris Tidak Valid & Duplikat Internal
        df = df[~df['sku_code'].isin(['nan', 'None', ''])]
        
        initial_len = len(df)
        # Menghapus duplikat kombinasi SKU + LINE
        df = df.drop_duplicates(subset=['sku_code', 'line'], keep='first')
        final_len = len(df)
        
        print(f"📊 Total baris unik yang akan diunggah: {final_len}")
        if initial_len > final_len:
            print(f"⚠️ Menghapus {initial_len - final_len} baris duplikat dari Excel.")

        # 7. Penanganan Nilai Kosong (Menjadi NULL di SQL)
        df = df.replace({np.nan: None})

        # 8. Eksekusi Upload
        print(f"🚀 Mengunggah data ke NeonDB...")
        
        # Menggunakan 'append' (Pastikan sudah TRUNCATE di SQL Editor)
        df.to_sql(
            TABLE_NAME, 
            engine, 
            if_exists='append', 
            index=False, 
            method='multi', 
            chunksize=500
        )
        
        print(f"✅ Selesai! {final_len} baris berhasil masuk.")

    except Exception as e:
        print(f"❌ Terjadi kesalahan: {e}")

if __name__ == "__main__":
    push_to_neon()