# Data Matching - Pencocokan Data Kependudukan

Aplikasi web modern untuk mencocokkan data kependudukan dari dua file Excel dengan tingkat akurasi tinggi menggunakan Python Flask dan Tailwind CSS.

## 🚀 Fitur Utama

- **Modern UI**: Desain clean dan profesional dengan Tailwind CSS
- **Field Mapping**: Sistem dropdown untuk mapping kolom antar file yang berbeda
- **Multi Upload**: Upload dua file Excel (.xlsx, .xls) atau CSV
- **Smart Preview**: Preview data untuk memudahkan pemilihan field
- **Flexible Matching**: Map field yang berbeda nama (contoh: NIK → No_Induk_Kependudukan)
- **Akurasi Threshold**: Pengaturan minimum similarity (0-100%)
- **Detailed Results**: Hasil matching dengan tingkat akurasi per mapping dan overall
- **Step-by-Step Process**: Progress indicator yang jelas untuk user experience
- **Responsive Design**: Tampilan optimal di desktop dan mobile

## 🎯 Cara Menggunakan

### 1. Setup Environment

```bash
# Clone atau download project ini
cd coklit-python

# Pastikan virtual environment sudah aktif
source .venv/bin/activate  # Linux/Mac
# atau
.venv\Scripts\activate     # Windows
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Jalankan Aplikasi

```bash
python app.py
```

Buka browser dan akses: `http://localhost:5001`

### 4. Proses Data Step-by-Step

**Step 1: Upload File**

- Pilih dua file Excel/CSV yang ingin dibandingkan
- Drag & drop atau klik untuk upload

**Step 2: Field Mapping**

- Preview data dari kedua file
- Buat mapping antara kolom File 1 dan File 2
- Tambah mapping tambahan dengan tombol "+"
- Atur minimum threshold akurasi

**Step 3: Lihat Hasil**

- Analisa summary statistics
- Lihat data yang cocok dan tidak cocok
- Export atau analisa lebih lanjut

## 📋 Contoh Penggunaan

### Skenario: Data Kependudukan

**File 1 (Database Lama):**

```
NIK                | Nama      | Alamat
1234567890123456  | John Doe  | Jl. Merdeka 1
```

**File 2 (Database Baru):**

```
No_Induk_Kependudukan | Nama_Lengkap | Alamat_Rumah
1234567890123456      | John Doe     | Jl. Merdeka No. 1
```

**Mapping yang dibuat:**

- NIK → No_Induk_Kependudukan
- Nama → Nama_Lengkap
- Alamat → Alamat_Rumah

**Hasil:** Akurasi 95% (NIK: 100%, Nama: 100%, Alamat: 85%)

## Struktur Project

```
coklit-python/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── static/
│   └── css/
│       └── style.css     # Custom styling
├── templates/
│   └── index.html        # Main web interface
├── uploads/              # Temporary file storage
└── README.md            # Dokumentasi ini
```

## Teknologi yang Digunakan

- **Python 3.13+**
- **Flask 3.0** - Web framework
- **Pandas 2.1** - Data manipulation
- **OpenPyXL 3.1** - Excel file handling
- **FuzzyWuzzy 0.18** - String matching algorithm
- **Bootstrap 5** - Frontend styling
- **jQuery 3.6** - Frontend interactions

## Algoritma Matching

Aplikasi menggunakan algoritma fuzzy string matching untuk:

1. **Exact Match**: Mencari kesamaan exact antara nilai
2. **Fuzzy Matching**: Menggunakan Levenshtein distance untuk kesamaan parsial
3. **Score Calculation**: Menghitung rata-rata akurasi dari semua field yang dipilih
4. **Threshold Filtering**: Memisahkan data berdasarkan minimum akurasi yang ditentukan

## Contoh Data Input

### File 1 (Data Penduduk A):

| NIK              | Nama     | Alamat        | Tanggal_Lahir |
| ---------------- | -------- | ------------- | ------------- |
| 1234567890123456 | John Doe | Jl. Merdeka 1 | 1990-01-01    |

### File 2 (Data Penduduk B):

| NIK              | Nama_Lengkap | Alamat_Rumah      | Tgl_Lahir  |
| ---------------- | ------------ | ----------------- | ---------- |
| 1234567890123456 | John Doe     | Jl. Merdeka No. 1 | 01/01/1990 |

### Hasil Matching:

- **NIK**: 100% (exact match)
- **Nama vs Nama_Lengkap**: 100% (exact match)
- **Alamat vs Alamat_Rumah**: 85% (fuzzy match)
- **Overall Accuracy**: 95%

## Catatan Pengembangan

- File yang diupload akan otomatis dihapus setelah proses matching selesai
- Maximum file size: 16MB
- Supported formats: .xlsx, .xls, .csv
- Aplikasi berjalan di localhost port 5000

## Troubleshooting

### Import Error

Jika terjadi error import module, pastikan virtual environment aktif dan semua dependencies terinstall:

```bash
pip install -r requirements.txt
```

### File Upload Error

- Pastikan file format supported (.xlsx, .xls, .csv)
- Check file size tidak melebihi 16MB
- Pastikan file tidak corrupt

### Matching Error

- Pastikan minimal satu field dipilih untuk matching
- Check apakah ada kolom yang sama di kedua file
- Periksa format data (kosong, null values)

## Kontribusi

Feel free to contribute to this project by:

1. Fork the repository
2. Create feature branch
3. Make your changes
4. Submit pull request

## License

MIT License - feel free to use and modify as needed.
