download_seq
==============

Program for download dna sequence (SRA, FASTQ, FASTA) from NCBI
You can download a lot of dna sequence file in one time! \
*remark the csv file must have column name Run or column name asm_acc (for Assembly) that contain run accession number 

Example CSV file

![image](https://user-images.githubusercontent.com/100277150/214243752-42ed0fd8-e24f-470b-9639-77fdaef7098b.png)



# installation
```
git clone https://github.com/Thanakron1997/download_seq.git
cd download_seq ; python3 install.py
```

# Usage 
```
usage: download_seq.py [-h] [-d DOWNLOAD_TYPE] [-o OUTPUTDIR] [-i INPUT_CSV]

Program for download Sequence file (Raw sequence SRA and FASTQ, Assembly sequence FASTA format)

optional arguments:
  -h, --help            show this help message and exit
  -d DOWNLOAD_TYPE, --download_type DOWNLOAD_TYPE
                        Operation for download all = download all format, fastq_only = download only fastq format, fasta_only download only fasta
                        format, raw_seq = download SRA and FASTQ format
  -o OUTPUTDIR, --outputdir OUTPUTDIR
                        Output directory folder Ex. /home/test/test_download
  -i INPUT_CSV, --input_csv INPUT_CSV
                        Input file in csv format
```

# Example 
```
```
