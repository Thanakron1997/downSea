downSea
==============

Program for multi-download sequence file in many formats (SRA, FASTQ, FASTA and GenBank).
You can select tools for download your sequence such as sratoolkit,entrez or datasets
*remark the csv file must have column name Run or column name asm_acc or Gi_list + Accession

Example CSV file
| Gi_list | Accession | Run | asm_acc |
|---|---|---|---|
| 1109557564 | NC_003197.2 |  |  |
| 378697983 | NC_016810.1 |  |  |
|  |  | SRR11942647 |  |
|  |  |  | GCA_024342705.1 |


# installation
```
git clone https://github.com/Thanakron1997/down_sea.git
cd download_seq ; python3 install.py
```

# Usage 
```
usage: down_sea.py [-h] {sratool_mode,entrez_mode,datasets_mode} ...

Program for multi-download sequence file by sratoolkit(output = SRA,FASTQ) or entrez with GI(GenBank, FASTA) or datasets(GenBank, FASTA)

positional arguments:
  {sratool_mode,entrez_mode,datasets_mode}
    sratool_mode        Download sequence by sratoolkit
    entrez_mode         Download sequence by entrez
    datasets_mode       Download sequence by datasets

options:
  -h, --help            show this help message and exit
```
- entrez_mode
```
usage: down_sea.py entrez_mode [-h] [--file_type FILE_TYPE] [-o OUTPUTDIR] [-i INPUT_CSV]
                               [-m MULTIPROCESSING]

Download file by entrez with GI (GenBank, FASTA)

options:
  -h, --help            show this help message and exit
  --file_type FILE_TYPE, -f FILE_TYPE
                        peration for download: all = download all sequences
                        format(GenBank, FASTA format) gb = download only GenBank format
                        fasta = download FASTA format
  -o OUTPUTDIR, --outputdir OUTPUTDIR
                        Output directory folder Ex. /home/test/test_download
  -i INPUT_CSV, --input_csv INPUT_CSV
                        Input file in csv format
  -m MULTIPROCESSING, --multiprocessing MULTIPROCESSING
                        Use Multiprocess for faster download enter code : 1 - 20
```
# Example 
- Run Command below
```
python3 down_sea.py entrez_mode -i test.csv -o test_seq -f gb -m 4
```
