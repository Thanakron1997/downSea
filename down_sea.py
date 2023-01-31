import argparse
import pandas as pd
from download_seq import download_sra_file, download_fastq_file, download_fasta_file

title_program = 'Program for download Sequence file (Raw sequence SRA and FASTQ, Assembly sequence FASTA format)'

parser = argparse.ArgumentParser(description=title_program)
parser.add_argument("-d","--download_type", help="Operation for download all = download all format, fastq_only = download only fastq format, fasta_only download only fasta format, raw_seq = download SRA and FASTQ format")
parser.add_argument("-o", "--outputdir", help="Output directory folder Ex.  /home/test/test_download")
parser.add_argument("-i", "--input_csv", help="Input file in csv format")


args = parser.parse_args()

if args.download_type == "all":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    metadata_dataframe = pd.read_csv(input_metadata_file)
    #แยกข้อมูล SRA กับ assembly file ออกจากกัน for download
    df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
    assembly_list = metadata_dataframe[metadata_dataframe['asm_acc'].notna()]
    download_sra_file(df_sra_list, output_seq_file)
    download_fastq_file(df_sra_list, output_seq_file)
    download_fasta_file(assembly_list, output_seq_file)
    print("Finished")

elif args.download_type == "fastq_only":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    metadata_dataframe = pd.read_csv(input_metadata_file)
    #แยกข้อมูล SRA กับ assembly file ออกจากกัน for download
    df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
    download_fastq_file(df_sra_list, output_seq_file)
    print("Finished")

elif args.download_type == "fasta_only":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    metadata_dataframe = pd.read_csv(input_metadata_file)
    #แยกข้อมูล SRA กับ assembly file ออกจากกัน for download
    assembly_list = metadata_dataframe[metadata_dataframe['asm_acc'].notna()]
    download_fasta_file(assembly_list, output_seq_file)
    print("Finished")

elif args.download_type == "raw_seq":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    metadata_dataframe = pd.read_csv(input_metadata_file)
    #แยกข้อมูล SRA กับ assembly file ออกจากกัน for download
    df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
    download_sra_file(df_sra_list, output_seq_file)
    download_fastq_file(df_sra_list, output_seq_file)
    print("Finished")
else:
    print("Invalid operation")

    # main.py -d raw_seq -o /home/ad0/test -i test_csv.csv