import argparse
import pandas as pd
from argparse import RawTextHelpFormatter
from func_downsea import multi_download_fastq,multi_download_sra,multi_download_fasta,multi_download_nucleotide,multi_download_nuc_by_gi

title_program = 'Program for download Sequence file (Raw sequence SRA and FASTQ, Assembly sequence FASTA format)'

parser = argparse.ArgumentParser(description=title_program,formatter_class=RawTextHelpFormatter)
parser.add_argument("-d","--download_type", help="Operation for download:\nall = download raw sequences and assembly (FASTQ, FASTA format)\nraw_seq = download only fastq format\nassembly =  download only fasta format\nsra = download SRA format\n\n")
parser.add_argument("-o", "--outputdir", help="Output directory folder Ex.  /home/test/test_download\n\n")
parser.add_argument("-i", "--input_csv", help="Input file in csv format\n\n")
parser.add_argument("-m", "--multiprocessing",help="Use Multiprocess for faster download enter code : 1 - 20\n\n")


args = parser.parse_args()

if args.download_type == "all":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    core_use = args.multiprocessing
    metadata_dataframe = pd.read_csv(input_metadata_file)
    df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
    assembly_list = metadata_dataframe[metadata_dataframe['asm_acc'].notna()]
    fastqonly = False
    if core_use == None:
        print("Using 1 core for downloading")
        number_core = 1
        multi_download_fastq(df_sra_list, output_seq_file,number_core,fastqonly)
        multi_download_fasta(assembly_list, output_seq_file,number_core)
    else:
        try:
            number_core = int(core_use)
            multi_download_fastq(df_sra_list, output_seq_file,number_core,fastqonly)
            multi_download_fasta(assembly_list, output_seq_file,number_core)
        except Exception as e:
            print("Error -> {}".format(e))
    print("Finished")

elif args.download_type == "raw_seq":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    core_use = args.multiprocessing
    metadata_dataframe = pd.read_csv(input_metadata_file)
    df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
    fastqonly = True
    if core_use == None:
        print("Using 1 core for downloading")
        number_core = 1
        multi_download_fastq(df_sra_list, output_seq_file,number_core,fastqonly)
    else:
        try:
            number_core = int(core_use)
            multi_download_fastq(df_sra_list, output_seq_file,number_core,fastqonly)
        except Exception as e:
            print("Error -> {}".format(e))
    print("Finished")

elif args.download_type == "assembly":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    core_use = args.multiprocessing
    metadata_dataframe = pd.read_csv(input_metadata_file)
    assembly_list = metadata_dataframe[metadata_dataframe['asm_acc'].notna()]
    if core_use == None:
        print("Using 1 core for downloading")
        number_core = 1
        multi_download_fasta(assembly_list, output_seq_file,number_core)
    else:
        try:
            number_core = int(core_use)
            multi_download_fasta(assembly_list, output_seq_file,number_core)
        except Exception as e:
            print("Error -> {}".format(e))
    print("Finished")
elif args.download_type == "sra":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    core_use = args.multiprocessing
    metadata_dataframe = pd.read_csv(input_metadata_file)
    df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
    if core_use == None:
        print("Using 1 core for downloading")
        number_core = 1
        multi_download_sra(df_sra_list, output_seq_file,number_core)
    else:
        try:
            number_core = int(core_use)
            multi_download_sra(df_sra_list, output_seq_file,number_core)
        except Exception as e:
            print("Error -> {}".format(e))

    print("Finished")
elif args.download_type == "nucleotide":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    core_use = args.multiprocessing
    metadata_dataframe = pd.read_csv(input_metadata_file)
    df_nuc_list = metadata_dataframe[metadata_dataframe['Accession'].notna()]
    if core_use == None:
        print("Using 1 core for downloading")
        number_core = 1
        multi_download_nucleotide(df_nuc_list, output_seq_file,number_core)
    else:
        try:
            number_core = int(core_use)
            multi_download_nucleotide(df_nuc_list, output_seq_file,number_core)
        except Exception as e:
            print("Error -> {}".format(e))
    print("Finished")

elif args.download_type == "gi":
    input_metadata_file = args.input_csv
    output_seq_file = args.outputdir
    core_use = args.multiprocessing
    metadata_dataframe = pd.read_csv(input_metadata_file)
    df_metadata = metadata_dataframe[metadata_dataframe['Accession'].notna()]
    if core_use == None:
        print("Using 1 core for downloading")
        number_core = 1
        multi_download_nuc_by_gi(df_metadata, output_seq_file,number_core)
    else:
        try:
            number_core = int(core_use)
            multi_download_nuc_by_gi(df_metadata, output_seq_file,number_core)
        except Exception as e:
            print("Error -> {}".format(e))
    print("Finished")
else:
    print("Invalid operation")
