import argparse
import pandas as pd
import os
from argparse import RawTextHelpFormatter
from down_sea.function import entrezMode,sratoolkitMode,datasetsMode

"""
Program for multi-download sequence file in many formats (SRA, FASTQ, FASTA and GenBank).
You can select tools for download your sequence such as sratoolkit,entrez or datasets
Created By THN (https://github.com/Thanakron1997)
"""

if __name__ == "__main__":
    title_program = 'Program for multi-download sequence file by sratoolkit(output = SRA,FASTQ) or entrez with GI(GenBank, FASTA) or datasets(GenBank, FASTA)'
    parser = argparse.ArgumentParser(description=title_program,formatter_class=RawTextHelpFormatter)
    parser.add_argument('--version',  '-v',action='version', version='%(prog)s 1.0.1')
    subparsers = parser.add_subparsers(dest='command') # dest = 'command' > specify object name command

    ### download by sratoolkit
    des_sratoolkit_mode = 'Download file by sratoolkit(output = SRA,FASTQ)'
    sratool_mode = subparsers.add_parser('sratool_mode', help='Download sequence by sratoolkit', description = des_sratoolkit_mode)
    sratool_mode.add_argument("--file_type", "-f", help="peration for download:\nall = download all sequences format(FASTQ, SRA format)\nfastq = download only fastq format\nsra = download SRA format\n\n") 
    sratool_mode.add_argument("-o", "--outputdir", help="Output directory folder Ex.  /home/test/test_download\n\n")
    sratool_mode.add_argument("-i", "--input_csv", help="Input file in csv format\n\n")
    sratool_mode.add_argument("-m", "--multiprocessing",help="Use Multiprocess for faster download enter code : 1 - 20\n\n")

    ### download by Entrez
    des_entrez_mode = 'Download file by entrez with GI (GenBank, FASTA)'
    entrez_mode = subparsers.add_parser('entrez_mode', help='Download sequence by entrez', description = des_entrez_mode)
    entrez_mode.add_argument("--file_type", "-f", help="peration for download:\nall = download all sequences format(GenBank, FASTA format)\ngb = download only GenBank format\nfasta = download FASTA format\n\n") 
    entrez_mode.add_argument("-o", "--outputdir", help="Output directory folder Ex.  /home/test/test_download\n\n")
    entrez_mode.add_argument("-i", "--input_csv", help="Input file in csv format\n\n")
    entrez_mode.add_argument("-m", "--multiprocessing",help="Use Multiprocess for faster download enter code : 1 - 20\n\n")

    ### download by datasets
    des_datasets_mode = 'Download file by datasets (GenBank, FASTA)'
    datasets_mode = subparsers.add_parser('datasets_mode', help='Download sequence by datasets', description = des_datasets_mode)
    datasets_mode.add_argument("--file_type", "-f", help="peration for download:\nall = download all sequences format(GenBank, FASTA format)\ngb = download only GenBank format\nfasta = download FASTA format\n\n") 
    datasets_mode.add_argument("-o", "--outputdir", help="Output directory folder Ex.  /home/test/test_download\n\n")
    datasets_mode.add_argument("-i", "--input_csv", help="Input file in csv format\n\n")
    datasets_mode.add_argument("-m", "--multiprocessing",help="Use Multiprocess for faster download enter code : 1 - 20\n\n")

    args = parser.parse_args()
    if args.command == 'sratool_mode':
        input_metadata_file = args.input_csv
        output_seq_file = args.outputdir
        core_use = args.multiprocessing
        file_type = args.file_type
        metadata_dataframe = pd.read_csv(input_metadata_file)
        df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
        if not os.path.exists(output_seq_file):
            os.mkdir(output_seq_file)
        if file_type is None:
            file_type = "all"
        if core_use is None:
            number_core = 1
            print("Using 1 core for downloading")
        else:
            number_core = int(core_use)
        if file_type in ['all','fastq','sra']:
            sratoolkitFuc = sratoolkitMode()
            sratoolkitFuc.multi_download_sratoolkit(df_sra_list, output_seq_file,number_core,file_type)
            print("Finished")
        else:
            print(f"Not Support format: {file_type}")

    elif args.command == 'entrez_mode':
        input_metadata_file = args.input_csv
        output_seq_file = args.outputdir
        core_use = args.multiprocessing
        file_type = args.file_type
        metadata_dataframe = pd.read_csv(input_metadata_file)
        df_gi_list = metadata_dataframe[metadata_dataframe['Gi_list'].notna()]
        if not os.path.exists(output_seq_file):
            os.mkdir(output_seq_file)
        if file_type is None:
            file_type = "fasta"
            print("Download with FASTA format")
        if core_use is None:
            number_core = 1
            print("Using 1 core for downloading")
        else:
            number_core = int(core_use)
        if file_type in ['gb','fasta']:
            entrezFuc = entrezMode()
            entrezFuc.multi_download_entrez(df_gi_list, output_seq_file,number_core,file_type)
            print("Finished")
        else:
            print(f"Not Support format: {file_type}")

    elif args.command == 'datasets_mode':
        input_metadata_file = args.input_csv
        output_seq_file = args.outputdir
        core_use = args.multiprocessing
        file_type = args.file_type
        metadata_dataframe = pd.read_csv(input_metadata_file)
        assembly_list = metadata_dataframe[metadata_dataframe['asm_acc'].notna()]
        if not os.path.exists(output_seq_file):
            os.mkdir(output_seq_file)
        if file_type is None:
            file_type = "fasta"
            print("Download with FASTA format")
        if core_use is None:
            number_core = 1
            print("Using 1 core for downloading")
        else:
            number_core = int(core_use)
        if file_type in ['gb','fasta']:
            datasetsFuc = datasetsMode()
            datasetsFuc.multi_download_datasets(assembly_list, output_seq_file,number_core,file_type)
            print("Finished")
        else:
            print(f"Not Support format: {file_type}")
    else:
        print('No subcommand specified')