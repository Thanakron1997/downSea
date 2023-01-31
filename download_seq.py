#download sra and fastq 
import os
import subprocess
import glob
import pandas as pd
import zipfile
import shutil

dir_path = os.path.dirname(os.path.realpath(__file__)) +'/'

#founction for download sequence  sra file
def download_sra_file(df_sra_list, output_seq_file):
    folder_output_sra = output_seq_file + '/sra/'
    if not os.path.exists(folder_output_sra): # create folder for save sra file if dont have
        os.mkdir(folder_output_sra)
    else:
        pass
    # /home/ad0/test/sratoolkit/bin/fasterq-dump

    sra_id_list = list(df_sra_list.index)
    for i in sra_id_list: # loop for download sra
        sra_run_i = df_sra_list['Run'][i]
        file_sra_i = folder_output_sra + sra_run_i + '.sra' #สำหรับ โหลดมาแล้วให้มาเป็นไฟล์เลย ปกติจะมาเป็น folder

        if not os.path.exists(file_sra_i): #checj sra existed or not
            prefetch_com = dir_path + 'sratoolkit/bin/prefetch'
            cmd_for_download_sra = prefetch_com +" -f yes -o " + file_sra_i + ' ' + sra_run_i
            print("Downloading {} SRA file.....".format(sra_run_i))
            subprocess.run(cmd_for_download_sra, shell=True ) #run prefetch for download sra stdout=subprocess.PIPE, stderr=subprocess.PIPE
            if not os.path.exists(file_sra_i): # check file ว่า โหลดสำเร็จไหม
                print("Can't Download : {} file".format(sra_run_i))
            else:
                print("Download Completed")
        elif os.path.exists(file_sra_i):
            print("{} file is existed".format(sra_run_i))
        else:
            pass

#founction for download sequence fastq file
def download_fastq_file(df_sra_list, output_seq_file):
    fasterq_domp_com = dir_path + 'sratoolkit/bin/fasterq-dump'
    folder_output_fastq = output_seq_file + '/fastq/'
    folder_output_sra = output_seq_file + '/sra/'
    if not os.path.exists(folder_output_fastq):
        os.mkdir(folder_output_fastq)
    else:
        pass

    sra_id_list = list(df_sra_list.index)
    try:
        for i in sra_id_list:
            sra_run_i = df_sra_list['Run'][i]
            file_sra_i = folder_output_sra + sra_run_i + '.sra'
            cmd_fasterq_dump = fasterq_domp_com + " " + file_sra_i + " -O " + folder_output_fastq
            print("Downloading {} FASTQ file.....".format(sra_run_i))
            subprocess.run(cmd_fasterq_dump, shell=True ) #stdout=subprocess.PIPE, stderr=subprocess.PIPE -> for hide download sra process 

            files_in_directory = os.listdir(folder_output_fastq)
            files_contain_target = [file for file in files_in_directory if sra_run_i in file]
            if files_contain_target:
                print('Download Completed')
            else:
                print("Can't Download : {} file".format(sra_run_i))
    except:
        for i in sra_id_list:
            sra_run_i = df_sra_list['Run'][i]
            cmd_fasterq_dump = fasterq_domp_com + " " + sra_run_i + " -O " + folder_output_fastq
            print("Downloading {} FASTQ file.....".format(sra_run_i))
            subprocess.run(cmd_fasterq_dump, shell=True) #stdout=subprocess.PIPE, stderr=subprocess.PIPE -> for hide download sra process 

            files_in_directory = os.listdir(folder_output_fastq)
            files_contain_target = [file for file in files_in_directory if sra_run_i in file]
            if files_contain_target:
                print('Download Completed')
            else:
                print("Can't Download : {} file".format(sra_run_i))

#founction for download sequence fasta file
def download_fasta_file(assembly_list, output_seq_file):
    dataset_path = dir_path + 'datasets'
    folder_output_fasta = output_seq_file + '/fasta/'
    if not os.path.exists(folder_output_fasta):
        os.mkdir(folder_output_fasta)
    else:
        pass

    assembly_path_list = list(assembly_list.index)
    for i in assembly_path_list:
        assembly_name_i = str(assembly_list['asm_acc'][i])
        file_out_put_name = folder_output_fasta + assembly_name_i + '.zip'
        cmd_for_download = dataset_path + " download genome accession "+assembly_name_i+ " --include genome --filename " + file_out_put_name
        print("Downloading {} FASTA file.....".format(assembly_name_i))
    
        subprocess.run(cmd_for_download, shell=True)
        if not os.path.exists(file_out_put_name):
            print("Can't Download : {} file".format(assembly_name_i))
        elif os.path.exists(file_out_put_name):
            print("Extracting Zip....")
            extract_zip_path = folder_output_fasta + assembly_name_i
            with zipfile.ZipFile(file_out_put_name, 'r') as zip_ref:
                zip_ref.extractall(extract_zip_path)
            os.remove(file_out_put_name)
            # /home/va/test_download/fasta/GCA_013392775.1/ncbi_dataset/data/GCA_013392775.1/
            file_fasta_i = folder_output_fasta + assembly_name_i + '/ncbi_dataset/data/' + assembly_name_i +'/*.fna'
            file_fasta_i_move = folder_output_fasta + assembly_name_i + '.fna'
            for i in glob.glob(file_fasta_i):
                shutil.move(i, file_fasta_i_move)
                file_remove_i = folder_output_fasta + assembly_name_i
                shutil.rmtree(file_remove_i)
            print("Download Completed")

# # input and output 
# input_metadata_file = 'test_csv.csv'
# output_seq_file = '/home/va/test_download'

# metadata_dataframe = pd.read_csv(input_metadata_file)
# #แยกข้อมูล SRA กับ assembly file ออกจากกัน for download
# df_sra_list = metadata_dataframe[metadata_dataframe['Run'].notna()]
# assembly_list = metadata_dataframe[metadata_dataframe['asm_acc'].notna()]


# download_sra_file(df_sra_list)
# download_fastq_file(df_sra_list)
# download_fasta_file(assembly_list)

