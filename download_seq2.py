#download sra and fastq 
import os
import subprocess
import glob
import zipfile
import shutil
import random
import time
import gzip

dir_path = os.path.dirname(os.path.realpath(__file__)) +'/'

#founction for download sequence  sra file
def download_sra_file(df_sra_list, output_seq_file):
    folder_output_sra = output_seq_file + '/sra/'
    if not os.path.exists(folder_output_sra): # create folder for save sra file if dont have
        os.mkdir(folder_output_sra)
    else:
        pass

    sra_id_list = list(df_sra_list.index)
    for i in sra_id_list: # loop for download sra
        sra_run_i = df_sra_list['Run'][i]
        file_sra_i = folder_output_sra + sra_run_i + '.sra' 

        if not os.path.exists(file_sra_i): #check sra existed or not
            prefetch_com = dir_path + 'sratoolkit/bin/prefetch'
            cmd_for_download_sra = prefetch_com +" -f yes -o " + file_sra_i + ' ' + sra_run_i
            print("Downloading {} SRA file.....".format(sra_run_i))
            subprocess.run(cmd_for_download_sra, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            if not os.path.exists(file_sra_i): 
                print("Can't Download : {} file".format(sra_run_i))
                sec_ran = random.randint(30, 150)
                time.sleep(sec_ran)
                print("Downloading {} SRA File Again....".format(sra_run_i))
                subprocess.run(cmd_for_download_sra, shell=True) 
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
    folder_output_sra = output_seq_file + '/sra_2/'
    if not os.path.exists(folder_output_fastq):
        os.mkdir(folder_output_fastq)
    else:
        pass
    if not os.path.exists(folder_output_sra):
        os.mkdir(folder_output_sra)
    else:
        pass
    sra_id_list = list(df_sra_list.index)
    for i in sra_id_list:
        sra_run_i = df_sra_list['Run'][i]
        file_sra_i = folder_output_sra + '/' + sra_run_i + '.sra' 
        print('Download Sequence Data - SRA Number: {} - ({})'.format(sra_run_i,i))
        try:
            
            cmd_for_download_sra = "/home/ad0/down_sea/sratoolkit/bin/prefetch -f yes -o " + file_sra_i + ' ' + sra_run_i
            subprocess.call(cmd_for_download_sra, shell=True)
        except subprocess.CalledProcessError as e:
            print("Can't run the command. Error message: ", e)
        if not os.path.exists(file_sra_i): # check file ว่า โหลดสำเร็จไหม
                print("Can't Download : {} file".format(file_sra_i))
                i_loop = 1
                while  i_loop < 5:
                    i_loop += 1
                    sec_ran_reload = random.randint(30, 120)
                    time.sleep(sec_ran_reload)
                    subprocess.run(cmd_for_download_sra, shell=True)
                    if not os.path.exists(file_sra_i):
                        print("Can't Download : {} file".format(file_sra_i))
                    else:
                        print("Download Completed") 
                        break
        try:
            cmd_fasterq_dump = "/home/ad0/down_sea/sratoolkit/bin/fasterq-dump " + file_sra_i + " -O " + folder_output_fastq 
            subprocess.call(cmd_fasterq_dump, shell=True)
        except subprocess.CalledProcessError as e:
            print("Can't run the command. Error message: ", e)

        files_in_directory = os.listdir(folder_output_fastq)
        files_contain_target = [file for file in files_in_directory if sra_run_i in file]

        for i in files_contain_target:
            file_name_path_raw = folder_output_fastq + str(i)
            file_name_path_gz = folder_output_fastq + str(i) + '.gz'
            with open(file_name_path_raw, 'rb') as f_in:
                with gzip.open(file_name_path_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_name_path_raw)
        try:
            if os.path.exists(file_sra_i):
                os.remove(file_sra_i)
        except Exception as e:
            print(e)

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
    
        subprocess.run(cmd_for_download, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        if not os.path.exists(file_out_put_name):
            print("Can't Download : {} file".format(assembly_name_i))
            sec_ran = random.randint(30, 150)
            time.sleep(sec_ran)
            subprocess.run(cmd_for_download, shell=True)
        else:
            pass
        if os.path.exists(file_out_put_name):
            print("Extracting Zip....")
            extract_zip_path = folder_output_fasta + assembly_name_i
            with zipfile.ZipFile(file_out_put_name, 'r') as zip_ref:
                zip_ref.extractall(extract_zip_path)
            os.remove(file_out_put_name)
            file_fasta_i = folder_output_fasta + assembly_name_i + '/ncbi_dataset/data/' + assembly_name_i +'/*.fna'
            file_fasta_i_move = folder_output_fasta + assembly_name_i + '.fna'
            for i in glob.glob(file_fasta_i):
                shutil.move(i, file_fasta_i_move)
                file_remove_i = folder_output_fasta + assembly_name_i
                shutil.rmtree(file_remove_i)
            print("Download Completed")
            file_name_path_raw = file_fasta_i_move
            file_name_path_gz = file_fasta_i_move + '.gz'
            with open(file_name_path_raw, 'rb') as f_in:
                with gzip.open(file_name_path_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_name_path_raw)

        if not os.path.exists(file_fasta_i_move):
            assembly_i_GCF = assembly_name_i.replace("GCA", "GCF")
            print("Can't Download : {} file and try to download by {}....".format(assembly_name_i,assembly_i_GCF))
            
            file_out_put_name_2 = folder_output_fasta +'/' + assembly_i_GCF + '.zip'
            cmd_for_download_2 = "/data/home/tiravut.per/down_sea/datasets download genome accession "+assembly_i_GCF+ " --include genome --filename " + file_out_put_name_2
            subprocess.run(cmd_for_download_2, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(file_out_put_name_2):
                print("Can't Download : {} file! - Try to download again...".format(assembly_i_GCF))
                i_loop = 1
                while  i_loop < 5:
                    i_loop += 1
                    sec_ran_reload = random.randint(30, 120)
                    time.sleep(sec_ran_reload)
                    subprocess.run(cmd_for_download_2, shell=True)
                    if not os.path.exists(file_out_put_name_2):
                        print("Can't Download : {} file".format(assembly_i_GCF))
                    else:
                        print("Download Completed") 
                        break
            else:
                print("Download Completed")

        if os.path.exists(file_out_put_name_2):
            print("Extracting Zip....")
            extract_zip_path = folder_output_fasta + assembly_i_GCF
            with zipfile.ZipFile(file_out_put_name_2, 'r') as zip_ref:
                zip_ref.extractall(extract_zip_path)
            os.remove(file_out_put_name)
            file_fasta_i = folder_output_fasta + assembly_i_GCF + '/ncbi_dataset/data/' + assembly_name_i +'/*.fna'
            file_fasta_i_move = folder_output_fasta + assembly_name_i + '.fna'
            for i in glob.glob(file_fasta_i):
                shutil.move(i, file_fasta_i_move)
                file_remove_i = folder_output_fasta + assembly_i_GCF
                shutil.rmtree(file_remove_i)
            print("Download Completed")
            file_name_path_raw = file_fasta_i_move
            file_name_path_gz = file_fasta_i_move + '.gz'
            with open(file_name_path_raw, 'rb') as f_in:
                with gzip.open(file_name_path_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_name_path_raw)