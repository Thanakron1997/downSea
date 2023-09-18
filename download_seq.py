#download sra and fastq 
import os
import subprocess
import glob
import zipfile
import shutil
import random
import time
import gzip
from multiprocessing import Pool

dir_path = os.path.dirname(os.path.realpath(__file__)) +'/'
fasterq_domp_com = dir_path + 'sratoolkit/bin/fasterq-dump'
prefetch_com = dir_path + 'sratoolkit/bin/prefetch'
dataset_path = dir_path + 'datasets'

def download_sra_file(file_sra_i,sra_run_i):
    try:
        cmd_for_download_sra = prefetch_com +" -f yes -o " + file_sra_i + ' ' + sra_run_i
        subprocess.run(cmd_for_download_sra, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print("Can't run the command. Error message: ", e)
    if not os.path.exists(file_sra_i): # check file ว่า โหลดสำเร็จไหม
        print("Can't Download : {} file".format(sra_run_i))
        i_loop = 1
        while  i_loop < 6:
            i_loop += 1
            sec_ran_reload = random.randint(30, 120)
            time.sleep(sec_ran_reload)
            subprocess.run(cmd_for_download_sra, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if not os.path.exists(file_sra_i):
                print("Can't Download (SRA): {} file".format(sra_run_i))
            else:
                break
    return cmd_for_download_sra

def download_fastq_file(file_sra_i,sra_run_i,folder_output_fastq,cmd_for_download_sra):
    try:
        cmd_fasterq_dump = fasterq_domp_com + " " + file_sra_i + " -O " + folder_output_fastq 
        subprocess.run(cmd_fasterq_dump, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print("Can't run the command. Error message: ", e)

    files_in_directory = os.listdir(folder_output_fastq)
    files_contain_target = [file for file in files_in_directory if sra_run_i in file]
    total_files_fastq = len(files_contain_target)
    if total_files_fastq > 0:
        print("Download {} Completed".format(sra_run_i))
    elif total_files_fastq == 0:
        print("Can't Download (FASTQ): {} file".format(sra_run_i))
        if not os.path.exists(file_sra_i):
            subprocess.run(cmd_for_download_sra, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(cmd_fasterq_dump, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.run(cmd_fasterq_dump, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            pass
    else:
        pass
    try:
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
    except Exception as e_1:
        print(e_1)

def process_fastq(args):
    sra_index, df_sra_list,folder_output_sra,folder_output_fastq = args
    sra_run_i = df_sra_list['Run'][sra_index]
    file_sra_i = folder_output_sra + '/' + sra_run_i + '.sra' 
    print('Download Sequence Data (FASTQ) - SRA Number: {} - ({})'.format(sra_run_i,sra_index))
    try:
        cmd_for_download_sra = download_sra_file(file_sra_i,sra_run_i)
        download_fastq_file(file_sra_i,sra_run_i,folder_output_fastq,cmd_for_download_sra)
    except Exception as e_all:
        print("Error -> {} in {}".format(e_all,sra_run_i))

def process_sra(args):
    sra_index, df_sra_list,folder_output_sra = args
    sra_run_i = df_sra_list['Run'][sra_index]
    file_sra_i = folder_output_sra + sra_run_i + '.sra'
    print('Download Sequence Data (SRA) - SRA Number: {} - ({})'.format(sra_run_i,sra_index))
    try:
        cmd_for_download_sra = download_sra_file(file_sra_i,sra_run_i)
    except Exception as e:
        print("Error -> {} in {}".format(e,sra_run_i))

def process_nuc(args):
    nuc_index, df_nuc_list,folder_output_nuc = args
    nuc_accession = str(df_nuc_list['asm_acc'][nuc_index])
    file_out_put_name = folder_output_nuc + nuc_accession + '.zip'
    cmd_for_download = dataset_path + " download virus genome accession "+nuc_accession+ " --include genome --filename " + file_out_put_name
    print('Download Sequence Data (Nucleotide) - Accession Number: {} - ({})'.format(nuc_accession,nuc_index))
    subprocess.run(cmd_for_download, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    if not os.path.exists(file_out_put_name):
        print("Can't Download (Nucleotide): {} file".format(nuc_accession))
        sec_ran = random.randint(30, 150)
        time.sleep(sec_ran)
        subprocess.run(cmd_for_download, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    else:
        pass
    if os.path.exists(file_out_put_name):
        extract_zip_path = folder_output_nuc + nuc_accession
        with zipfile.ZipFile(file_out_put_name, 'r') as zip_ref:
            zip_ref.extractall(extract_zip_path)
        os.remove(file_out_put_name)
        file_fasta_i = folder_output_nuc + nuc_accession + '/ncbi_dataset/data/*.fna'
        file_fasta_i_move = folder_output_nuc + nuc_accession + '.fna'
        for i in glob.glob(file_fasta_i):
            shutil.move(i, file_fasta_i_move)
            file_remove_i = folder_output_nuc + nuc_accession
            shutil.rmtree(file_remove_i)
        file_name_path_raw = file_fasta_i_move
        file_name_path_gz = file_fasta_i_move + '.gz'
        with open(file_name_path_raw, 'rb') as f_in:
            with gzip.open(file_name_path_gz, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(file_name_path_raw)
        print("Download Completed")
        
def process_fasta(ass_index,assembly_list,folder_output_fasta):
    assembly_name_i = str(assembly_list['asm_acc'][ass_index])
    file_out_put_name = folder_output_fasta + assembly_name_i + '.zip'
    cmd_for_download = dataset_path + " download genome accession "+assembly_name_i+ " --include genome --filename " + file_out_put_name
    print('Download Sequence Data (FASTA) - Assembly Number: {} - ({})'.format(assembly_name_i,ass_index))
    subprocess.run(cmd_for_download, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    if not os.path.exists(file_out_put_name):
        print("Can't Download (FASTA): {} file".format(assembly_name_i))
        sec_ran = random.randint(30, 150)
        time.sleep(sec_ran)
        subprocess.run(cmd_for_download, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
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

    if not os.path.exists(file_name_path_gz):
        assembly_i_GCF = assembly_name_i.replace("GCA", "GCF")
        print("Can't Download : {} file and try to download by {}....".format(assembly_name_i,assembly_i_GCF))
        file_out_put_name_2 = folder_output_fasta +'/' + assembly_i_GCF + '.zip'
        cmd_for_download_2 = dataset_path + " download genome accession "+assembly_i_GCF+ " --include genome --filename " + file_out_put_name_2
        subprocess.run(cmd_for_download_2, shell=True,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not os.path.exists(file_out_put_name_2):
            print("Can't Download (FASTA): {} file! - Try to download again...".format(assembly_i_GCF))
            i_loop = 1
            while  i_loop < 5:
                i_loop += 1
                sec_ran_reload = random.randint(30, 120)
                time.sleep(sec_ran_reload)
                subprocess.run(cmd_for_download_2, shell=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                if not os.path.exists(file_out_put_name_2):
                    print("Can't Download (FASTA): {} file".format(assembly_i_GCF))
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

def multi_download_fastq(df_sra_list,output_seq_file,number_core):
    folder_output_fastq = output_seq_file + '/raw_sequences/'
    folder_output_sra = output_seq_file + '/temp_sra/'
    if not os.path.exists(folder_output_fastq):
        os.mkdir(folder_output_fastq)
    else:
        pass
    if not os.path.exists(folder_output_sra):
        os.mkdir(folder_output_sra)
    else:
        pass
    df_sra_index = df_sra_list.index
    with Pool(processes=number_core) as pool:
        pool.map(process_fastq, [(sra_index, df_sra_list,folder_output_sra,folder_output_fastq) for sra_index in df_sra_index])
    if os.path.exists(folder_output_sra):
        shutil.rmtree(folder_output_sra, ignore_errors = False)
        print("All Download Completed")

def multi_download_sra(df_sra_list, output_seq_file,number_core):
    folder_output_sra = output_seq_file + '/sra/'
    if not os.path.exists(folder_output_sra): # create folder for save sra file if dont have
        os.mkdir(folder_output_sra)
    else:
        pass
    df_sra_index = df_sra_list.index
    with Pool(processes=number_core) as pool:
        pool.map(process_sra, [(sra_index, df_sra_list,folder_output_sra) for sra_index in df_sra_index])
    # if os.path.exists(folder_output_sra):
    #     shutil.rmtree(folder_output_sra, ignore_errors = False)
        print("All Download Completed")

def multi_download_nucleotide(df_nuc_list, output_seq_file,number_core):
    folder_output_nuc = output_seq_file + '/nucleotide/'
    if not os.path.exists(folder_output_nuc): # create folder for save sra file if dont have
        os.mkdir(folder_output_nuc)
    else:
        pass
    df_nuc_index = df_nuc_list.index
    with Pool(processes=number_core) as pool:
        pool.map(process_nuc, [(nuc_index, df_nuc_list,folder_output_nuc) for nuc_index in df_nuc_index])
        print("All Download Completed")

def multi_download_fasta(assembly_list, output_seq_file,number_core):
    folder_output_fasta = output_seq_file + '/Assembly/'
    if not os.path.exists(folder_output_fasta):
        os.mkdir(folder_output_fasta)
    else:
        pass
    assembly_path_list = assembly_list.index
    with Pool(processes=number_core) as pool:
        pool.map(process_fasta, [(ass_index, assembly_list,folder_output_fasta) for ass_index in assembly_path_list])
    print("All Download Completed")


