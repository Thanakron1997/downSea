#download sra and fastq 
import os
import subprocess
import glob
import zipfile
import shutil
import random
import time
import gzip
import json
from tqdm import tqdm 
import multiprocessing
from down_sea.errorlog import error_logs,error_logs_try

file_path = os.path.dirname(os.path.realpath(__file__))
dir_path = os.path.dirname(file_path) + '/'
with open('down_sea/config.json', 'r') as f:
    config = json.load(f)
gz_file = config['gz_file']
count_loop = config['count_loop']
fasterq_domp_com = dir_path + 'sratoolkit/bin/fasterq-dump'
prefetch_com = dir_path + 'sratoolkit/bin/prefetch'
dataset_path = dir_path + 'datasets'
fastq_dump = dir_path + 'sratoolkit/bin/fastq-dump'
# ===================================================================== #
#                       download datasets function
# ===================================================================== #

def download_sequences_by_datasets(args):
    ass_index,assembly_list,folder_output_fasta,file_type = args
    assembly_name_i = str(assembly_list['asm_acc'][ass_index])
    file_out_put_name = folder_output_fasta + assembly_name_i + '.zip'
    if file_type == 'gb':
        load_type = 'gbff'
        dot_file = ".gbff"
    else:
        load_type = 'genome'
        dot_file = ".fna"

    cmd_for_download = dataset_path + " download genome accession "+assembly_name_i+ " --include "+load_type+" --filename " + file_out_put_name
    print(cmd_for_download)
    processresult = subprocess.run(cmd_for_download,shell=True, capture_output=True)
    error_logs(cmd_for_download,processresult)
    if not os.path.exists(file_out_put_name):
        sec_ran = random.randint(30, 150)
        time.sleep(sec_ran)
        processresult = subprocess.run(cmd_for_download,shell=True, capture_output=True)
        error_logs(cmd_for_download,processresult)

    if os.path.exists(file_out_put_name):
        extract_zip_path = folder_output_fasta + assembly_name_i
        with zipfile.ZipFile(file_out_put_name, 'r') as zip_ref:
            zip_ref.extractall(extract_zip_path)
        os.remove(file_out_put_name)
        file_fasta_i = folder_output_fasta + assembly_name_i + '/ncbi_dataset/data/' + assembly_name_i +'/*' + dot_file
        file_fasta_i_move = folder_output_fasta + assembly_name_i + dot_file
        for i in glob.glob(file_fasta_i):
            shutil.move(i, file_fasta_i_move)
            file_remove_i = folder_output_fasta + assembly_name_i
            shutil.rmtree(file_remove_i)

    if not os.path.exists(file_fasta_i_move):
        assembly_i_GCF = assembly_name_i.replace("GCA", "GCF")
        file_out_put_name_2 = folder_output_fasta +'/' + assembly_i_GCF + '.zip'
        cmd_for_download_2 = dataset_path + " download genome accession "+assembly_i_GCF+ " --include "+load_type+" --filename " + file_out_put_name_2
        processresult = subprocess.run(cmd_for_download_2,shell=True, capture_output=True)
        error_logs(cmd_for_download_2,processresult)
        if not os.path.exists(file_out_put_name_2):
            i_loop = 1
            while  i_loop < count_loop:
                i_loop += 1
                sec_ran_reload = random.randint(30, 120)
                time.sleep(sec_ran_reload)
                processresult = subprocess.run(cmd_for_download_2,shell=True, capture_output=True)
                error_logs(cmd_for_download_2,processresult)
                if os.path.exists(file_out_put_name_2):
                    break
    
        if os.path.exists(file_out_put_name_2):
            extract_zip_path = folder_output_fasta + assembly_i_GCF
            with zipfile.ZipFile(file_out_put_name_2, 'r') as zip_ref:
                zip_ref.extractall(extract_zip_path)
            os.remove(file_out_put_name)
            file_fasta_i = folder_output_fasta + assembly_i_GCF + '/ncbi_dataset/data/' + assembly_name_i +'/*' + dot_file
            file_fasta_i_move = folder_output_fasta + assembly_name_i + dot_file
            for i in glob.glob(file_fasta_i):
                shutil.move(i, file_fasta_i_move)
                file_remove_i = folder_output_fasta + assembly_i_GCF
                shutil.rmtree(file_remove_i)

        if os.path.exists(file_fasta_i_move) and gz_file == True and file_type == 'fasta':
            file_name_path_raw = file_fasta_i_move
            file_name_path_gz = file_fasta_i_move + '.gz'
            with open(file_name_path_raw, 'rb') as f_in:
                with gzip.open(file_name_path_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(file_name_path_raw)


# ===================================================================== #
#                       process entrez function
# ===================================================================== #

def download_nucleotide_by_entrez(args):
    try:
        gi_index, df_gi,folder_output_gi,file_type = args
        gi_i = int(df_gi['Gi_list'][gi_index])
        name_i = df_gi['Accession'][gi_index]
        if file_type == 'gb':
            link = 'efetch.fcgi?db=nuccore&id=' +str(gi_i) +'&rettype=gbwithparts&retmode=text'
            fileName = str(name_i) + '.gb'
        else:
            link = 'efetch.fcgi?db=nuccore&id=' +str(gi_i) +'&rettype=fasta&retmode=text'
            fileName = str(name_i) + '.fasta'
        cmd_eutils = 'wget -P '+  folder_output_gi +' "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/' + link +'"'
        processresult = subprocess.run(cmd_eutils, shell=True, capture_output=True)
        error_logs(cmd_eutils,processresult)
        os.rename(folder_output_gi+link, folder_output_gi + fileName) 
    except Exception as e_1:
        error_logs_try("Error -> {} in {}".format(e_1,name_i),e_1)


# ===================================================================== #
#                       process sratoolkit function
# ===================================================================== #

def download_sra_file(file_sra_i,sra_run_i):
    cmd_for_download_sra = prefetch_com +" -f yes -o " + file_sra_i + ' ' + sra_run_i
    processresult = subprocess.run(cmd_for_download_sra,shell=True, capture_output=True)
    error_logs(cmd_for_download_sra,processresult)

    if not os.path.exists(file_sra_i): # check file 
        i_loop = 1
        while  i_loop < count_loop:
            i_loop += 1
            sec_ran_reload = random.randint(30, 120)
            time.sleep(sec_ran_reload)
            processresult = subprocess.run(cmd_for_download_sra,shell=True, capture_output=True)
            error_logs(cmd_for_download_sra,processresult)
            if not os.path.exists(file_sra_i):
                pass
            else:
                break
    return cmd_for_download_sra

def download_fastq_file(file_sra_i,sra_run_i,folder_output_fastq,cmd_for_download_sra):
    cmd_fasterq_dump = fasterq_domp_com + " " + file_sra_i + " -O " + folder_output_fastq 
    processresult = subprocess.run(cmd_fasterq_dump, shell=True, capture_output=True)
    error_logs(cmd_fasterq_dump,processresult)

    files_in_directory = os.listdir(folder_output_fastq)
    files_contain_target = [file for file in files_in_directory if sra_run_i in file]
    total_files_fastq = len(files_contain_target)
    if total_files_fastq > 0:
        pass
    elif total_files_fastq == 0:
        if not os.path.exists(file_sra_i):
            processresult = subprocess.run(cmd_for_download_sra,shell=True, capture_output=True)
            error_logs(cmd_for_download_sra,processresult)
        else:
            cmd_fastq_dump = fastq_dump + " " + file_sra_i + " -O " + folder_output_fastq
            processresult = subprocess.run(cmd_fastq_dump, shell=True, capture_output=True)
            error_logs(cmd_fasterq_dump,processresult)
    else:
        pass
    if gz_file == True:
        try:
            for i in files_contain_target:
                file_name_path_raw = folder_output_fastq + str(i)
                file_name_path_gz = folder_output_fastq + str(i) + '.gz'
                with open(file_name_path_raw, 'rb') as f_in:
                    with gzip.open(file_name_path_gz, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                os.remove(file_name_path_raw)
        except Exception as e_1:
            error_logs_try("Error -> {} in {}".format(e_1,sra_run_i),e_1)

def process_sratoolkit_func(args):
    sra_index, df_sra_list,folder_output_sra,folder_output_fastq,file_type = args
    sra_run_i = df_sra_list['Run'][sra_index]
    file_sra_i = folder_output_sra + '/' + sra_run_i + '.sra'
    # download only sra
    try:
        cmd_for_download_sra = download_sra_file(file_sra_i,sra_run_i)
    except Exception as e:
        print("Error -> {} in {}".format(e,sra_run_i))
        error_logs_try("Error -> {} in {}".format(e,sra_run_i),e)
    # download fastq
    if  file_type == 'fastq' or file_type == 'all':
        try:
            # cmd_for_download_sra = download_sra_file(file_sra_i,sra_run_i)
            download_fastq_file(file_sra_i,sra_run_i,folder_output_fastq,cmd_for_download_sra)
        except Exception as e_all:
            print("Error -> {} in {}".format(e_all,sra_run_i))
            error_logs_try("Error -> {} in {}".format(e_all,sra_run_i),e_all)
 
# ===================================================================== #
#                       multidownload function
# ===================================================================== #

def sratoolkit_working(job_queue, result_queue):
    while True:
        job = job_queue.get()
        if job is None:
            break
        result = process_sratoolkit_func(job)
        result_queue.put(result)

def multi_download_sratoolkit(df_fastq,output_seq_file,number_core,file_type):
    folder_output_fastq = output_seq_file + '/fastq/'
    folder_output_sra = output_seq_file + '/sra/'
    if not os.path.exists(folder_output_fastq):
        os.mkdir(folder_output_fastq)
    if not os.path.exists(folder_output_sra):
        os.mkdir(folder_output_sra)

    job_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(processes=number_core, initializer=sratoolkit_working, initargs=(job_queue, result_queue))
    df_fastq_index = df_fastq.index.tolist()
    jobs =  [(index_, df_fastq,folder_output_sra,folder_output_fastq,file_type) for index_ in df_fastq_index]
    with tqdm(total=len(jobs), desc="Processing Jobs", ncols=70) as pbar:
        for job in jobs:
            job_queue.put(job)
        for _ in range(number_core):
            job_queue.put(None)
        results = []
        for _ in range(len(jobs)):
            result = result_queue.get()
            results.append(result)
            pbar.update(1)
    pool.close()
    pool.join()
    if file_type == "fastq":
        if os.path.exists(folder_output_sra):
            shutil.rmtree(folder_output_sra, ignore_errors = False)
    elif file_type == "sra":
        if os.path.exists(folder_output_fastq):
            shutil.rmtree(folder_output_fastq, ignore_errors = False)

def assembly_datasets(job_queue, result_queue):
    while True:
        job = job_queue.get()
        if job is None:
            break
        result = download_sequences_by_datasets(job)
        result_queue.put(result)

def multi_download_datasets(df_ssembly_list, output_seq_file,number_core,file_type):
    if file_type == 'gb':
        folder_output = output_seq_file + '/genbank_dataset/'
    else:
        folder_output = output_seq_file + '/fasta_dataset/'
    if not os.path.exists(folder_output):
        os.mkdir(folder_output)

    job_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(processes=number_core, initializer=assembly_datasets, initargs=(job_queue, result_queue))
    df_fasta_index = df_ssembly_list.index.tolist()
    jobs =  [(index_, df_ssembly_list,folder_output,file_type) for index_ in df_fasta_index]
    with tqdm(total=len(jobs), desc="Processing Jobs", ncols=70) as pbar:
        for job in jobs:
            job_queue.put(job)
        for _ in range(number_core):
            job_queue.put(None)
        results = []
        for _ in range(len(jobs)):
            result = result_queue.get()
            results.append(result)
            pbar.update(1)
    pool.close()
    pool.join()
    print("All Download Completed")

def entrez_working(job_queue, result_queue):
    while True:
        job = job_queue.get()
        if job is None:
            break
        result = download_nucleotide_by_entrez(job)
        result_queue.put(result)

def multi_download_entrez(df_gi,output_seq_file,number_core,file_type):
    if file_type == 'gb':
        folder_output = output_seq_file + '/genbank_entrez/'
    else:
        folder_output = output_seq_file + '/fasta_entrez/'
    if not os.path.exists(folder_output):
        os.mkdir(folder_output)

    job_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()
    pool = multiprocessing.Pool(processes=number_core, initializer=entrez_working, initargs=(job_queue, result_queue))
    df_gi_index = df_gi.index.tolist()
    jobs =  [(index_, df_gi,folder_output,file_type) for index_ in df_gi_index]
    with tqdm(total=len(jobs), desc="Processing Jobs", ncols=70) as pbar:
        for job in jobs:
            job_queue.put(job)
        for _ in range(number_core):
            job_queue.put(None)
        results = []
        for _ in range(len(jobs)):
            result = result_queue.get()
            results.append(result)
            pbar.update(1)
    pool.close()
    pool.join()