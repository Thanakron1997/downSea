import os
import subprocess
import shutil
import random
import time
import gzip
import json
from tqdm import tqdm 
import multiprocessing
from src.errorlog import errorsLog

class sratoolkitMode:
    def __init__(self,
                program_path : str = None,
                gz_file : bool = None,
                count_loop: int = None,
                ):
        file_path = os.path.dirname(os.path.realpath(__file__))
        dir_path = os.path.dirname(file_path) + '/'
        with open(os.path.join(file_path,"config.json"), 'r') as f:
            config = json.load(f)
        if program_path:
            self.fasterq_domp_com =  os.path.join(program_path,'sratoolkit/bin/fasterq-dump')
            self.prefetch_com = os.path.join(program_path,'sratoolkit/bin/prefetch')
            self.fastq_dump = os.path.join(program_path,'sratoolkit/bin/fastq-dump')
        else:
            self.fasterq_domp_com = os.path.join(dir_path,'sratoolkit/bin/fasterq-dump')
            self.prefetch_com = os.path.join(dir_path,'sratoolkit/bin/prefetch')
            self.fastq_dump = os.path.join(dir_path,'sratoolkit/bin/fastq-dump')
        if gz_file:
            self.gz_file = gz_file
        else:
            self.gz_file = config['gz_file']
        if count_loop:
            self.count_loop = count_loop
        else:
            self.count_loop = config['count_loop']
        self.errorsLogFun = errorsLog()

# sratoolkit output change to dir 
    def download_sra_file(self,output_dir,sra_i):
        cmd_for_download_sra = f"{self.prefetch_com} --force yes --output-directory {output_dir} {sra_i}"
        processresult = subprocess.run(cmd_for_download_sra,shell=True, capture_output=True)
        self.errorsLogFun.error_logs(cmd_for_download_sra,processresult)
        sra_file_i = os.path.join(output_dir,f"{sra_i}/{sra_i}.sra")
        if not os.path.exists(sra_file_i): # check file 
            i_loop = 1
            while  i_loop < self.count_loop:
                i_loop += 1
                sec_ran_reload = random.randint(30, 120)
                time.sleep(sec_ran_reload)
                processresult = subprocess.run(cmd_for_download_sra,shell=True, capture_output=True)
                self.errorsLogFun.error_logs(cmd_for_download_sra,processresult)
                if os.path.exists(sra_file_i):
                    break
        return cmd_for_download_sra,sra_file_i

    def download_fastq_file(self,sra_file_i,sra_i,output_dir,cmd_sra):
        cmd_fasterq_dump = f"{self.fasterq_domp_com} {sra_file_i} --outdir {output_dir}"
        processresult = subprocess.run(cmd_fasterq_dump, shell=True, capture_output=True)
        self.errorsLogFun.error_logs(cmd_fasterq_dump,processresult)

        files_in_directory = os.listdir(output_dir)
        files_contain_target = [file for file in files_in_directory if sra_i in file]
        total_files_fastq = len(files_contain_target)
        if total_files_fastq > 0:
            pass
        elif total_files_fastq == 0:
            if not os.path.exists(sra_file_i):
                processresult = subprocess.run(cmd_sra,shell=True, capture_output=True)
                self.errorsLogFun.error_logs(cmd_sra,processresult)
            else:
                cmd_fastq_dump = f"{self.fastq_dump} {sra_file_i} --split-3 --outdir {output_dir}"
                processresult = subprocess.run(cmd_fastq_dump, shell=True, capture_output=True)
                self.errorsLogFun.error_logs(cmd_fasterq_dump,processresult)
        else:
            pass
        if self.gz_file == True:
            try:
                files_in_directory = os.listdir(output_dir)
                files_contain_target = [file for file in files_in_directory if sra_i in file]
                for i in files_contain_target:
                    file_name_path_raw = output_dir + str(i)
                    file_name_path_gz = output_dir + str(i) + '.gz'
                    with open(file_name_path_raw, 'rb') as f_in:
                        with gzip.open(file_name_path_gz, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    os.remove(file_name_path_raw)
            except Exception as e_1:
                self.errorsLogFun.error_logs_try("Error -> {} in {}".format(e_1,sra_i),e_1)

    def process_sratoolkit_func(self,args):
        try:
            data_i,folder_output_sra,folder_output_fastq,file_type = args
            # download only sra
            try:
                cmd_for_download_sra,sra_file_i = self.download_sra_file(output_dir=folder_output_sra,sra_i=data_i)
            except Exception as e:
                self.errorsLogFun.error_logs_try("Error -> {} in {}".format(e,data_i),e)
            # download fastq
            if  file_type == 'fastq' or file_type == 'all':
                try:
                    self.download_fastq_file(sra_file_i=sra_file_i,sra_i=data_i,output_dir=folder_output_fastq,cmd_sra=cmd_for_download_sra)
                except Exception as e_all:
                    self.errorsLogFun.error_logs_try("Error -> {} in {}".format(e_all,data_i),e_all)
            if file_type != 'sra':
                try:
                    shutil.rmtree(os.path.join(folder_output_sra,data_i), ignore_errors = True)
                    os.remove(sra_file_i)
                except:
                    pass
            return data_i
        except:
            return "Error!"
 
    def sratoolkit_working(self,job_queue, result_queue):
        while True:
            job = job_queue.get()
            if job is None:
                break
            result = self.process_sratoolkit_func(job)
            result_queue.put(result)

    def multi_download_sratoolkit(self,list_data,output_seq_file,number_core,file_type):
        folder_output_fastq = output_seq_file + '/fastq/'
        folder_output_sra = output_seq_file + '/sra/'
        if not os.path.exists(folder_output_fastq):
            os.mkdir(folder_output_fastq)
        if not os.path.exists(folder_output_sra):
            os.mkdir(folder_output_sra)

        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()
        pool = multiprocessing.Pool(processes=number_core, initializer=self.sratoolkit_working, initargs=(job_queue, result_queue))
        jobs =  [(data_i,folder_output_sra,folder_output_fastq,file_type) for data_i in list_data]
        with tqdm(total=len(jobs), desc="Downloading sequences", ncols=100,colour="#00FF21",leave=True) as pbar:
            for job in jobs:
                job_queue.put(job)
            for _ in range(number_core):
                job_queue.put(None)
            for _ in range(len(jobs)):
                result = result_queue.get()
                pbar.update(1)
                pbar.set_postfix(completed_sequence=result, refresh=True)
        pool.close()
        pool.join()
        if file_type == "fastq":
            if os.path.exists(folder_output_sra):
                shutil.rmtree(folder_output_sra, ignore_errors = False)
        elif file_type == "sra":
            if os.path.exists(folder_output_fastq):
                shutil.rmtree(folder_output_fastq, ignore_errors = False)
            