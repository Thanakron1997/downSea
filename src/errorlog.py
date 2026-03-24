import traceback
import datetime
import json
import os

class errorsLog:
    def __init__(self,
                logFile: str = None,
                ):
        if logFile:
            self.log_file_path = logFile
        else:
            file_path = os.path.dirname(os.path.realpath(__file__)) + '/'
            with open(os.path.join(file_path,"config.json"), 'r') as f:
                config = json.load(f)
                self.log_file_path = os.path.join(file_path,config["logFIle"])

    def error_logs(self,cmd,result):
        if result.returncode != 0:
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file_path, "a") as log:
                log.write(f"{current_time} - Error executing command: {cmd}\n")
                log.write(f"{current_time} - Return code: {result.returncode}\n")
                log.write(f"{current_time} - Error output: {result.stderr.decode() }\n")
                log.close()


    def error_logs_try(self,command,e):
        traceback_info = traceback.format_exc()
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file_path, "a") as log:
                log.write(f"{current_time} - Error executing command: {command}\n")
                log.write(f"{current_time} - Exception: {e}\n")
                log.write(f"{current_time} - Traceback:\n{traceback_info}\n")
                log.close()