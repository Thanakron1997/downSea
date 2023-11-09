import os
import datetime
main = os.path.dirname(os.path.realpath(__file__)) 
log_file_path = main + "error_log.txt"

def error_logs(cmd,result):
    if result.returncode != 0:
    # Save the error to the log file
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file_path, "a") as log:
            log.write(f"{current_time} - Error executing command: {cmd}\n")
            log.write(f"{current_time} - Return code: {result.returncode}\n")
            log.write(f"{current_time} - Error output: {result.stderr.decode() }\n")
            log.close()

def error_logs_try(command,e):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file_path, "a") as log:
            log.write(f"{current_time} - Error executing command: {command}\n")
            log.write(f"{current_time} - Exception: {e}\n")
            log.close()