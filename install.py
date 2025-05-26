import subprocess
import sys
import os

def install_if_missing(package):
    try:
        __import__(package)
        print(f"'{package}' is already installed.")
    except ImportError:
        print(f"'{package}' not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

os.system("wget --output-document sratoolkit.tar.gz https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz")
os.system("tar -vxzf sratoolkit.tar.gz")
os.system("curl -o datasets 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets'")
os.system("chmod +x datasets")
list_dir =  [name for name in os.listdir(".") if os.path.isdir(name)]

for i in list_dir:
    if 'sratoolkit' in i:
        os.rename(i, "sratoolkit")

# Check and install pandas and tqdm
install_if_missing('pandas')
install_if_missing('tqdm')
