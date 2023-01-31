import os
os.system("wget --output-document sratoolkit.tar.gz https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/current/sratoolkit.current-ubuntu64.tar.gz")
os.system("tar -vxzf sratoolkit.tar.gz")
os.system("curl -o datasets 'https://ftp.ncbi.nlm.nih.gov/pub/datasets/command-line/v2/linux-amd64/datasets'")

list_dir =  [name for name in os.listdir(".") if os.path.isdir(name)]

for i in list_dir:
    if 'sratoolkit' in i:
        os.rename(i, "sratoolkit")