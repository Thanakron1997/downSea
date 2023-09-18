import os
import re
import shutil
for directory in os.listdir() :
    if re.fullmatch('fasterq.tmp.ad0-Precision-3630-Tower.*',directory):
        shutil.rmtree(directory)