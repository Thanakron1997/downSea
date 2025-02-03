import os
# dir_path = os.path.dirname(os.path.realpath(__file__)) +'/'
dir_path = os.path.dirname(os.path.realpath(__file__))  # Current script directory
mother_path = os.path.dirname(dir_path) + '/'  # Parent directory
print(dir_path)
print(mother_path)