import os

is_windows = os.system("env\\bin\\activate && python main.py")

if is_windows == 1:
    os.system("env\\Scripts\\activate && python main.py")



directory = r"C:\Users\Llubr\Desktop\Github\ubuntu-production\scripts"

subfolders = [ f.path for f in os.scandir(directory) if f.is_dir() ]

