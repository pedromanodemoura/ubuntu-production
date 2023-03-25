import os

is_windows = os.system("env\\bin\\activate && python main.py")

if is_windows == 1:
    os.system("env\\Scripts\\activate && python main.py")