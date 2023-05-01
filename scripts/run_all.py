# In[2]:
import glob
import os

start_dir = os.path.dirname(os.path.abspath(__file__))

projects = [f.path.split('\\')[-1] for f in os.scandir(start_dir) if f.is_dir()]

# In[2]:
for project in projects:
    print(f"Running {project}")
    if os.name == 'nt':
        os.system(f"cd {project} && env\\Scripts\\activate && python run_windows.py")
    elif os.name == 'posix':
        os.system(f"cd {project} && env\\bin\\activate && python run_ubuntu.py")