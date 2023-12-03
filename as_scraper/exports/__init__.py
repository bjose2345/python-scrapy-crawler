import os
import dotenv
## default directory for .env file is the current directory
## if you set .env in different directory, put the directory address load_dotenv("directory_of_.env)
dotenv.load_dotenv()
__all__ = []
dirname = os.path.dirname(os.path.abspath(__file__))

for f in os.listdir(dirname):
    if f != "__init__.py" and os.path.isfile("%s/%s" % (dirname, f)) and f[-3:] == ".py":
        __all__.append(f[:-3])