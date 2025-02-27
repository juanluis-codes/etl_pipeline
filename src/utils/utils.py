import os

class dir_utils:
    def recursive_deletion(path):
            for root, dirs, files in os.walk(path, topdown=False):
                # Deleting files
                for name in files:
                    os.remove(os.path.join(root,name))
                # Deleting dirs
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            # Delete the root path
            os.rmdir(path)  