import os

class DataNode():
    '''
    Read/Write a local chunk
    Write a chunk via a local directory path
    '''
    def __init__(self,index):
        self.index = index
        self.path = "dfs/datanode%d"%index
    '''    
    def upload_file(self, file_index, chunk_index, position_size, file):
        partfilename = str(file_index)+".part"+str(chunk_index)
        partfilepath = self.path + "/" + partfilename
        current_position, filepartsize = position_size
        file.seek(current_position,0)
        content = file.read(filepartsize)
        with open(partfilepath, "wb") as filepart:
            filepart.write(content)
            filepart.flush()
    '''

    def upload_file(self,partfilename, partfilecontent):
        partfilepath = self.path + "/" + partfilename
        with open(partfilepath, "wb") as filepart:
            filepart.write(partfilecontent)
            filepart.flush()
    
    def download_file(self, file_index, chunk_index, file):
        partfilename = str(file_index)+".part"+str(chunk_index)
        partfilepath = self.path + "/" + partfilename
        if not os.path.isfile(partfilepath):
            print ("file or chunk not in this Node !")
            return
        filepart = open(partfilepath, 'rb')
        file.write(filepart.read())
        filepart.close()
        