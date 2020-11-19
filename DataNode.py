import os
import argparse
from fn_socket import *



parser = argparse.ArgumentParser(description='Input the number of this DataNode Server')
parser.add_argument('-n', '--number', help = 'number of the server', type = int)
args = parser.parse_args()

class DataNode():
    '''
    Read/Write a local chunk
    Write a chunk via a local directory path
    '''
    def __init__(self,index):
        self.index = index
        self.path = "dfs/datanode%d"%index
        self.connection()
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
    
    '''
    def download_file(self, file_index, chunk_index, file):
        partfilename = str(file_index)+".part"+str(chunk_index)
        partfilepath = self.path + "/" + partfilename
        if not os.path.isfile(partfilepath):
            print ("file or chunk not in this Node !")
            return
        filepart = open(partfilepath, 'rb')
        file.write(filepart.read())
        filepart.close()
    '''
    def download_file(self, file_index, chunk_index):
        partfilename = str(file_index)+".part"+str(chunk_index)
        partfilepath = self.path + "/" + partfilename
        if not os.path.isfile(partfilepath):
            print ("file or chunk not in this Node !")
            return
        filepart = open(partfilepath, 'rb')
        content = filepart.read()
        filepart.close()
        return content

    def connection(self):
        num_nodes = 4
        datanode_port = list(range(2222,2222+num_nodes))
        if self.index >= num_nodes:
            raise Exception('DataNode index out of range')
        self.port = datanode_port[index]
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        host = socket.gethostname() 
        self.server.bind((host, self.port))
        self.server.listen(2)



if __name__ == "__main__":
    index = args.number
    print ('create DataNode of index {}'.format(index))
    DataNode = DataNode(index)
    
    

