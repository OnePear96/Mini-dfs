import os
from NameNode import NameNode
from DataNode import DataNode
import shutil 


class Client():
    '''
    Provide read/write interfaces of a file
    '''
    def __init__(self):
        self.N = 4
        self.num_replicas = 3
        self.num_chunks = 3
        self.NameNode = NameNode(num_nodes = self.N, num_replicas = self.num_replicas, num_chunks = self.num_chunks)
        self.DataNodes = []
        for i in range(self.N):
            self.DataNodes.append(DataNode(index = i))

        if not os.path.isdir("dfs"):
            os.makedirs("dfs")
            for i in range(self.N):
                os.makedirs("dfs/datanode%d"%i)
            os.makedirs("dfs/namenode")
        
        if not os.path.isdir("download"):
            os.makedirs("download")

    def upload_file(self, filepath):
        if not os.path.isfile(filepath):
            print ('Error: input file does not exist')
            return
        nodes_assigned, position_size = self.NameNode.upload_file(filepath)
        file_index = list(self.NameNode.index2filename.keys())[-1]
        file = open(filepath, 'rb')
        for i in range(self.num_chunks):
            chunk_index = i
            list_nodes = nodes_assigned[i]
            partfilename = str(file_index)+".part"+str(chunk_index)
            current_position, filepartsize = position_size[i]
            file.seek(current_position,0)
            partfilecontent = file.read(filepartsize)
            for node_index in list_nodes:
                Node = self.DataNodes[node_index]
                Node.upload_file(partfilename = partfilename, partfilecontent = partfilecontent)
    
    def download_file(self, fileindex): #or file index
        filename = self.NameNode.index2filename[fileindex]
        filepath = "download/"+filename
        file = open(filepath, "wb")
        chunk_node = self.NameNode.download_file(fileindex)
        for chunk_index in range(self.num_chunks):
            Node = self.DataNodes[chunk_node[chunk_index]]
            Node.download_file(fileindex, chunk_index, file)
        file.close()


    def clear(self):
        self.NameNode.clear()
        if os.path.isdir("dfs"):
                shutil.rmtree("dfs")
        if os.path.isdir("download"):
            shutil.rmtree("download") 
        os.makedirs("dfs") 
        for i in range(self.N):
            os.makedirs("dfs/datanode%d"%i)
        os.makedirs("dfs/namenode")
        os.makedirs("download")
        