import re
import random
import os
import pickle


def getFileSize(file):
    file.seek(0, os.SEEK_END)
    fileLength = file.tell()
    file.seek(0, 0)
    return fileLength

class NameNode():
    '''
    List the relationships between file and chunks
    List the relationships between replicas and data servers
    Data server management
    '''
    def __init__(self, num_nodes, num_replicas, num_chunks):
        self.num_nodes = num_nodes
        self.num_replicas = num_replicas
        self.num_chunks = num_chunks
        self.path = "dfs/namenode"
        self.load_meta()

    def clear(self):
        self.file_storage = {}
        self.filename2index = {}
        self.index2filename = {}
        self.num_total_files = 0
        
    def load_meta(self):
        meta_path = self.path + "/metadata"
        if not os.path.isfile(meta_path):
            self.file_storage = {}
            self.filename2index = {}
            self.index2filename = {}
            self.num_total_files = 0
        else:
            with open(meta_path, 'rb') as f:
                metas = pickle.load(f)
            self.file_storage = metas['file_storage']
            self.filename2index = metas['filename2index']
            self.index2filename = metas['index2filename']
            self.num_total_files = metas['num_total_files']
        
    def update_meta(self):
        meta_path = self.path + "/metadata"
        metas = {}
        metas['file_storage'] = self.file_storage 
        metas['filename2index'] = self.filename2index 
        metas['index2filename'] = self.index2filename 
        metas['num_total_files'] = self.num_total_files 
        with open(meta_path, 'wb') as f:
            pickle.dump(metas, f)
        
    def upload_file(self,filepath):
        filename = re.sub(r'.+/','',filepath)
        self.filename2index[filename] = self.num_total_files
        self.index2filename[self.num_total_files] = filename
        
        l = list(range(self.num_nodes))
        nodes_assigned = {}
        for i in range(self.num_chunks):
            random.shuffle(l)
            nodes_assigned[i] = l[:self.num_replicas]
        self.file_storage[self.num_total_files] = nodes_assigned
        
        self.num_total_files += 1
        
        position_size = self.file_split_postion_size(filepath)
        self.update_meta()
        return nodes_assigned, position_size #position_size: 每个chunk对应的读取位置和读取size
        
        
    def download_file(self,file_index):
        nodes_assigned = self.file_storage[file_index]
        chunk_node = [-1]*self.num_chunks
        node_chunk = {}
        for i in range(self.num_chunks):
            chunk_node[i] = random.choice(nodes_assigned[i])
        return chunk_node
    
    def list_file(self):
        for key in self.index2filename:
            print ("{}: {}".format(key, self.index2filename[key]))
            
            
    def file_split_postion_size(self,filepath):
        file = open(filepath, 'rb')
        filesize = getFileSize(file)
        filepartsize = filesize // self.num_chunks
        position_size = []
        current_position = 0
        for i in range(self.num_chunks-1):
            position_size.append([current_position, filepartsize])
            current_position += filepartsize
        position_size.append([current_position, filesize - current_position])
        return position_size
