import re
import random
import os
import pickle
import socket
import sys
import json

from fn_socket import *


class Params():
    def __init__(self, path):
        with open(path) as params_file:
            params = json.load(params_file)
            self.__dict__.update(params)

params = Params('params.json')


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
    def __init__(self, num_nodes = params.num_nodes, num_replicas = params.num_replicas, num_chunks = params.num_chunks):
        self.num_nodes = num_nodes
        self.num_replicas = num_replicas
        self.num_chunks = num_chunks
        self.path = "dfs/namenode"
        self.load_meta()
        self.connection()

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
        Str = ''
        for key in self.index2filename:
            Str += ("{}: {} \n".format(key, self.index2filename[key]))
        return Str
            
            
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


    def connection(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        host = socket.gethostname() 
        self.port = 3333
        self.server.bind((host, self.port))
        self.server.listen(2)

    def client_server(self, conn, addr):
        print ('Accept connection from {0}'.format(addr))
        while 1:
            msgr = conn.recv(1024)
            msgr = msgr.decode('utf-8').lower()
            if msgr == 'exit':
                conn.close()
                break
            
            if 'upload' in msgr:
                print ('receive upload commad')
                _, filepath = msgr.split('#')
                print ('filepath: {}'.format(filepath))
                nodes_assigned, position_size = NameNode.upload_file(filepath)
                print ('nodes_assigned: {}'.format(nodes_assigned))
                print ('position_size: {}'.format(position_size))
                nodes_assigned_pk = pickle.dumps(nodes_assigned)
                position_size_pk = pickle.dumps(position_size)
                send_data(conn, nodes_assigned_pk)
                send_data(conn, position_size_pk)
                conn.sendall(str(list(self.index2filename.keys())[-1]).encode('utf-8'))

            if 'download' in msgr:
                print ('receive download commad')
                _, fileindex = msgr.split('#')
                fileindex = int(fileindex)
                print ('target file index: {}'.format(fileindex))
                filename = self.index2filename[fileindex]
                print ('target file name: {}'.format(filename))
                send_data(conn, filename.encode('utf-8'))
                chunk_node = self.download_file(fileindex)
                print ('chunk_node: ',chunk_node)
                chunk_node_pk = pickle.dumps(chunk_node)
                send_data(conn, chunk_node_pk)

            if 'ls' in msgr:
                print ('receive ls command')
                file_list = self.list_file()
                print (file_list)
                if file_list == '':
                    send_data(conn, b'No file in dfs')
                else:
                    send_data(conn, file_list.encode('utf-8'))

            if 'clear' in msgr:
                print ('receive clear command')
                self.clear()
                print ('clear!')

            if 'quit' in msgr:
                print ('close server')
                conn.close()
                break

            if 'state' in msgr:
                print ('receive state command')
                _, fileindex = msgr.split('#')
                fileindex = int(fileindex)
                print ('target file index: {}'.format(fileindex))
                filename = self.index2filename[fileindex]
                print ('target file name: {}'.format(filename))
                send_data(conn, filename.encode('utf-8'))
                chunk_node = self.file_storage[fileindex]
                print ('chunk_node: ',chunk_node)
                chunk_node_pk = pickle.dumps(chunk_node)
                send_data(conn, chunk_node_pk)

            if 'check' in msgr:
                print ('receive check command')
                file_storage_pk = pickle.dumps(self.file_storage)
                send_data(conn, file_storage_pk)

            if 'fetch' in msgr:
                _, fileindex, chunkindex = msgr.split('#')
                fileindex = int(fileindex)
                chunkindex = int(chunkindex)
                print ('target file index: {}'.format(fileindex))
                filename = self.index2filename[fileindex]
                print ('target file name: {}'.format(filename))
                send_data(conn, filename.encode('utf-8'))
                chunk_node = self.download_file(fileindex)[chunkindex]
                print ('chunk_node: ',chunk_node)
                send_data(conn, str(chunk_node).encode('utf-8'))






if __name__ == "__main__":
    NameNode = NameNode()
    conn, addr = NameNode.server.accept()
    NameNode.client_server(conn, addr)
