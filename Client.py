import os
from NameNode import NameNode
from DataNode import DataNode
import shutil 

import socket
import sys
import pickle 
import threading
import time
import os
import struct 
import json

from fn_socket import *

class Params():
    def __init__(self, path):
        with open(path) as params_file:
            params = json.load(params_file)
            self.__dict__.update(params)

params = Params('params.json')


class Client():
    '''
    Provide read/write interfaces of a file
    '''
    def __init__(self):
        self.num_nodes = params.num_nodes
        self.num_replicas = params.num_replicas
        self.num_chunks = params.num_chunks

        if not os.path.isdir("dfs"):
            os.makedirs("dfs")
            for i in range(self.num_nodes):
                os.makedirs("dfs/datanode%d"%i)
            os.makedirs("dfs/namenode")
        
        if not os.path.isdir("download"):
            os.makedirs("download")

        self.connection()

    def upload_file(self, filepath):
        if not os.path.isfile(filepath):
            print ('Error: input file does not exist')
            return
        self.namenode_conn.send(('upload#{}'.format(filepath)).encode('utf-8'))
        nodes_assigned_pk = recv_data(self.namenode_conn, False)
        position_size_pk = recv_data(self.namenode_conn, False)
        nodes_assigned = pickle.loads(nodes_assigned_pk)
        position_size =  pickle.loads(position_size_pk)
    #    print (nodes_assigned)
    #    print (position_size)
        file_index = int(self.namenode_conn.recv(1024).decode('utf-8'))
    #    print (file_index)
        file = open(filepath, 'rb')
        for i in range(self.num_chunks):
            chunk_index = i
            list_nodes = nodes_assigned[i]
            partfilename = str(file_index)+".part"+str(chunk_index)
            current_position, filepartsize = position_size[i]
            file.seek(current_position,0)
            partfilecontent = file.read(filepartsize)
            for node_index in list_nodes:
                DataNode_conn = self.datanodes_conn[node_index]
                send_data(DataNode_conn, ('upload#{}'.format(partfilename)).encode('utf-8'))
                print ('sending part file to node {}'.format(node_index))
                send_content(DataNode_conn, partfilecontent, filepartsize)
        print ('finish upload, file ID', file_index)
    
    def download_file(self, fileindex): 
        self.namenode_conn.send(('download#{}'.format(fileindex)).encode('utf-8'))
        filename = recv_data(self.namenode_conn)
        print ('receive filename: {}'.format(filename))
        filepath = "download/"+filename
        file = open(filepath, "wb")
        chunk_node_pk = recv_data(self.namenode_conn, False)
        chunk_node = pickle.loads(chunk_node_pk)

        for chunk_index in range(self.num_chunks):
            node_index = chunk_node[chunk_index]
            DataNode_conn = self.datanodes_conn[node_index]
            send_data(DataNode_conn, ('download#{}#{}'.format(fileindex, chunk_index)).encode('utf-8'))
            print ('receiving part file from node {}'.format(node_index))
            filepartcontent = recv_file(DataNode_conn)
            file.write(filepartcontent)
        file.close()
        print ('finish download')

    def fetch_file(self, fileindex, chunkindex):
        self.namenode_conn.send(('fetch#{}#{}'.format(fileindex, chunkindex)).encode('utf-8'))
        filename = recv_data(self.namenode_conn)
        print ('receive filename: {}'.format(filename))
        filepath = "download/"+filename + '.part'+str(chunkindex)
        chunk_node = recv_data(self.namenode_conn)
        chunk_node = int (chunk_node)
        print ('retrieve from node {}'.format(chunk_node))

        file = open(filepath, "wb")
        DataNode_conn = self.datanodes_conn[chunk_node]
        send_data(DataNode_conn, ('download#{}#{}'.format(fileindex, chunkindex)).encode('utf-8'))
        print ('receiving part file from node {}'.format(chunk_node))
        filepartcontent = recv_file(DataNode_conn)
        file.write(filepartcontent)
        file.close()
        print ('finish fetch')

    def file_state(self, fileindex):
        self.namenode_conn.send(('state#{}'.format(fileindex)).encode('utf-8'))
        filename = recv_data(self.namenode_conn)
        chunk_node_pk = recv_data(self.namenode_conn, False)
        chunk_node = pickle.loads(chunk_node_pk)
        print ('{} {}: {}'.format(fileindex,filename, chunk_node))

    def check(self):
        self.namenode_conn.send(('check').encode('utf-8'))
        file_storage_pk = recv_data(self.namenode_conn, False)
        file_storage = pickle.loads(file_storage_pk)
    #    print ('file storage: {}'.format(file_storage))
        node_file_storage = {}
        for node in range(self.num_nodes):
            node_file_storage[node] = {}
        for file_index in file_storage:
            for chunk_index in file_storage[file_index]:
                nodes_list = file_storage[file_index][chunk_index]
                for node in nodes_list:
                    if file_index in node_file_storage[node].keys():
                        node_file_storage[node][file_index].append(chunk_index)
                    else:
                        node_file_storage[node][file_index] = [chunk_index]
        print ('file storage by node: {}'.format(node_file_storage))
        for node in range(self.num_nodes):
            DataNode_conn = self.datanodes_conn[node]
            send_data(DataNode_conn, b'check')
            files_sto = node_file_storage[node]
            files_sto_pk = pickle.dumps(files_sto)
            send_data(DataNode_conn,files_sto_pk)
            result = int(recv_data(DataNode_conn))
            print ('node {} check result: {}'.format(node, 'dead' if result == 0 else 'working'))
            if result == 0:
                print ('recoverying node', node)
                self.node_recovery(file_storage, node_file_storage, node)
                print ('node {} recovery complet!'.format(node))


    def node_recovery(self, file_storage, node_file_storage, node):
        if os.path.isdir("dfs/datanode%d"%node):
            shutil.rmtree("dfs/datanode%d"%node)
        os.makedirs("dfs/datanode%d"%node)
        file_sto = node_file_storage[node]
        node_conn = self.datanodes_conn[node]
        for file_index in file_sto:
            for chunk in file_sto[file_index]:
                node_list = file_storage[file_index][chunk]
                for i in node_list:
                    if i != node:
                        help_node_conn = self.datanodes_conn[i]
                        send_data(help_node_conn, ('recov_help#{}#{}'.format(file_index, chunk)).encode('utf-8'))
                        filepartcontent = recv_file(help_node_conn)
                        filepartsize = len (filepartcontent)
                        send_data(node_conn, ('recovery#{}#{}'.format(file_index, chunk)).encode('utf-8'))
                        send_content(node_conn, filepartcontent, filepartsize)
                        break
            
        


    def clear(self):
        self.namenode_conn.send(b'clear')
        if os.path.isdir("dfs"):
                shutil.rmtree("dfs")
        if os.path.isdir("download"):
            shutil.rmtree("download") 
        os.makedirs("dfs") 
        for i in range(self.num_nodes):
            os.makedirs("dfs/datanode%d"%i)
        os.makedirs("dfs/namenode")
        os.makedirs("download")

    def mkdir(self,dirname):
        os.makedirs('download/'+dirname)

    def list_file(self):
        self.namenode_conn.send(b'ls')
     #   print("Waitting for recving message...")
        file_list = recv_data(self.namenode_conn)
        print (file_list)

    def quit(self):
        self.namenode_conn.send(b'quit')
        self.namenode_conn.close()
        for DataNode_conn in self.datanodes_conn:
            DataNode_conn.send(b'quit')
            DataNode_conn.close()


    def connection(self):
        self.namenode_port = 3333
        self.datanode_port = list(range(2222,2222+self.num_nodes))
        self.namenode_conn = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM) 
        host = socket.gethostname() 
        self.namenode_conn.connect((host, self.namenode_port))
        self.datanodes_conn = []
        for i in range(self.num_nodes):
            conn = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM) 
            conn.connect((host, self.datanode_port[i]))
            self.datanodes_conn.append(conn)
        print ('connect to name node and data nodes servers!')



if __name__ == "__main__":
    Client = Client()
    host = socket.gethostname() 
    Client.namenode_conn.connect((host, Client.namenode_port))
    Client.namenode_conn.send('successfully connected!'.encode('utf-8'))
    msg = Client.namenode_conn.recv(1024)
    print (msg.decode('utf-8'))
    Client.namenode_conn.send(b'exit')
    Client.namenode_conn.close()
    
        

