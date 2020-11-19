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

from fn_socket import *


class Client():
    '''
    Provide read/write interfaces of a file
    '''
    def __init__(self):
        self.num_nodes = 2
        self.num_replicas = 2
        self.num_chunks = 2
        '''
        self.NameNode = NameNode(num_nodes = self.num_nodes, num_replicas = self.num_replicas, num_chunks = self.num_chunks)
        
        self.DataNodes = []
        for i in range(self.num_nodes):
            self.DataNodes.append(DataNode(index = i))
        '''
        
        
    #    self.datanode_conn = []

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
     #   nodes_assigned_pk = self.namenode_conn.recv(1024)
     #   position_size_pk = self.namenode_conn.recv(1024)
        nodes_assigned_pk = recv_data(self.namenode_conn, False)
        position_size_pk = recv_data(self.namenode_conn, False)
        nodes_assigned = pickle.loads(nodes_assigned_pk)
        position_size =  pickle.loads(position_size_pk)
        print (nodes_assigned)
        print (position_size)
    #    nodes_assigned, position_size = self.NameNode.upload_file(filepath)
    #    file_index = list(self.NameNode.index2filename.keys())[-1]
        file_index = int(self.namenode_conn.recv(1024).decode('utf-8'))
        print (file_index)
        file = open(filepath, 'rb')
        for i in range(self.num_chunks):
            chunk_index = i
            list_nodes = nodes_assigned[i]
            partfilename = str(file_index)+".part"+str(chunk_index)
            current_position, filepartsize = position_size[i]
            file.seek(current_position,0)
            partfilecontent = file.read(filepartsize)
            for node_index in list_nodes:
            #    Node = self.DataNodes[node_index]
            #    Node.upload_file(partfilename = partfilename, partfilecontent = partfilecontent)
                DataNode_conn = self.datanodes_conn[node_index]
            #    DataNode_conn.send(('upload#{}'.format(partfilename)).encode('utf-8'))
                send_data(DataNode_conn, ('upload#{}'.format(partfilename)).encode('utf-8'))
                send_content(DataNode_conn, partfilecontent, filepartsize)
    
    def download_file(self, fileindex): #or file index
    #    self.namenode_conn.send(b'download')
    #    self.namenode_conn.send(str(fileindex).encode('utf-8'))
        self.namenode_conn.send(('download#{}'.format(fileindex)).encode('utf-8'))
    #    filename = self.NameNode.index2filename[fileindex]
    #    filename = self.namenode_conn.recv(1024).decode('utf-8')
        filename = recv_data(self.namenode_conn)
        print ('receive filename: {}'.format(filename))
        filepath = "download/"+filename
        file = open(filepath, "wb")
    #    chunk_node = self.NameNode.download_file(fileindex)
    #    chunk_node_pk = self.namenode_conn.recv(1024)
        chunk_node_pk = recv_data(self.namenode_conn, False)
        chunk_node = pickle.loads(chunk_node_pk)

        for chunk_index in range(self.num_chunks):
         #   Node = self.DataNodes[chunk_node[chunk_index]]
         #   filepartcontent = Node.download_file(fileindex, chunk_index)
            node_index = chunk_node[chunk_index]
            DataNode_conn = self.datanodes_conn[node_index]
            send_data(DataNode_conn, ('download#{}#{}'.format(fileindex, chunk_index)).encode('utf-8'))
        #    DataNode_conn.send(('download#{}#{}'.format(fileindex, chunk_index)).encode('utf-8'))
            filepartcontent = recv_file(DataNode_conn)
            file.write(filepartcontent)
        file.close()


    def clear(self):
    #    self.NameNode.clear()
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

    def list_file(self):
        self.namenode_conn.send(b'ls')
        print("Waitting for recving message...")
    #    file_list = self.namenode_conn.recv(1024).decode('utf-8')
        file_list = recv_data(self.namenode_conn)
    #    file_list = file_list.decode('utf-8')
        print (file_list)

    def quit(self):
        self.namenode_conn.send(b'quit')
        self.namenode_conn.close()


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

        for i in self.datanodes_conn:
            print (i)


        '''
    def connect_namenode(self):
        self.conn = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM) 
        host = socket.gethostname()
        self.conn.connect((host, self.namenode_port))

    def connect_datanode(self,id):
        self.conn = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM) 
        datanode_port = self.datanode_port[id]
        host = socket.gethostname()
        self.conn.connect((host, datanode_port))
        '''


if __name__ == "__main__":
    Client = Client()
    host = socket.gethostname() 
    Client.namenode_conn.connect((host, Client.namenode_port))
    Client.namenode_conn.send('successfully connected!'.encode('utf-8'))
    msg = Client.namenode_conn.recv(1024)
    print (msg.decode('utf-8'))
    Client.namenode_conn.send(b'exit')
    Client.namenode_conn.close()
    
        

'''
    def connection(self):
        self.client_node_conn = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM) 
        host = socket.gethostname()
        port = 9999
        self.client_node_conn.bind((host, port))
        self.client_node_conn.listen(6)
        for i in range(self.N+1):
            conn, addr = self.client_node_conn.accept()
        #    t = threading.Thread(target=client_server, args=(conn, addr))
        #    t.start()


    def _client_server(self,conn, addr):
        name =  conn.recv(1024).decode('utf-8').lower()
        print ('Accept new connection from {0}, address: {}'.format(name,addr.decode('utf-8'))
        #conn.settimeout(500)
        conn.sendall('Successfully conneted to the client node!'.encode('utf-8'))
        if 'namenode' in name:
            self.name_node_conn = conn
        elif 'datanode' in name:
            id = len(self.datanode_conn)
            self.datanode_conn.append(conn)
            conn.send(id)

    '''

