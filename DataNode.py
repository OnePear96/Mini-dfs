import os
import argparse
from fn_socket import *
import pickle 
import json



class Params():
    def __init__(self, path):
        with open(path) as params_file:
            params = json.load(params_file)
            self.__dict__.update(params)

params = Params('params.json')



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


    def upload_file(self,partfilename, partfilecontent):
        partfilepath = self.path + "/" + partfilename
        with open(partfilepath, "wb") as filepart:
            filepart.write(partfilecontent)
            filepart.flush()

    def download_file(self, file_index, chunk_index):
        partfilename = str(file_index)+".part"+str(chunk_index)
        partfilepath = self.path + "/" + partfilename
        if not os.path.isfile(partfilepath):
            print ("file or chunk not in this Node !")
            return
        return partfilepath

    def connection(self):
        num_nodes = params.num_nodes
        datanode_port = list(range(2222,2222+num_nodes))
        if self.index >= num_nodes:
            raise Exception('DataNode index out of range')
        self.port = datanode_port[index]
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        host = socket.gethostname() 
        self.server.bind((host, self.port))
        self.server.listen(2)

    def check(self, file_sto):
        print ('file storage in this node: ', file_sto)
        if not os.path.isdir(self.path):
            return 0
        file_list = os.listdir(self.path)
        print ('file list in this node: ', file_list)
        for file_index in file_sto:
            for chunk_index in file_sto[file_index]:
                partfilename = str(file_index)+".part"+str(chunk_index)
                if partfilename not in file_list:
                    print (partfilename, 'not in node!') 
                    return 0
        return 1

    def client_server(self, conn, addr):
        print ('Accept connection from {0}'.format(addr))
        while 1:
            msgr = recv_data(conn, decode = False)
            msgr = msgr.decode('utf-8').lower()
        #    conn.send(b'r')
            print ('receive message: {}'.format(msgr))
            if msgr == 'exit':
                conn.close()
                break

            if 'download' in msgr:
                print ('receive download commad')
                _, fileindex, chunk_index = msgr.split('#')
                fileindex = int(fileindex)
                chunk_index = int(chunk_index)
                print ('target file index: {}, chunk index: {}'.format(fileindex, chunk_index))
                partfilepath = self.download_file(fileindex, chunk_index)
                send_file(conn, partfilepath)

            if 'upload' in msgr:
                print ('receive upload commad')
                _, partfilename = msgr.split('#')
                print ('target partfilename: {}'.format(partfilename))
                filepartcontent = recv_file(conn)
                self.upload_file(partfilename,filepartcontent)

            if 'check' in msgr:
                print ('receive check commad')
                files_sto_pk = recv_data(conn, False)
                file_sto = pickle.loads(files_sto_pk)
                result = self.check(file_sto)
                print ('check result:',result)
                result = str(result).encode('utf-8')
                send_data(conn, result)

            if 'recov_help' in msgr:
                print ('receive recov_help commad')
                _, file_index, chunk = msgr.split('#')
                file_index = int(file_index)
                chunk = int(chunk)
                print ('target file index: {}, chunk index: {}'.format(file_index, chunk))
                partfilepath = self.download_file(file_index, chunk)
                send_file(conn, partfilepath)

            if 'recovery' in msgr:
                print ('receive recovery commad')
                _, file_index, chunk = msgr.split('#')
                partfilename = str(file_index)+".part"+str(chunk)
                filepartcontent = recv_file(conn)
                self.upload_file(partfilename,filepartcontent)

            if 'quit' in msgr:
                print ('close server')
                conn.close()
                break






if __name__ == "__main__":
    index = args.number
    print ('create DataNode of index {}'.format(index))
    DataNode = DataNode(index)
    conn, addr = DataNode.server.accept()
    DataNode.client_server(conn, addr)
    
    

