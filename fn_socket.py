import socket
import struct
import copy
import os
import hashlib


def send_data(conn,data):
    conn.sendall(data)
    msg = conn.recv(1024).decode('utf-8')
    if msg.lower() == 'r':
        print ('data successfully send')
    return


def recv_data(conn, decode = True):
    data = conn.recv(1024)
    if decode:
        data = data.decode('utf-8')
#    print ("data: ",data)
#    data = copy.deepcopy(data)
    conn.send(b'r')
    return data


def send_file(conn, filepath):
    if not os.path.isfile(filepath):
        raise Exception('file not exist')
    f = open(filepath,'rb')
    m = hashlib.md5()
    filesize = os.stat(filepath).st_size
    
    conn.send(str(filesize).encode(encoding='utf-8'))
#    ack = conn.recv(1024)
    print('filesize:',filesize)
    '''
    for line in f:
        m.update(line)#.encode(encoding='utf-8')
        conn.send(line)#.encode(encoding='utf-8')
    '''
    while 1:
        data = f.read(1024)
        if not data:
            print ('{} file send over...'.format(filepath))
            break
        m.update(data)
        conn.send(data)

    print('server send file md5:',m.hexdigest())
    f.close()
    conn.send(m.hexdigest().encode(encoding='utf-8'))#
    print("send done")


def recv_file(conn):
    file_total_size = int(conn.recv(1024).decode())
    print("file_total_size:",file_total_size)
    recv_size = 0
    recv_data = b''
    count = 0
 #   filename = send_cmd.split()[1]
 #   f = open(filename + 'new','wb')
    m = hashlib.md5()

    print ('start receiving.....')

    while not recv_size == file_total_size:
        if file_total_size - recv_size > 1024:
            data = conn.recv(1024)
            recv_size += len(data)
            recv_data += data
        else:
            data = conn.recv(file_total_size - recv_size)
            recv_size = file_total_size
            recv_data += data
        m.update(data)
     #   file.write(data)
    
    else:
        new_file_md5 = m.hexdigest()
        server_datamd5 = conn.recv(1024).decode()
        print("file recv done {}/{}".format(recv_size,file_total_size))
        print("new_file_md5:",new_file_md5)
        print('server_datamd5:',server_datamd5)
        if new_file_md5 == server_datamd5:
            print ('md5 code affirms same file')
        else:
            print ('md5 code mismatch')
    
#    print ('end receive ...')
    '''
    while recv_size != file_total_size:
        if file_total_size - recv_size > 1024:
            size = 1024
        else:
            size = file_total_size - recv_size
            print('last recv_size:',recv_size)
        data = client.recv(size)
        recv_size += len(data)
        m.update(data)
        f.write(data)
        f.flush()
        print('recv_size:',recv_size,'file_total_size:',file_total_size)
    else:
        new_file_md5 = m.hexdigest()
        server_datamd5 = client.recv(1024).decode()
        print("file recv done",recv_size,file_total_size)
        print("new_file_md5:",new_file_md5)
        print('server_datamd5:',server_datamd5)
    '''

    return recv_data