import socket
import struct
import copy
import os
import hashlib


def send_data(conn,data):
    conn.sendall(data)
    msg = conn.recv(2048).decode('utf-8')
#    if msg.lower() == 'r':
#        print ('data successfully send')
    return


def recv_data(conn, decode = True):
    data = conn.recv(2048)
    if decode:
        data = data.decode('utf-8')
    conn.send(b'r')
    return data


def send_content(conn, filecontent, content_size):
    m = hashlib.md5() 
    conn.send(str(content_size).encode(encoding='utf-8'))
    ack = conn.recv(1024)
#    print('filesize:',content_size)
    i = 0
    while 1:
        if (i+1)*1024 < content_size:
            data = filecontent[i*1024:(i+1)*1024]
        else:
            data = filecontent[i*1024:content_size]
        if not data:
        #    print ('file send over...')
            break
        m.update(data)
        conn.send(data)
        i += 1

#    print('server send file md5:',m.hexdigest())
    conn.send(m.hexdigest().encode(encoding='utf-8'))#
    ack = conn.recv(1024)
#    print("send done")


def send_file(conn, filepath):
    if not os.path.isfile(filepath):
        raise Exception('file not exist')
    f = open(filepath,'rb')
    m = hashlib.md5()
    filesize = os.stat(filepath).st_size
    
    conn.send(str(filesize).encode(encoding='utf-8'))
    ack = conn.recv(1024)
#    print('filesize:',filesize)
    while 1:
        data = f.read(1024)
        if not data:
        #    print ('{} file send over...'.format(filepath))
            break
        m.update(data)
        conn.send(data)

#    print('server send file md5:',m.hexdigest())
    f.close()
    conn.send(m.hexdigest().encode(encoding='utf-8'))#
    ack = conn.recv(1024)
#    print("send done")


def recv_file(conn):
    file_total_size = int(conn.recv(1024).decode())
#    print("part file size:",file_total_size)
    conn.send(b'recv')
    recv_size = 0
    recv_data = b''
    count = 0
    m = hashlib.md5()

#    print ('start receiving.....')

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
        conn.send(b'recv')
    #    print("file recv done {}/{}".format(recv_size,file_total_size))
        print("new_file_md5:",new_file_md5)
        print('server_datamd5:',server_datamd5)
        if new_file_md5 == server_datamd5:
            print ('md5 code affirms same file')
        else:
            print ('md5 code mismatch')

    return recv_data