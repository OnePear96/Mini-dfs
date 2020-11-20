# Mini Distributed File System

A mini-dfs is implemented in python that is able to

- upload a file to the mini-dfs
- download a file from the mini-dfs 
- download certain chunk of a file
- list the files in the mini-dfs
- state the chunk storage of the files
- recover a data node from other data nodes

The parameters of the dfs are stored in `params.json`

The mini-dfs files are stored in the folder named `dfs`

`socket` is applied for the communiaction between client and the nodes

## Demonstration

![demo1](/Users/onepear/Documents/大数据课/project/Mini-dfs-dis/docs/demo1.gif)

![demo3](/Users/onepear/Documents/大数据课/project/Mini-dfs-dis/docs/demo3.gif)

![demo2](/Users/onepear/Documents/大数据课/project/Mini-dfs-dis/docs/demo2.gif)



## How to use

### Preparation: servers

1. Open N terminals for Data node servers (in our case, N=4),  in each terminal, type: 

   `python DataNode.py -n [number of node]`

2. Open 1 terminal for Name node server: 

   ` python NameNode.py`

3. Open 1 terminal for the client:

   `python run.py`



### Play with the dfs

1. list files

   `ls` 

2. upload file

   `upload [file_path]`

3. download file

   `download [file_index]`

4. fetch chunk of file

   `fetch [file_index] [chunk_index]` 

5. state storagy situation of a file

   `state [file_index]`

6. check data node storage, if dead recover it

   `check` 

7. clear the dfs

   `clear` 

8. quit the dfs

   `quit` 



have fun!