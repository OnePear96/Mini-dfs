#python NameNode.py
#python DataNode.py -n 0
#python DataNode.py -n 1
#python run.py

gnome-terminal -t "NameNode" -x bash -c "python NameNode.py;exec bash;"
gnome-terminal -t "DataNode 0" -x bash -c "python DataNode.py -n 0;exec bash;"
gnome-terminal -t "DataNode 1" -x bash -c "python DataNode.py -n 1;exec bash;"
gnome-terminal -t "Client" -x bash -c "python run.py;exec bash;"

#open -a Terminal.app python NameNode.py