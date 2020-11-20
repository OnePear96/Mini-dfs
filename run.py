from Client import Client
from NameNode import NameNode
from DataNode import DataNode
from command import process_cmd

cmd_prompt = 'Mini-DFS >> '
openrations = {"upload", "download", 'ls', 'quit'}



if __name__ == "__main__":
    Client = Client()
    while True:
        print (cmd_prompt, end = '')
        cmd_str = input()
        action, content = process_cmd(cmd_str)
    #   print ('action: {}, content: {}'.format(action, content))
        if action == "upload":
            Client.upload_file(content)
        if action == "download":
            Client.download_file(content)
        if action == "ls":
            Client.list_file()
        if action == 'clear':
            Client.clear()
        if action == 'state':
            Client.file_state(content)
        if action == 'check':
            Client.check()
        if action == 'fetch':
            Client.fetch_file(content[0], content[1])
        if action == 'quit':
            Client.quit()
            break

