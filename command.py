import re

cmd_prompt = 'Mini-DFS >> '
openrations = {"upload", "download", 'ls', 'quit', 'clear'}

def process_cmd(cmd_str):
    cmd = cmd_str.split()
    if len(cmd) == 0:
        print ('Usage: \n upload file_path \n download file_index \n ls \n quit')
        return None, None
    action = None
    content = None
    if cmd[0].lower() == "upload":
        action = "upload"
        if len(cmd) < 2:
            print ('Usage: \n upload file_path \n download file_index \n ls \n quit')
            return None, None
        content = cmd[1]

    elif cmd[0].lower() == "download":
        action = "download"
        try:
            content = int(cmd[1])
        except:
            print ('Usage: \n upload file_path \n download file_index \n ls \n quit')
            return None, None
    
    elif cmd[0].lower() == 'ls':
        action = 'ls'
        content = None

    elif cmd[0].lower() == 'quit' or cmd[0].lower() == 'exit':
        action = 'quit'
        content = None

    elif cmd[0].lower() == 'clear':
        action = 'clear'
        content = None

    else: 
        print ('Usage: \n upload file_path \n download file_index \n ls \n quit')
        return None, None
    
    return action, content


if __name__ == "__main__":
    #print ('Usage: \n upload file_path \n download file_index \n ls \n quit')
    while True:
        print (cmd_prompt, end = '')
        cmd_str = input()
        action, content = process_cmd(cmd_str)
    #   print ('action: {}, content: {}'.format(action, content))
        if action == 'quit':
            break
