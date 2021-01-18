ps ux | grep autodms_server.py | grep -v grep | awk '{print $2}' | xargs kill

nohup python autodms_server.py &
