from gevent.pywsgi import WSGIServer
from myapp_server import app
import os,sys

mypid = os.getpid()
mypid_file = "run/%s.pid" % (os.path.basename(sys.argv[0]).rstrip(".py"))
f = open(mypid_file,"w+")
f.write("%s" % (mypid))
f.close()
http_server = WSGIServer(('0.0.0.0', 5020), app)
http_server.serve_forever()
