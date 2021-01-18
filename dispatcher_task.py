# -*- coding=utf-8 -*-
import os,sys
import MySQLdb
import traceback
from pooldb import *
from logger import *
from dispatcher import *
import time

reload(sys)
sys.setdefaultencoding('utf-8')

mypid = os.getpid()
mypid_file = "run/%s.pid" % (os.path.basename(sys.argv[0]).rstrip(".py"))
f = open(mypid_file,"w+")
f.write("%s" % (mypid))
f.close()

if __name__ == '__main__':
        #print g_config.configinfo
        dispatcher_music = dispatcher(g_config,logging,"http://centerproxy.kuwo.cn/centerserver/add_task")
        dispatcher_music.create_workers()
        dispatcher_music.dispatch(300,3000,g_pool_Src,g_pool_Run,g_pool_Res)
	time.sleep(10)
