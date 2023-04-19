# coding=UTF-8
'''
Author: xiaoyichao
LastEditors: xiaoyichao
Date: 2020-08-20 11:09:45
LastEditTime: 2021-12-15 15:32:26
Description: kill 进程
'''
import os
import signal
import subprocess
import time


class KillProgram:
    '''
    Author: xiaoyichao
    param {type}
    Description: kill进程
    '''

    def kill(self, pid):
        '''
        Author: xiaoyichao
        param {*}
        Description: 根据pid 杀掉进程
        '''
        # print(pid)
        try:
            a = os.kill(pid, signal.SIGKILL)
            print('已杀死pid为%s的进程' % (pid))

        except OSError:
            print('没有如此进程!!!')

    def port_examine(self, port):
        port_status = int(subprocess.getoutput(
            '''netstat -tulpn | egrep ":%s" | wc -l''' % port))
        try_count = 0
        while port_status != 0:
            time.sleep(1)
            print("wait")
            port_status = int(subprocess.getoutput(
                '''netstat -tulpn | egrep ":%s" | wc -l''' % port))
            try_count += 1
            if try_count >= 200:
                raise Exception("server cannot restart properly")

    def killport(self, port):
        command4try = '''/usr/sbin/lsof  -i:%s -t''' % port
        if_use = os.system(command4try)
        try_count = 1
        if if_use != 0:
            print("端口未被占用")
            command = '''kill -9 $(/usr/sbin/lsof  -i:%s -t)''' % port
            os.system(command)
        else:
            while if_use == 0:
                time.sleep(1)
                command = '''kill -9 $(/usr/sbin/lsof  -i:%s -t)''' % port
                os.system(command)
                print("try_count:", try_count, " kill command:", command)
                command4try = '''/usr/sbin/lsof  -i:%s -t''' % port
                if_use = os.system(command4try)
                try_count += 1
                if try_count >= 200:
                    raise Exception("server cannot restart properly")

    def kill_program(self, program_name=None, port=None):
        '''
        Author: xiaoyichao
        param {type} program_name需要包含.py
        Description: 用于杀掉没有端口释放需求的程序
        '''
        # if program_name is not None:

        if port is not None:
            self.killport(port)
            # self.port_examine(port)
        elif program_name is not None:
            get_pid = "ps aux | grep %s " % program_name
            out = os.popen(get_pid).read()
            current_pid = os.getpid()
            print("current_pid", current_pid)

            for line in out.splitlines():
                if program_name in line:
                    pid = int(line.split()[1])
                    if int(pid) != int(current_pid):  # 不能杀掉当前进程
                        self.kill(pid)
            print("kill: ", program_name)
        else:
            print("program_name and port are none")

if __name__ == "__main__":
    killprogram = KillProgram()
    killprogram.kill_program("search_opt_server.py", 8139)
