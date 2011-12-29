#!/usr/bin/env python
import paramiko

# REQUIRES the following args:
# sshuser = "user"
# sshpass = "password"
# sshhost = "hadoop.server.com"
# logssh = "F"
# xferlog = "/dev/null"
# skiptrash = "T"

# example:
# tmpdir = "/data/tmp"
# logdate = strftime("%Y%m%d", localtime())
# xferlog = "%s/%s-%s.log" % (tmpdir,name,logdate)
# h = paradoop(xferlog)
# localfile = '%s/%s-%s-%s.csv' % (tmpdir,system,name,logdate)
# remotefile = '%s/%s-%s-%s.csv' % (tmpdir,system,name,logdate)
# h.transfer(localfile,remotefile)

class paradoop(object):
        def __init__(self,xferlog):
                global sshuser
                global sshpass
                global sshhost
                global skiptrash
                if skiptrash == 'T':
                        trashcom = "-skipTrash "
                else:
                        trashcom = ""
                global logssh
                if logssh == 'T':
                        paramiko.util.log_to_file(xferlog)
        def transfer(self,localfile,remotefile):
                # transfer the file to a temp location on sshhost
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(sshhost, username = sshuser, password = sshpass)
                sftp = ssh.open_sftp()
                try:
                        sftp.put(localfile, remotefile)
                except:
                        print "ERROR: Unable to upload %s to %s!" % (localfile,sshhost)
                sftp.close()
                ssh.close()
        def mkdir(self,hdfspath):
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(sshhost, username = sshuser, password = sshpass)
                # test if target hdfs location exists
                sc = "hadoop dfs -ls %s" % (hdfspath)
                try:
                        stdin, stdout, stderr = ssh.exec_command(sc)
                except:
                        print "ERROR: failed to execute %s against %s" % (sc, sshhost)
                # if it doesn't exist, make it
                if len(stderr.readlines()) > 0:
                      try:
                         sc = "hadoop dfs -mkdir %s" % (hdfspath)
                         stdin, stdout, stderr = ssh.exec_command(sc)
                      except:
                         print "ERROR: unable to create %s!" % (hdfspath)
                ssh.close()
        def rmdir(self,hdfspath):
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(sshhost, username = sshuser, password = sshpass)
                # test if target hdfs location exists
                sc = "hadoop dfs -rmr %s %s" % (self.trashcom,hdfspath)
                try:
                        stdin, stdout, stderr = ssh.exec_command(sc)
                except:
                        print "ERROR: failed to execute %s against %s" % (sc, sshhost)
                ssh.close()
        def cp(self,localfile,hdfspath):
                # once we have a directory we can move the file there
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(sshhost, username = sshuser, password = sshpass)
                sc = "hadoop dfs -copyFromLocal %s %s" % (localfile, hdfspath)
                try:
                        stdin, stdout, stderr = ssh.exec_command(sc)
                except:
                        print "ERROR: unable to move %s to %s!" % (localfile, hdfspath)
                ssh.close()
        def rm(self,hdfsfile):
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(sshhost, username = sshuser, password = sshpass)
                # test if target hdfs location exists
                sc = "hadoop dfs -rm %s %s" % (self.trashcom,hdfsfile)
                try:
                        stdin, stdout, stderr = ssh.exec_command(sc)
                except:
                        print "ERROR: failed to execute %s against %s" % (sc, sshhost)
                ssh.close()
		def rmlocalfile(self,localfile):
				# delete temp files on sshhost
				ssh = paramiko.SSHClient()
				ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
				ssh.connect(sshhost, username = sshuser, password = sshpass)
				sc = "rm -f %s" % (localfile)
				try:
					stdin, stdout, stderr = ssh.exec_command(sc)
				except:
					print "ERROR: unable to execute %s on %s!" % (sc,sshhost)
				ssh.close()