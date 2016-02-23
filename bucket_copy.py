import threading
import datetime
import time
import sys
from threading import Thread
from multiprocessing.synchronize import BoundedSemaphore
from boto.s3.connection import S3Connection

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

def copy_s3_bucket(SOURCE_BUCKET, DEST_BUCKET, prefix=None, threads=10, preserve_acl=False):
	"""
	Example usage: copy_s3_bucket(SOURCE_BUCKET='my-source-bucket', DEST_BUCKET='my-destination-bucket', prefix='parent/child/dir/', threads=20)
	"""
	# Init s3
	conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	bucket = conn.get_bucket(SOURCE_BUCKET)
	dest_bucket = conn.get_bucket(DEST_BUCKET)

	# Filter by prefix
	rs = bucket.list()
	if prefix: rs = bucket.list(prefix)

	class CopyKey(Thread):
		def __init__ (self, key_name):
			Thread.__init__(self)
			self.key_name = key_name
			self.status = False
		def run(self):
			# We must create new bucket instances for each thread, passing the key is not threadsafe
			thread_conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
			thread_bucket = conn.get_bucket(SOURCE_BUCKET)
			thread_dest_bucket = conn.get_bucket(DEST_BUCKET)
			thread_key = thread_bucket.get_key(self.key_name)

			# Only copy if not exists on dest bucket
			if not thread_dest_bucket.get_key(self.key_name):
				pool_sema.acquire()
				self.status = "%s : Sempahore Acquired, Copy Next" % datetime.datetime.now()
				try:
					thread_key.copy(DEST_BUCKET, self.key_name, preserve_acl=preserve_acl)
					self.status = "%s : Copy Success : %s" % (datetime.datetime.now(), self.key_name)
				except:
					self.status = "%s : Copy Error : %s" % (datetime.datetime.now(), sys.exc_info())
				finally:
					pool_sema.release()
			else:
				self.status = "%s : Key Already Exists, will not overwrite." % datetime.datetime.now()

	key_copy_thread_list = []
	pool_sema = BoundedSemaphore(value=threads)
	total_keys = 0

	# Request threads
	for key in rs:
		total_keys += 1
		print "%s : Requesting copy thread for key %s" % (datetime.datetime.now(), key.name)
		current = CopyKey(key.name)
		key_copy_thread_list.append(current)
		current.start()

		# Pause if max threads reached - note that enumerate returns all threads, including this parent thread
		if len(threading.enumerate()) >= threads:
			print "%s : Max Threads (%s) Reached: Pausing until threadcount reduces." % (datetime.datetime.now(), threads)
			while 1:
				if len(threading.enumerate()) < threads:
					print "%s : Continuing thread creation." % datetime.datetime.now()
					break
				time.sleep(1)

	for key_copy_thread in key_copy_thread_list:
		key_copy_thread.join(30) # Bring this particular thread to this current "parent" thread, blocks parent until joined or 30s timeout
		if key_copy_thread.isAlive():
			print "%s : TIMEOUT on key %s" % (datetime.datetime.now(), key_copy_thread.key_name)
			continue
		print "%s : Status Output: %s" % (datetime.datetime.now(), key_copy_thread.status)

	print "%s : Complete : %s Total Keys Requested" % (datetime.datetime.now(), total_keys)
