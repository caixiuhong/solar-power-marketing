import random
import numpy as np
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer


class Producer(object):

	def __init__(self, addr):
		self.producer = KafkaProducer(bootstrap_server=addr)

	def produce_msgs(self, source_symbol):
		msg_cnt = 0
		while True:
			time_field= datatime.now().strftime("%Y%m%d %H%M%S")
			number_field = np.random.normal(10,1)
			str_fmt="{};{};{}"
			message_info = str_fmt.format(source_symbol, time_field, number_field)

			print message_info
			self.producer.send('Gaussian', message_info)
			msg_cnt += 1

if __name__ == "__main__":
	args = sys.argv
	ip_addr=str(args[1])
	partition_key = str (args[2])
	prod = Producer(ip_addr)
	prod.produce_msgs(partition_key)
	