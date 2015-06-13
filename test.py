from random import randint
import random

for i in range(0, 5000):
	print "INSERT INTO tbl VALUES (%d, %d, %.3f);" % (i,randint(2,100000),random.uniform(1000, 2000))
