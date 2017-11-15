#!/usr/bin/env python
##################################
#Name: Shaival Dalal
#NetID: sd3462
#Course: Big Data Analytics
#Term: Fall 2017
#Assignment: 2
##################################

#Reduce function for computing matrix multiply A*B

#Input arguments:
#variable n should be set to the inner dimension of the matrix product (i.e., the number of columns of A/rows of B)

import sys
import string

#number of columns of A/rows of B
n = int(sys.argv[1])

currentkey = None
product=0

#Creating two dictionaries to store values of A and B
A_values={}
B_values={}

# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
	key,value = line.strip().split('\t',1)
	value=value.split(" ")

	#If currentkey is equal to key, add values to the matrix
	if currentkey==key:
		if value[0]=="A":
			A_values[value[1]]=float(value[2])
		else:
			B_values[value[1]]=float(value[2])
    
    #If curretkey is not equal, then execute the below loop    
	else:
		#If currentkey is not none and is new, compute values for existing values in the matrix
		if currentkey!=None:
			for each in A_values:
				product+=A_values[each]*B_values[each]
			print "%s %f" %(currentkey,product)
			product=0
		#Execute one time when the first record is passed to the reducer
		if value[0]=="A":
			A_values[value[1]]=float(value[2])
		else:
			B_values[value[1]]=float(value[2])
		#Assign currentkey to key on completion of conditional statements
		currentkey=key

#For the last record, compute the product
if currentkey==key:
	for each in A_values:
		product+=A_values[each]*B_values[each]
	print "%s %f" % (key,product)
