#!/usr/bin/env python
##################################
#Name: Shaival Dalal
#NetID: sd3462
#Course: Big Data Analytics
#Term: Fall 2017
#Assignment: 2
##################################

# map function for matrix multiply
#Input file assumed to have lines of the form "A,i,j,x", where i is the row index, j is the column index, and x is the value in row i, column j of A. Entries of A are followed by lines of the form "B,i,j,x" for the matrix B. 
#It is assumed that the matrix dimensions are such that the product A*B exists. 
#
#
#Input arguments:
#m should be set to the number of rows in A, p should be set to the number of columns in B.
 
import sys
import string

#number of rows in A
m = int(sys.argv[1]) 

#number of columns in B
p = int(sys.argv[2])


# input comes from STDIN (stream data that goes to the program)
for line in sys.stdin:
    
	#Remove leading and trailing whitespace
	line = line.strip()

	#Split line into array of entry data
	entry = line.split(",")
    
	# Set row, column, and value for this entry
	row = int(entry[1])
	col = int(entry[2])
	value = float(entry[3])

	#If this is an entry in matrix A...
	if (entry[0] == "A"):
		#Generate the necessary key-value pairs
 		for i in range(0,p):
 			print '%d %d\tA %d %f' % (row,i,col,value)
 			
	#Otherwise, if this is an entry in matrix B...
	else:
		#Generate the necessary key-value pairs
 		for i in range(0,m):
 			print '%d %d\tB %d %f' % (i,col,row,value)
