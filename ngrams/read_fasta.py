#!/usr/bin/python

def read_fasta(filename):

    f= open(filename, "r")
    finalstring = ""
    #save header
    lines = f.readlines()
    for line in lines:
        if line.startswith('>') : continue
        elif line.endswith('*') :
            line = line[:-1]
            finalstring = finalstring + line
        else: finalstring = finalstring + line
    finalstring = finalstring.replace('\n','')
    finalstring = finalstring.replace('*','')
    return finalstring
