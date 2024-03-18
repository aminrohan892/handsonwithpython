"""
# SelectionSort: Find the i-th smallest element in a, and moves it to the i-th position
# Prerequsite: List must contain unique values
"""
from random import random
from random import randint


def CreateList():
    inputlist = []
    for i in range(1,20):
        inputlist.append(randint(0,1000))
    return inputlist

def SelectionSort(inputlist):
    print(f'Input List = {inputlist}')
    sortedlist = []
    for i in range(len(inputlist)):
        x = inputlist[i]
        #print(f' outer loop {i} and value of x = {x}')
        z = x
        for m in range(len(inputlist[i:])):
            inputlist2 = inputlist[i:]
            y = inputlist2[m]
            #print(f' inner loop {m} and value of y = {y}')
            if y <= z:
                z = y
            else:
                z
            #print(f' inner loop {m} and value of z = {z}')
        #print(f' outer loop {i} and value of z = {z}')
        #print(f'index of {z} = {inputlist.index(z)}')
        inputlist.pop(inputlist.index(z))
        inputlist.insert(i,z)
        print(inputlist)

inputlist = CreateList()

SelectionSort(inputlist)

