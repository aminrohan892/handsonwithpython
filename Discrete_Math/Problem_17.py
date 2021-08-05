# Problem 17: Find a two-digit (positive) integer that becomes 7 times smaller when its first (=leftmost) digit is removed


for i in range(10,100):
    if i == 7 * int(str(i)[1:]):
        print(i)



