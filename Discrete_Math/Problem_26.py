# Problem 26: Find a number of the form 111....1111 that is divisible by 57.


def find_number():
    i = 1
    while i % 57 > 0:
        a = str(i)
        a = str(a) + str(1)
        i = int(a)
    print(i)

find_number()
