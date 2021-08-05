# Problem 15: Is there a positive integer that is divisible by 13 and ends with 15?


for n in range(10 ** 3):
    if n % 13 == 0 and n % 100 == 15:
        print(str(n))
    else:
        print(str(n) + ' Is not valid')
