# Problem 22: Are there positive integers a,b,c such that a ** 3 + b ** 3 = c ** 3?
#             Are there positive integers a,c,b,d such that a ** 4 + b * 4 + c ** 4 = d ** 4


for a in range(1000):
    for b in range(1000):
        for c in range(1000):
            if a ** 3 + b ** 3 == c ** 3:
                print(a,b,c)
