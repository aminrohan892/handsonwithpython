"""
DYNAMIC PROGRAMMING ALGORITHMent_

######## Pseudo Code ########

Rocks(n,m):
    # we hard coded the 0,0 position to Loss
    R(0,0) <-- L

    # in this for loop we are only looking at 1st column in our n * m matrix
    for i from 1 to n:
        if R(i-1,0) == W:
            R(i,0) <-- L
        else:
            R(i,0) <-- W

    # in this for loop we are only looking at 1st row in our n * m matrix
    for j from 1 to m:
        if R(0, j-1) = W
            R(0,j) <-- L
        else:
            R(0,j) <-- W

    # in this for loop we are looking at all intermediate cells in our n * m matrix
    for i from 1 to n:
        for j from 1 to m:
            if R(i-1,j-1) == W and R(i-1,j) == W and R(i,j-1) == W:
                R(i,j) <-- L
            else:
                R(i,j) <-- W
    return R(n,m)
"""


#Define the actual function:
def CreateRocks(n,m):
    A = []
    for i in range(0,n):
        B = []
        for j in range(0,m):
            B.append('')
        A.append(B)
    return A

def Rocks(n,m):
    C = CreateRocks(n,m)
    C[0][0] = 'Loss'
    for i in range(1,n):
        if C[i - 1][0] == 'Win':
            C[i][0] = 'Loss'
        else:
            C[i][0] = 'Win'
    for j in range(1,m):
        if C[0][j-1] == 'Win':
            C[0][j] = 'Loss'
        else:
            C[0][j] = 'Win'
    # in this for loop we are looking at all intermediate cells in our n * m matrix
    for i in range(1,n):
        for j in range(1,m):
            if C[i-1][j-1] == 'Win' and C[i-1][j] == 'Win' and C[i][j-1] == 'Win':
                C[i][j] = 'Loss'
            else:
                C[i][j] = 'Win'
    return C

print(Rocks(3,3))
