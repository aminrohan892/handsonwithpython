"""
RECURSIVE PROGRAMMING ALGORITHM

######## Pseudo Code ########

HanoiTowers(n,fromPeg,toPeg):
   if n = 1:
        output "Move disk from peg fromPeg to peg toPeg"
        return
    unusedPeg <-- 6 - fromPeg - toPeg
    HanoiTowers(n-1,fromPeg,unusedPeg)
    output "Move disk from peg fromPeg to peg toPeg"
    HanoiTowers(n-1,unusedPeg,toPeg)
"""

fromPeg = 1
toPeg = 3
unusedPeg = 2

def movePeg(fromPeg,toPeg):
    print(f'moving disk from peg {fromPeg} to peg {toPeg}')

def HanoiTowers(n,fromPeg,toPeg,unusedPeg):
    if n > 1:
        HanoiTowers(n-1,fromPeg,unusedPeg,toPeg)
        movePeg(fromPeg,toPeg)
        HanoiTowers(n-1,unusedPeg,toPeg,fromPeg)
    else:
        movePeg(fromPeg,toPeg)

print(HanoiTowers(4,fromPeg,toPeg,unusedPeg))







