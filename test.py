def calculate(x):
    mul=[1,3,7,9,1,3,7,9,1,3,1]
    tot=0
    for i in range(11):
        tot+=(int(x[i])*mul[i])
    
    mod=tot%10
    
    if mod==0:
        return 'Y'
    else:
        return 'N'
    
    return tot

def main():
    count = int(input())
    for i in range(count):
        x=input()
        print(calculate(x))

if __name__ == '__main__':
    main()
    
#a=calculate('44051401458')
#b=calculate('12345678901')