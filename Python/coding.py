# greatest common divisor


def find_gcd(a,b):
    gcd = 0
    if a>b:
        for i in range(b,0,-1):
            if a%i==0 and b%i==0:
                gcd = i
                break
    elif b>a:
        for i in range(a,0,-1):
            if a%i==0 and b%i==0:
                gcd = i
                break
    print(gcd)


if __name__ == "__main__":
    find_gcd(19,20)