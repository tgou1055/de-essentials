
print("Hello World!")

# Troubleshooting Compilation Errors
# Troubleshooting Runtime Errors - use try except


# One of the ways to handle runtime errors gracefully is by having a 
# proper exception handling.


try:
    n = int(input('Enter integer to check if it is even: '))
except ValueError:
    print('Run program again with valid integer')
    exit()

if n % 2 == 0:
    print(f'{n} is even')
else:
    print(f'{n} is odd')
