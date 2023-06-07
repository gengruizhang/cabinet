import sys

# Get the value of x from the command line argument
x = int(sys.argv[1])

# Initialize the sum to 0
sum = 0

# Loop from 1 to x and add each number to the sum
for i in range(1, x+1):
    sum += i

# Print the sum
print("The sum of the numbers from 1 to", x, "is", sum)