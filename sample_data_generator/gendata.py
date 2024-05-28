import time
import people

# let's use a low tech timer to see how long the data gen takes
t_start = time.time()

print("generating records, please hold..")

# args: (user csv headers?, how many people?, create transactions?, how many transactions?)
args = (True, 1000, True, 10)

newdata = people.create_data(*args)

# people.create_data returns 5 giant elements in one big array :)
# 1,000,000 people records takes about 10 minutes

with open('people.csv', 'w') as f:
	f.write(newdata[0])
	print("finished writing people data")

with open('people.json', 'w') as f:
	f.write(newdata[1])
	print("finished writing people JSON data")

with open('transactions.csv', 'w') as f:
	f.write(newdata[2])
	print("finished writing transaction data")

with open('transactions.json', 'w') as f:
	f.write(newdata[3])
	print("finished writing transaction JSON data")

with open('social.csv', 'w') as f:
	f.write(newdata[4])
	print("finished writing social interaction data")

t_end = time.time()
totaltime = t_end-t_start
print(str(totaltime) + " seconds")