import os

# for any csv or txt file found in the ./inputs directory, we load the data into memory for use by this program
# e.g. cities.csv becomes df_cities()

for filename in os.listdir("inputs"):
	#first, if we want current dates, regen this file prior to loading into memory
	if filename.endswith(".csv") or filename.endswith(".txt"): 
		arrayname = os.path.join(filename[:-4])
		globals()["df_"+arrayname] = 0
		with open("inputs/" + filename, "r") as file:
			globals()["df_"+arrayname] = file.read().split('\n')
			file.close()
	else:
		continue