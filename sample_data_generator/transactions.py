import random
import datetime 

import core as c

column_data = [
'customer_id',
'orderid',
'purchasedatetime',
'transactiontotal',
'numberofitems',
'productcode',
'productcategory',
'cc_number']

def generate_transactions(customerid: str, maxtrans: int):

	rowcount = 0
	transactions = ""

	for newrow in range(maxtrans):   # for each customer we gen 1-N transactions, change as desired
		newrow = ""
		transtotal = round(random.triangular(100, 2000),2) # triangular is awesome and creates a nice normal distro of values
		numitems = random.randint(1,15)

		newrow += c.quote + customerid + c.quotecomma
		newrow += c.quote + c.nextId() + c.quotecomma
		random_datetime = c.trans_start_date + datetime.timedelta(seconds=random.randint(1,c.max_seconds))	
		newrow += c.quote + str(random_datetime.replace(microsecond=0)) + c.quotecomma
		newrow += c.quote + str(transtotal) + c.quotecomma
		newrow += c.quote + str(numitems) + c.quotecomma		

		prodcode = random.randint(700000000,900000000)

		#product codes
		newrow += c.quote + str(prodcode) + c.quotecomma		
		newrow += c.quote + ("PR" + str(prodcode)[:3]) + c.quotecomma

		# CC number
		newrow += c.quote + str(random.randint(1000,9999)) + " "  \
						+ str(random.randint(1000,9999)) + " "  \
						+ str(random.randint(1000,9999)) + " "  \
						+ str(random.randint(1000,9999)) + c.quote

		if rowcount != maxtrans:			
			newrow += c.newline

		transactions += newrow
		rowcount+=1

	return(transactions)

def generate_transactions_json(customerid: str, maxtrans: int, peoplerowcount: int, peoplerows: int):

	rowcount = 0
	transactions = ""

	for newrow in range(maxtrans):   # for each customer we gen 1-N transactions, change as desired
		newrow = ""
		transtotal = round(random.triangular(100, 2000),2) # triangular is awesome and creates a nice normal distro of values
		numitems = random.randint(1,15)
		random_datetime = c.trans_start_date + datetime.timedelta(seconds=random.randint(1,c.max_seconds))	
		prodcode = random.randint(700000000,900000000)
		cc_number = str(random.randint(1000,9999)) + " "  \
					+ str(random.randint(1000,9999)) + " "  \
					+ str(random.randint(1000,9999)) + " "  \
					+ str(random.randint(1000,9999))

		newrow += c.openCurly + c.newline
		newrow += c.quote + "customer_id" + c.quote + c.colon + c.quote + customerid + c.quote + c.comma + c.newline
		newrow += c.quote + "orderid" + c.quote + c.colon + c.quote + c.nextId() + c.quote + c.comma + c.newline
		newrow += c.quote + "purchasedatetime" + c.quote + c.colon + c.quote + str(random_datetime.replace(microsecond=0)) + c.quote + c.comma + c.newline

		newrow += c.quote + "purchase_info" + c.quote + c.colon + c.openCurly + c.newline

		newrow += c.tab + c.quote + "transactiontotal" + c.quote + c.colon + str(transtotal) + c.comma + c.newline
		newrow += c.tab + c.quote + "numberofitems" + c.quote + c.colon + str(numitems) + c.comma + c.newline
		newrow += c.tab + c.quote + "productcode" + c.quote + c.colon + c.quote + str(prodcode) + c.quote + c.comma + c.newline
		newrow += c.tab + c.quote + "productcategory" + c.quote + c.colon + c.quote + ("PR" + str(prodcode)[:3]) + c.quote + c.newline
		newrow += c.tab + c.closeCurly + c.comma + c.newline

		newrow += c.quote + "cc_number" + c.quote + c.colon + c.quote + cc_number + c.quote + c.newline

		if peoplerowcount == peoplerows-1 and rowcount == maxtrans-1:
			newrow += c.closeCurly + c.newline
		else:
			newrow += c.closeCurly + c.comma + c.newline


		transactions += newrow
		rowcount+=1

	return(transactions)

