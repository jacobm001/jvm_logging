#!/usr/bin/python3.5

import pymssql, re, sys, time

def parse(raw_data):
	results = re.findall("([A-Za-z]+) = ([0-9\.]+);", raw_data)

	if len(results) == 0:
		sys.stderr.write("No matches found in input")
		sys.exit(1)
	else:
		return results

def clean_str(val):
	bad = ["--", "drop", ",", "insert"]

	val = val.lower()
	for b in bad:
		val = val.replace(b, "")

	return val

def build_query(parsed_data):
	if len(sys.argv) < 2:
		print("Need a hostname parameter")
		sys.exit(4)

	hostname = sys.argv[1]

	query = "insert into [jaspersoft].[jvm_logging] (servername, timestamp, category, value) values "

	for cat, val in parsed_data:
		cat = clean_str(cat)
		val = clean_str(val)

		query += "('{0}', CURRENT_TIMESTAMP, '{1}', {2}), ".format(hostname, cat, val)

	query  = query.rstrip(", ")
	query += ";"

	return query

def record_data(query):
	server   = 'bfpdb.engr.oregonstate.edu'
	username = ""
	password = ""

	try:
		con = pymssql.connect(server, username, password, 'Metrics')
		cur = con.cursor()
	except pymssql.Error as e:
		sys.stderr.write("Could not connect to database")
		sys.stderr.write(str(e))
		sys.exit(2)

	try:
		cur.execute(query)
	except pymssql.ProgrammingError as e:
		sys.stderr.write("Could not write to database")
		sys.stderr.write(str(e))
		sys.exit(3)

	con.commit()
	con.close()

if __name__ == "__main__":
	raw_data = ""
	for line in sys.stdin:
		raw_data += line

	parsed_data = parse(raw_data)
	query       = build_query(parsed_data)
	record_data(query)

	sys.exit(0)