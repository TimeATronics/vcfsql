# MIT License
# Copyright (c) 2022 Aradhya Chakrabarti
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import textwrap
import argparse
import sys
import pydoc
import mysql.connector as sql
from tabulate import tabulate

punctuation = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

parser = argparse.ArgumentParser(
    prog='vcfsql',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=textwrap.dedent('''\
    \tvCard to SQL Database Parser:
            Use this program to convert a
            Virtual Contact File (vcf/vCard) to
            an SQL database and pretty print it.
            '''))
parser.add_argument('-file', type=str, help='input', required=True)
parser.add_argument("-save", help="output (no pretty printing)", action="store_true")
parser.add_argument("-cond", type=str, help="condition", required=False)

try:
    args = parser.parse_args()
except Exception:
    parser.print_help()
    sys.exit()

filename = str(args.file)
save_arg = args.save
cond_arg = args.cond

# Constants:
DB_NAME = "CONTACTS"  # Name of Table in SQL DB
if any(i in filename for i in punctuation): # Check if there are any punctuations in filename
    TABLE = filename.translate(str.maketrans('', '', punctuation)) + "sql" # Name of SQL DB cannot contain special symbols
else:
    TABLE = filename + "sql"

file = open(filename, "r")

# Converts readlines input to list of lists by splitting via "BEGIN:VCARD and removing newlines
# First empty list in the whole list of lists and first and last empty strings in each list
# are also removed altogether. The "END:VCARD" item for the last list removed in the previous
# step is again added later. "BEGIN:VCARD" is additionally added to each element in the input lists.
# 
# list_comp is a list of lists of readlines() input of individual contacts delimited by BEGIN
# and END clauses in the input file.

list_comp = [l.split(',')[1:-1] for l in ','.join([i.strip() for i in file.readlines() if i != ""]).split('BEGIN:VCARD')][1:]
list_comp[-1].append("END:VCARD")
for i in list_comp:
    i.insert(0, "BEGIN:VCARD")

# final_list is a list of dictionaries of correct contact details
final_list = []

# ind_list is an intermediate list containing readlines() input for individual contacts
for ind_list in list_comp:
    ind_dict = {}  # ind_dict contains individual contacts converted to key:value pairs
    for line in ind_list:
        if "BEGIN:VCARD" not in line:
            if line == "END:VCARD":
                break  # Condition to end input into current ind_dict
            else:
                # No method has been implemented to parse compound tags (eg: EMAIL;TYPE=INTERNET,FAX)
                # Thus, all tags are implicitly removed
                # This will cause the script to behave in an undefined way if the input contains
                # multiple tags of the same type, since multiple values for the same key would be present.

                key_init = line.split(":")[0]
                if "=" in key_init:  # Condition for single tag types (eg: TYPE=)
                    key = key_init.split(";")[0]
                elif ";" in key_init:  # Condition for single tag types (eg: TEL;FAX)
                    key = key_init.replace(";", "")
                else:
                    key = key_init
                value = line.split(":")[1]
                ind_dict[key] = value
        else:
            continue
    final_list.append(ind_dict)

# headers is a list of the Column titles for the SQL DB.
# set() has been used to remove duplicates after merging all keys from all
# the dictionaries in final_list
headers = sorted(list(set(sum([list(i.keys()) for i in final_list], []))))

# Initialisation of SQL DB:
db = sql.connect(host="localhost", user="root", port=7265)
cursor = db.cursor(buffered=True)

check_db = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + DB_NAME + "';"
cursor.execute(check_db)
if cursor.rowcount > 0:
	cursor.execute("DROP DATABASE %s" % DB_NAME)
	cursor.execute("CREATE DATABASE %s" % DB_NAME)
	db.commit()
else:
	cursor.execute("CREATE DATABASE %s" % DB_NAME)
	db.commit()
	
#cursor.execute("CREATE DATABASE %s" % DB_NAME)
#db.commit()
cursor.execute("USE %s" % DB_NAME)

check_table = "SELECT * FROM information_schema.tables WHERE table_schema = '" + DB_NAME + "' AND table_name = '" + TABLE + "' LIMIT 1;"
if cursor.rowcount > 0:
	cursor.execute("DROP TABLE %s" % TABLE)
	db.commit()
else:
	pass
#db.commit()

# Command to create CONTACTS table using the headers defined earlier:
create_table_cmd = "CREATE TABLE " + TABLE + " (" + ", ".join([i + " TEXT" for i in headers]) + ")"
cursor.execute(create_table_cmd)
db.commit()

# Inserting individual dictionaries as rows in the SQL DB:
# Keys and values are iteratively merged into separate strings.
# Thus the dictionaries are finally converted into SQL "INSERT" statements

for dictionary in final_list:
    insert_col_str = ""
    insert_val_str = ""
    item_list = sorted(list(dictionary.items()))
    for i in item_list:
        insert_col_str += i[0]
        insert_col_str += ", "
        insert_val_str += "\""
        insert_val_str += i[1]
        insert_val_str += "\""
        insert_val_str += ", "
    insert_col_str = insert_col_str[:-2] # Removing trailing comma and whitespace
    insert_val_str = insert_val_str[:-2]
    insert_col_str = "INSERT INTO " + TABLE + "(" + insert_col_str + ") VALUES("
    insert_val_str = insert_val_str + ")"
    insert_str = insert_col_str + insert_val_str
    # Inserting individual rows into SQL DB
    cursor.execute(insert_str)
    db.commit()

file.close()

def prettyprint():
    """ Pretty Printing output to a table """
    cursor.execute("SELECT * FROM %s" % TABLE)
    result_table = cursor.fetchall()
    return tabulate(result_table, headers=headers, tablefmt='psql')

def select_range(input_statement):
	cursor.execute("SELECT * FROM " + TABLE + " where %s" % input_statement)
	result_table = cursor.fetchall()
	return tabulate(result_table, headers=headers, tablefmt='psql')

def main():
	if save_arg is False and cond_arg is not None:
		print(select_range(cond_arg))
	elif save_arg is True and cond_arg is None:
		with open("out.txt", "w") as save_file:    # Save to file
			save_file.write(prettyprint())
	elif save_arg is True and cond_arg is not None:
		with open("out.txt", "w") as save_file:    # Save to file
			save_file.write(select_range(cond_arg))
	else:
		try:
			print(prettyprint())
		except Exception:
			print(prettyprint())

if __name__ == "__main__":
	main()
	cursor.close()
	db.close()
