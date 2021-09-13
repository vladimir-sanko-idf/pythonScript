# this python script generate 3 files :
#                                   1. script that create table and view(create_table_script.txt)
#                                   2. script that generate insert scipt(insert_script.txt)
#                                   3. script that that you run in console(console_script.txt)
#
#
#                                         what in input file(input.txt):
#                                                                     1 row table name
#                                                                     2 row schema name
#                                                                     3 row interval (like 2 DAY)
#                                                                     4 row ordered by, write like assumeNotNull(id)
#                                                                     5 row schema with dictionary
#
#
#                                                                         !!!warning moments:
#                                                                            1. in insert file from cause creds
#                                                                            2. no insert time in console script
#                                                                            3. in console script insert part form cause
#
#
#

import re
from datetime import datetime

def read_text_file():
    f = open("input.txt", "r")
    temp = [line[:-1] for line in f]
    table_name = temp[0]
    db_name = temp[1]
    interval = temp[2]
    ordered = temp[3]
    dict = temp[4]
    tmp = temp[7]
    pos = 7
    fields = []
    while tmp != ")":
        fields.append(tmp)
        pos = pos + 1
        tmp = temp[pos]
    return [db_name, table_name, interval, ordered, dict, fields]

def write_in_file_cr_t(table, db, interval, fields, ordered, dict):
    f = open('create_table_script.txt', 'w')
    f.write("CREATE TABLE " + db + "." + table + "_archive\n")
    f.write("(" + "\n")
    [f.write(field + "\n") for field in fields]
    f.write(",`triggeredTime` DateTime" + "\n")
    f.write(")" + "\n")
    f.write("ENGINE = MergeTree ORDER BY " + ordered + "\n")
    f.write("TTL triggeredTime + INTERVAL " + interval + "\n")
    f.write("partition by triggeredTime" + "\n")
    f.write("SETTINGS index_granularity = 8192;" + "\n")
    f.write("\n")
    f.write("drop view if exists " + db + "." + table + ";\n")
    f.write("create view " + db + "." + table + " as" + "\n")
    f.write("select t1.*"+"\n")
    f.write("from " + db + "." + table +"_archive t1\n")
    f.write("where t1.triggeredTime = dictGet('" + dict + ".last_mysql_scheduled_dump_dictionary', 'last_succeed_time'," + "\n")
    f.write(" tuple('"+table +".sql'));")
    f.close()

def write_inserting_file(parsed_fields, db, table):
    f = open('insert_script.txt', 'w')
    f.write("insert into " + db + "." + table + "_archive\n")
    f.write("select ")
    [f.write(field + ",\n") for field in parsed_fields]
    f.write("{triggeredTime}" +"\n")
    f.write("from mysql('slkz_mysql_host:slkz_mysql_port', 'solva_kz', '" + table_name +"', 'slkz_mysql_login',\n")
    f.write("'slkz_mysql_password');")
    f.close()

def write_console_script(db, table, parsed_fields, fields, ordered, dict, interval):
    f = open('console_script.txt', 'w')
    now = datetime.now()

    f.write("CREATE TABLE " + db + "." + table + "_archive\n")
    f.write("(" + "\n")
    [f.write(field + "\n") for field in fields]
    f.write(",`triggeredTime` DateTime" + "\n")
    f.write(")" + "\n")
    f.write("ENGINE = MergeTree ORDER BY " + ordered + "\n")
    f.write("TTL triggeredTime + INTERVAL " + interval + "\n")
    f.write("partition by triggeredTime" + "\n")
    f.write("SETTINGS index_granularity = 8192;" + "\n")
    f.write("\n")
    f.write("insert into " + db + "." + table + "_archive\n")
    f.write("select ")
    [f.write(field + ",\n") for field in parsed_fields]
    f.write("{triggeredTime}" + "\n")
    f.write("from mysql('internal.risk.slave.solva.kz:3306', 'solva_kz', '" + table_name + "', 'warehouse',\n")
    f.write("'RrLnchhMTJd6m7OTPkUX');")
    f.write("\n")
    f.write("insert into " + dict + ".last_mysql_scheduled_dump_table\n")
    f.write("values (' " + table + ".sql', '{triggeredTime}', 0);\n")
    f.write("system reload dictionary " + db + ".last_mysql_scheduled_dump_dictionary;\n")
    f.write("\n")
    f.write("drop view if exists " + db + "." + table + ";\n")
    f.write("create view " + db + "." + table + " as" + "\n")
    f.write("select t1.*" + "\n")
    f.write("from " + db + "." + table + "_archive t1\n")
    f.write(
        "where t1.triggeredTime = dictGet('" + dict + ".last_mysql_scheduled_dump_dictionary', 'last_succeed_time'," + "\n")
    f.write(" tuple('" + table + ".sql'));")
    f.close()


if __name__ == '__main__':
    now = datetime.now()
    time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(time)
    ans = []
    ans = read_text_file()
    db_name = ans[0]
    table_name = ans[1]
    interval = ans[2]
    ordered = ans[3]
    dict = ans[4]
    fields = ans[5]
    parsed_fields = [re.search("`.*`", field).group(0)[1:-1] for field in fields]
    write_in_file_cr_t(table_name, db_name, interval, fields, ordered, dict)
    write_inserting_file(parsed_fields, db_name, table_name)
    write_console_script(db_name, table_name, parsed_fields, fields, ordered, dict, interval)
    print(table_name)
    print(db_name)



