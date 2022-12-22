'''
python3 load_logs.py /home/bigdata/nasa-logs-2 cda78 nasalogs

'''

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import os, re, gzip
import sys
import datetime
def main(input_dir, keyspace, table_name):
    users_to_insert = []
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                users_to_insert.append(line)
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)
    batch = BatchStatement(consistency_level=1)
    insert_user = session.prepare("INSERT INTO "+table_name+"(host, datetime, path, bytes, id) VALUES (?, ?, ?, ?, uuid())")
    count = 0
    for line in users_to_insert:
        line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
        regex_result = re.search(line_re,line)
        if regex_result != None:
            if count < 100:
                host, date_time, path, bytes = regex_result.groups()
                date_time = datetime.datetime.strptime(date_time, '%d/%b/%Y:%H:%M:%S')
                batch.add(insert_user, (host, date_time, path, int(bytes)))
                count += 1
            elif count == 100:
                count = 0
                session.execute(batch)
                batch.clear()
    if count != 0:
        session.execute(batch)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)