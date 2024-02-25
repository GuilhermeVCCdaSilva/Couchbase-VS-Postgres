from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, QueryOptions)
import json
import csv
import time

header = ['Iteration', 'Couchbase_Insert', 'Couchbase_Select']
iteration_data = []
insert_data = []
select_data = []

username = "Admin"
password = "password"
bucket_name = "storage"

auth = PasswordAuthenticator(
    username,
    password,
)

cluster = Cluster('couchbase://localhost', ClusterOptions(auth))
cb = cluster.bucket(bucket_name)
cb_coll = cb.scope("_default").collection("default")

cluster.query("CREATE PRIMARY INDEX ON `{}`.`{}`.{}".format("storage","_default","default")).execute()

def select(count):
    try:
        result = cluster.query("SELECT * FROM `{}`.`{}`.{} WHERE {} = {}".format("storage","_default","default","Line",count)).execute()
    except Exception as e:
        print(e)

doc = json.load(open('BigSession.json'))

iteration = 1
count_insert = 1
count_select = 1

for i in range(10):
    start_time = time.time()
    for i in range(100) : 
        try:
            doc["Line"] = count_insert
            key =  f'{doc["Tab"]}_{count_insert}'
            cb_coll.insert(key, doc)
        except Exception as e:
            print(e)
        count_insert += 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    insert_data.append(elapsed_time)

    start_time = time.time()
    for i in range(100) :
        select(count_select)
        count_select += 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    select_data.append(elapsed_time)

    iteration_data.append(iteration)
    iteration += 1

# Zipping the three variables together
zipped_data = zip(iteration_data, insert_data, select_data)

# Write data to CSV file
with open('bentchmarkCouchBase.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    for row in zipped_data:
        writer.writerow(row)

