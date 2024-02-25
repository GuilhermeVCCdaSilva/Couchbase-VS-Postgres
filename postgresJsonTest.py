import psycopg2
import json
import time
import csv

DATABASE_CONFIG = { 
    "database": "postgres", 
    "user": "postgres", 
    "password": "password", 
    "host": "localhost", 
    "port":  5432, 
} 



def get_connection(): 
    return psycopg2.connect( 
        database = DATABASE_CONFIG.get('database'), 
        user = DATABASE_CONFIG.get('user'), 
        password = DATABASE_CONFIG.get('password'), 
        host = DATABASE_CONFIG.get('host'), 
        port = DATABASE_CONFIG.get('port'), 
    )

doc = json.load(open('BigSession.json'))


header = ['Iteration', 'Postgres_Insert', 'Postgres_Select']
iteration_data = []
insert_data = []
select_data = []

iteration = 1
count_insert = 1
count_select = 1

connection = get_connection() 
current = connection.cursor()
for i in range(10):
    start_time = time.time()
    for i in range(100):
        try:
            doc["Line"] = count_insert
            id =  f'{doc["Tab"]}_{count_insert}'
            sql = "INSERT INTO json_example (id, json_col) VALUES (%s, %s)"
            current.execute(sql, (id, json.dumps(doc)))
            connection.commit()
        except Exception as e:
            print(e)
        count_insert += 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    insert_data.append(elapsed_time)


    start_time = time.time()
    for i in range(100) :
        sql = f"SELECT * FROM json_example where json_col->>'Line' = '{count_select}'"
        current.execute(sql)
        connection.commit()
        current.fetchall()
        count_select += 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    select_data.append(elapsed_time)

    iteration_data.append(iteration)
    iteration +=1

connection.close()

# Zipping the three variables together
zipped_data = zip(iteration_data, insert_data, select_data)

# Write data to CSV file
with open('bentchmarkPostgres.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(header)
    for row in zipped_data:
        writer.writerow(row)


