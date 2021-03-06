from selenium import webdriver
import csv
import random
import time
import os

def get_column_headers_for_table(table_element):
    table_header_row = table_element.find_element_by_xpath('//thead/tr[//th]')
    table_header_row_cells = table_header_row.find_elements_by_xpath('th')
    return [cell.text for cell in table_header_row_cells]

def get_body_rows_for_table(table_element):
    return table_element.find_elements_by_xpath('//tbody/tr')

def get_title_for_row_element(row_element):
    if len(row_element.find_elements_by_xpath('td[@title]')) != 0:
        row_title_cell = row_element.find_element_by_xpath('td[@title]')
        return row_title_cell.get_attribute('title')
    else:
        return ""

def get_link_for_row_element(row_element):
    if len(row_element.find_elements_by_xpath('td//a')) != 0:
        row_link_element = row_element.find_element_by_xpath('td//a')
        return row_link_element.get_attribute('href')
    else:
        return ""

def create_dictionary_from_row_element(row_element, column_lables):
    row_cells = row_element.find_elements_by_xpath('td')
    row_cells_text = [cell.text for cell in row_cells]
    
    row = {column_lables[i]: row_cells_text[i] for i in range(len(column_lables))}
    row['COLUMN DESCRIPTION'] = get_title_for_row_element(row_element)
    row['HREF'] = get_link_for_row_element(row_element)
    
    return row

def create_dictionary_from_table_element(table_element):   
    table = []
    column_lables = get_column_headers_for_table(table_element)
    table_body_rows = get_body_rows_for_table(table_element)

    for row in table_body_rows:
        row_values = create_dictionary_from_row_element(row, column_lables)
        table.append(row_values)

    return table

def clean_table_by_seperating_name_from_description(table_dictionary):
    for row in table_dictionary:
        split = row['TABLE NAME'].split('\n')
        row['TABLE NAME'] = split[0].lower()
        row['TABLE DESCRIPTION'] = split[1]
    return table_dictionary


## basic values
database = 'cas'
cluster = 'nhs'
schema = 'cas_schema'
tags = ''
is_view = 'false'
description_source = ''
fake_users = [
    'Esther.Gillespie@example.com',
    'Aoife.Kramer@example.com',
    'Saffron.Lin@example.com',
    'Eva.Krueger@example.com',
    'Ella.Levy@example.com',
    'Gloria.Parker@example.com',
    'Eleanor.Figueroa@example.com',
    'Hana.Stone@example.com',
    'Katie.Maldonado@example.com',
    'Ellis.Moses@example.com',
    'Daniel.Strickland@example.com',
    'Wayne.Craig@example.com',
    'Bilal.Conley@example.com',
    'Aidan.Lyons@example.com',
    'William.Avila@example.com',
    'Jesse.Dale@example.com'
]

## Run extration to CSV
driver = webdriver.Firefox()
tables_url = 'https://www.cancerdata.nhs.uk/explorer/cas_tables'
driver.get(tables_url)
table_element = driver.find_element_by_xpath('//table')
table_of_tables_dictionary = clean_table_by_seperating_name_from_description(create_dictionary_from_table_element(table_element))

all_columns_dictionary = []
for table in table_of_tables_dictionary:
    driver.get(table['HREF'])
    table_element = driver.find_element_by_xpath('//table')
    col_table = create_dictionary_from_table_element(table_element)
    for col in col_table:
        col['COLUMN NAME'] = col['COLUMN NAME'].lower()
        col['TABLE NAME'] = table['TABLE NAME']
    all_columns_dictionary += col_table
driver.close()

if not os.path.exists('Data'):
    os.makedirs('Data')

with open('Data/CAS_table.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['database','cluster','schema','name','description','tags','is_view','description_source'])
    for x in table_of_tables_dictionary:
        writer.writerow([database,cluster,schema,x['TABLE NAME'],x['TABLE DESCRIPTION'],tags,is_view,description_source])

with open('Data/CAS_col.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['name','fname','description','col_type','sort_order','database','cluster','schema','table_name'])
    i = 0  
    for y in all_columns_dictionary:
        i += 1
        writer.writerow([y['COLUMN NAME'],'fname '+y['COLUMN NAME'],y['COLUMN DESCRIPTION'],'string',i,database,cluster,schema,y['TABLE NAME']])

with open('Data/CAS_table_column_stats.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['cluster','db','schema','table_name','col_name','stat_name','stat_val','start_epoch','end_epoch'])
    for y in all_columns_dictionary:
        writer.writerow([cluster,database,schema,y['TABLE NAME'],y['COLUMN NAME'],"Total records",'"'+y['TOTAL RECORDS']+'"',1432300762,1562300762])
        writer.writerow([cluster,database,schema,y['TABLE NAME'],y['COLUMN NAME'],"Completed records",'"'+y['COMPLETED RECORDS']+'"',1432300762,1562300762])
        writer.writerow([cluster,database,schema,y['TABLE NAME'],y['COLUMN NAME'],"Percentage",'"'+y['PERCENTAGE']+'"',1432300762,1562300762])

with open('Data/CAS_application.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['task_id','dag_id','exec_date','application_url_template','db_name','schema','table_name','cluster'])
    for y in table_of_tables_dictionary:
        table_name = y['TABLE NAME']
        writer.writerow([f'{cluster}.{schema}.{table_name}','event_test',"2018-05-31T00:00:00","https://airflow_host.net/admin/airflow/tree?dag_id={dag_id}",cluster,schema,table_name,database])

with open('Data/CAS_column_usage.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['database','cluster','schema','table_name','column_name','user_email','read_count'])
    i = 0
    while i < 20:
        i += 1
        column_index = random.randint(0, len(all_columns_dictionary)-1)
        x = all_columns_dictionary[column_index]
        user_index = random.randint(0, len(fake_users)-1)
        y = fake_users[user_index]
        z = random.randint(10, 100)
        writer.writerow([database,cluster,schema,x['TABLE NAME'],x['COLUMN NAME'],y,z])

with open('Data/CAS_schema_description.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['schema_key','schema','description'])
    schema_key = f'{database}://{cluster}.{schema}'
    writer.writerow([schema_key,schema,'cas schema description'])

with open('Data/CAS_source.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['db_name','cluster','schema','table_name','source','source_type'])
    for y in table_of_tables_dictionary:
        writer.writerow([database,cluster,schema,y['TABLE NAME'],y['HREF'],'CAS'])

with open('Data/CAS_table_last_updated.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['cluster','db','schema','table_name','last_updated_time_epoch'])
    for y in table_of_tables_dictionary:
        update_time = round(time.time())
        writer.writerow([cluster,database,schema,y['TABLE NAME'],update_time])

with open('Data/CAS_table_owner.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['db_name','schema','cluster','table_name','owners'])
    for y in table_of_tables_dictionary:
        z = random.randint(0, len(fake_users)-1)
        writer.writerow([database,schema,cluster,y['TABLE NAME'],fake_users[z]])

with open('Data/CAS_table_programmatic_source.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['database','cluster','schema','name','description','tags','description_source'])
    for y in table_of_tables_dictionary:
        writer.writerow([database,cluster,schema,y['TABLE NAME'],y['TABLE DESCRIPTION'],'',''])
