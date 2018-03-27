
# coding: utf-8

# <h1>How Much Water?</h1>
# 
# The goal of this project is to download data from an API, parse the data, load it into a local database and create an analytical reports for this data. For this exercise we will be pulling data from the United States Geological Survey (USGS) water service to determine how much *streamflow* was recorded at the Ohio River by the flow meter at the Louisville Water Station (03292494). The user can enter dates of their choosing if they like.
# 
# **Version**
# <p>This tool was made with Python 3.6.1 and Jupyter Notebooks 5.0.0</p>
# <p>A requirements.txt accompanies this file, which defines all of the requirements used (including others) in developing this tool. Be sure to install the requests and bokeh packages into your environment before running the program.</p>
# 
# **Tools**
# * *USGS Data* :https://waterservices.usgs.gov/rest/DV-Service.html
# * *Markdown* :https://guides.github.com/pdfs/markdown-cheatsheet-online.pdf
# * *USGS Data Requests Helper* :https://waterservices.usgs.gov/rest/DV-Test-Tool.html
# * *Bokeh* :https://bokeh.pydata.org/en/latest/
# * *SQLite3* :https://docs.python.org/2/library/sqlite3.html
# 
# **Special Thanks to**
# 
# I just wanted to say a very big thanks to all of mentors at Code Louisville who guided, taught and helped me get to this point!
# 

# In[24]:

#import pandas as pd
import os
import csv
import requests
import json
from datetime import datetime, date, timedelta
from collections import namedtuple
import sqlite3
from bokeh.io import output_file, show, output_notebook
from bokeh.plotting import figure
from bokeh.models import HoverTool, NumeralTickFormatter, ColumnDataSource


# <h2>Getting the Date parameters</h2>
# 
# So we need to get the date parameters to construct it into a string for our request to the API. We create a function that asks the user to make the request. I also added in some validation so that its in the proper format.

# In[25]:

site = '03292494'
tbl_name = 'site_'+site

def enterdate(text_date):
    #create a condition where the user is asked for a correct date
    exit = False
    while(not exit):
        date_entry = input(f'Enter {text_date} in YYYY-MM-DD format: ')
        try:
            year, month, day = map(int, date_entry.split('-'))
            try:
                date1 = date(year, month, day)
                exit = True
                return date1
            except ValueError:
                print(f"That is not a valid {text_date}, please enter a date in the YYYY-MM-DD format")
        except ValueError:
            print(f"That is not a valid {text_date}, please enter a date in the YYYY-MM-DD format")

#create a continous loop so that the user will be asked if they
out = False
while(not out):
    #set end date variable
    enddate = str(enterdate("End Date"))

    #check if the end date is valid, if not reset it to today
    checkdate = datetime.strptime(enddate, '%Y-%m-%d').date()
    if checkdate > date.today():
        enddate = str(date.today())
        print("Great Scott! that date is way into the future! I'll reset it to {}.".format(enddate))

    startdate = str(enterdate("Start Date"))

    #check if the end date is greater than the start date
    end_date = datetime.strptime(enddate, '%Y-%m-%d').date()
    start_date = datetime.strptime(startdate, '%Y-%m-%d').date()

    if end_date <= start_date:
        one_week = timedelta(weeks=1)
        start_date = end_date - one_week
        print(f'The Start Date should be less than the End date\nI set the value to {start_date}')
        startdate = str(start_date)
    
    #allow user to check date params
    print(f'\nStart Date: {startdate}\nEnd Date: {enddate}\n')
    escape = input('Are you happy with these parameters? [Y/n] ').strip('').lower()
    if escape == 'y':
        out = True
        url = 'http://waterservices.usgs.gov/nwis/dv/?format=json&indent=on&sites={}&startDT={}&endDT={}&siteStatus=all'.format(site,startdate,enddate)
        #print(url)

#print(enddate)




# <h2>Also, check if the response of the request</h2>

# In[26]:

test_url = requests.get(url)
if test_url.status_code != 200:
    print(f'Hey there, something went wrong with our request.\nIt returned as status code of {test_url.status_code}.\nAre we sure about {site} as the site name?')
else:
    print("The request returned some data")


# <h2>Parsing</h2>
# 
# Here we use the json library to get the text response and access the values. I printed them out here so we can check if we are getting the correct response.

# In[27]:

response_url = requests.get(url)
parsed_json = json.loads(response_url.text)

#print(parsed_json['value']['timeSeries'][0]['values'][0]['value'])
#print()

#checking the values so that we can get to the data we want.
print(parsed_json['value']['timeSeries'][0]['values'][0]['value'][0]['value'])
print(parsed_json['value']['timeSeries'][0]['values'][0]['value'][3])
#print(parsed_json)


# In[28]:

#save the list of values in a variable. (because...)
daily_dis = parsed_json['value']['timeSeries'][0]['values'][0]['value']

#lets look at the data...
def insp_Request(request_list):
    for value in request_list:
        #get the current value of the date and change it to a datetime object, well actually the datetime object should be stored in sqllite.
        date_text = value['dateTime'][:10]
        #date = datetime.strptime(date_text, '%Y-%m-%d').date()

        #get the depth and cast it as a string
        discharge = int(value['value'])

        #get the qualifier for the data you downloaded. note that P means "provisional"
        qualifier = value['qualifiers'][0]

        print('Date: {} \tDischarge: {}\t Qualifier: {}'.format(date_text,discharge,qualifier))

#insp_Request(daily_dis)


# <h2>Using the NamedTuple</h2>
# 
# So now that we have our data where we want it we can now load it into a SQL database. But before we do that we should use the namedtuple parameter in case we had to handle a larger table to load into sql

# In[29]:

daily_discharge = namedtuple('daily_discharge','date, discharge, qualifier')
discharge_list = []

#lets now pack that data into a namedtuple...
for val in daily_dis:
    #get the current value of the date and change it to a datetime object
    date_text = val['dateTime'][:10]
    #date = datetime.strptime(date_text, '%Y-%m-%d').date()
    
    #get the depth and cast it as a string
    discharge = int(val['value'])
    
    #get the qualifier for the data you downloaded. note that P means "provisional"
    qualifier = val['qualifiers'][0]
    
    discharge_list.append(daily_discharge(date_text,discharge,qualifier))
    
#check how you can access the namedtuple
#print(discharge_list[0].discharge)
#print(discharge_list[0].date)
#print(discharge_list[0].qualifier)
print(discharge_list[0])


# In[9]:

#checking the range of what we are about to input into the database.
print(discharge_list[0].date)
print(discharge_list[len(discharge_list)-1].date)


# <h2>Loading into the SQLite Database</h2>
# 
# We load it into the SQL database, take note that since we are loading this into a SQLite database. You should know that SQLite does not store dates as date types. Its stored as REAL, TEXT or INTERGER. Lets use integer and create a database using a cursor methon on the sql database object. Then we call sql statements using the execute command.
# 
# *Check this link for more info: https://docs.python.org/2/library/sqlite3.html*
# 
# **Design Decision**
# 
# When i was thinking about the design of this i wanted the user to be able to run this script again and load as much data they wanted *or needed* in the database for reporting. So the next step goes through checking if the table already exists and then later checking if the record for that table already exists to prevent duplication.

# In[30]:

conn = sqlite3.connect('daily_discharge.db')
cur = conn.cursor()


# In[31]:

sql_text = (tbl_name,)
#print(sql_text)
#check if the table exists, TRUE if the select statement returns nothing, FALSE if it returns something.  
List_tables = cur.execute('SELECT name FROM sqlite_master WHERE type="table" AND name=?',sql_text)

Not_exists = True

#loop through the returned table and output check if it exists
for i in List_tables:
    #print(i)
    if i[0] == tbl_name:
        print("The table already exists") #found the matching table and breaks it.
        Not_exists = False
        break
        


# In[32]:

#if the table exists do nothing
if(Not_exists):
    print(f'The table, {tbl_name} does not exist yet\n Creating {tbl_name}')
    #create the table if it does not exist
    try:
        query = f'CREATE TABLE {sql_text[0]}(date TEXT, discharge REAL, qualifier TEXT)'
        cur.execute(query)
        conn.commit()
    except:
        print("This table may already exist.")
else:
    print('The table {} already exists'.format(sql_text[0]))
    #insert the values of the table into the created table


# <h2>Preventing Duplicate Records</h2>
# This is a time series dataset for each day of the station and there could a point where we request data that is already in the dataset. So lets run a few sql commands to the existing database to see if the data is already there. If it does, then we just ignore it (#needResearch). If not, then we insert that record.

# In[33]:

for record in discharge_list:
    date = (tbl_name, record.date)
    #print(date)
    sql_stmt = f'SELECT date FROM {date[0]} WHERE date="{date[1]}"'
    check = cur.execute(sql_stmt).fetchone() is not None
    if(check):
        #The record is there...so continue to the next record.
        #print("returned True, is not None")
        continue
        #print("this is ignored")
    else:
        #The record is not there so insert it
        sql_stmt = f'INSERT INTO {date[0]} VALUES(?,?,?)'
        value = (record.date, record.discharge, record.qualifier)
        #print("returned False, the record is not there")
        cur.execute(sql_stmt, value)
        conn.commit()


# <h2>Getting Data Back for Reports</h2>
# Great! Now we have data that can persist through multiple uses! Woop Woop! Next is we get all of the records that we have to see the trend of waterflow that went pass the sensor at the Louisville Water Tower station.

# In[34]:

results = cur.execute('SELECT * FROM site_03292494 ORDER BY date DESC')

dates = []
discharges = []
for row in results:
    #print('Date: {} \tDischarge: {} \t Qualifier:{}'.format(row[0],row[1],row[2]))
    if row[1] > 0:
        discharges.append(row[1])
        date_text = row[0]
        date = datetime.strptime(date_text, '%Y-%m-%d').date()
        dates.append(date)
    else:
        continue
    
    
    



# <h2>Using Bokeh for Plots</h2>
# In order to see that data we just pulled I used Bokeh to plot it out.

# In[35]:

output_notebook()


# <h1>How Much Water?</h1>
# 
# As you can see that the peak flow of the request is done. The user can keep making the same requests for a larger and larger database. If the user looked for data for 2018-01-01 to 2018-03-01 then the user would have seen that *655,000 cubic feet/second* of water flowed through the Ohio River Station at the Louisville Water Tower. Thats a lot!

# In[36]:

#output_file("discharges.html")

TOOLs = "pan,lasso_select,box_select,tap,zoom_in,zoom_out"

source = ColumnDataSource(data=dict(dates=dates,discharges=discharges))

plot_fig = figure(plot_width=700, plot_height=500, title="Ohio River Station 03292494", x_axis_type='datetime', tools=TOOLs)

plot_fig.circle('dates', 'discharges', size=8, color='navy', alpha=0.5, source=source)
plot_fig.line('dates','discharges', color='navy', source=source)

hover = HoverTool(
    tooltips=[
        ('Discharge', '@discharges')
    ])

#add the hover tool tips
plot_fig.add_tools(hover)

#format the axes
plot_fig.yaxis.formatter = NumeralTickFormatter(format='0,0')
plot_fig.yaxis.axis_label = "Streamflow (cfs)"

show(plot_fig)


# In[23]:

#disconnect to the sql database
conn.close()


# what if it was stored in a csv? the data is stored as a text file in the data folder. Now based on the file structure there is a data folder where the root of the 
# 
# **More research**
# look up 
# *click libraries
# *goey libraries
# 
# https://pypi.python.org/pypi/Gooey

# ```
# #get the current working directory
# path = os.getcwd()
# file = 'MSDRainGaugeReport25.csv'
# folder = '\\data\\'
# full_file = path+folder+file
# 
# with open(full_file, 'rt') as csvfile:
#     read_csv_data = csv.reader(csvfile)
#     #print(read_csv_data)
#     for row in read_csv_data:
#         print(row)
# ```
