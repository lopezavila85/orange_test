#!/bin/bash

# Create HDFS folder
hdfs dfs -mkdir -p /user/cloudera/orange/birthday

# Load birthdays CSV to HDFS
hdfs dfs -put birthdays.csv /user/cloudera/orange/birthday

# Create birthday table in hive
hive -f create_birthday.hql

# Obtain most birthdays on one day. The year is not considered.
hive -f most_date_birthdays.hql

# Obtain the people whose birthday is in the next eleven months
hive -f birthdays_in_eleven_months.hql

# Obtain the people whose birthday is in the next eleven months. Least frequencies
hive -f birthdays_in_eleven_months_least.hql
