#!/bin/bash

# Copy billinginfo data to target host. This is too large to do all at once, so we do it one day at a time.

last_date="2022-09-09";
for date in $(cat dates.txt)
do
  if [[ "$(cat started_dates.txt)" == *"$last_date"* ]]
  then
    echo "$last_date in started_dates.txt, skipping"
  else
    echo "copying >= $last_date and < $date"
    echo "$last_date" >> started_dates.txt
    copied=$(psql -U enstore_reader --command="\copy (select * from billinginfo where datestamp >= '$last_date' and datestamp < '$date') to STDOUT" billing | psql -U postgres -h $TARGET_HOST -c '\copy billinginfo from STDIN' billing)
    echo "$last_date: $copied ($(date +"%d-%m-%y %T"))" >> copied_dates.txt
  fi
  last_date="$date"
done
