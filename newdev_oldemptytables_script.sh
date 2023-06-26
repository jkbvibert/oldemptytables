## Identify empty tables that are older than 7 days and are not in an exceptions table (where we put tables that are ok to exceed this rule)
#check to make sure all three parameters are entered
if [ $# -ne 3 ]
then
   echo "< 3 parameters <target dbname> <db hostname> <notice email groups>"
   echo "< 3 parameters <target dbname> <db hostname> <notice email groups>" | mailx -r <team_email> -s "Issue with $0 on $1" <my_email>
   exit 1
fi

#set the entered parameters to variables
export DBNAME=$1
export HOSTNM=$2


export VSQL="/opt/vertica/bin/vsql -U <service_acct> -w <password> -d $DBNAME -h $HOSTNM -P footer=off -p 5433"
EMAIL_FROM="<team_email>"
EMAIL_TO="<my_email>"
export SCRDIR=/home/srvc_bds_tidal/vertica/mnt_scripts_daily

#get output for whether database is up or not
$VSQL << EOF
\o /tmp/node_status.out
select node_name, node_address, node_state from nodes;
\o
\q
EOF

## validate if database is exist and up running
if [ $? -ne 0 ]
then
   echo "Some error occurred when checking \"select node_name, node_address, node_state from nodes;\". \nIt likely did not complete successfully." | mailx -r <team_email> -s "Not able to connect to DB $2" <my_email>
   exit 1
fi

export DBNAME=`cat /tmp/node_status.out | grep "^ v_" | head -1 | awk '{print $1}'|cut -d"_" -f2-|rev|cut -d"_" -f2-|rev`

if [ $1 != $DBNAME ]
then
   echo "The db name entered in the command did not match the db name in the \"nodes\" table" | mailx -r <team_email> -s "Not able to connect to DB $2" <my_email>
   exit 1
fi

ssh vibertj@$HOSTNM << EOF
pbrun su - vertica -c "/opt/vertica/bin/vsql -t -c \"select 'drop table '||anchor_table_schema||'.'||anchor_table_name||' CASCADE;' from (select distinct anchor_table_schema, anchor_table_name from projection_storage left outer join schemata on projection_storage.anchor_table_schema = schemata.schema_name where row_count = 0 and create_time <= (current_timestamp - INTERVAL '7 days') and anchor_table_schema <> 'dba_test' and anchor_table_schema <> 'hp_metrics' except select schema_name, table_name from dev_automatedrestrictions.emptytable_exceptions)a order by 1;\" | /usr/bin/tee /home/vertica/newdev_oldemptytables_input.txt" #rows marked for deletion, but not purged  will still count here. Leaving in since there is no other viable alternatives found and job run occurrance is 7 days, a sizeable timeframe to likely not catch too many of these cases.
pbrun su - vertica -c "/bin/sed -i '$d' /home/vertica/newdev_oldemptytables_input.txt" #remove the last line of the file
pbrun su - vertica -c "/bin/sed -i -e 's/^.//' /home/vertica/newdev_oldemptytables_input.txt" #remove the random space at the beginning of each line
pbrun su - vertica -c "/opt/vertica/bin/vsql -f /home/vertica/newdev_oldemptytables_input.txt | /usr/bin/tee /home/vertica/newdev_oldemptytables_erroroutput.txt" #run the input file as vsql commands and output to a txt file
EOF

rm /tmp/node_status.*

: <<'COMMENT'
create schema dev_automatedrestrictions;
create table dev_automatedrestrictions.emptytable_exceptions(schema_name varchar(128), table_name varchar(128));

insert into dev_automatedrestrictions.emptytable_exceptions values ('schema_name', 'table_name');
COMMENT
