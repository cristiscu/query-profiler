"""
Created By:    Cristian Scutaru
Creation Date: Nov 2021
Company:       XtractPro Software
"""

import os, sys
import argparse
import configparser
import snowflake.connector
from pathlib import Path
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def dumpDictionary(props):
    for key in props:
        print(key + "=" + str(props[key]))

def sizeof_fmt(num, suffix = ""):
    orig_num = num
    suffix_bytes = f" ({orig_num} bytes)"
    for unit in [" bytes", " KB", " MB", " GB", " TB", " PB", " EB", " ZB"]:
        if abs(num) < 1024.0:
            s = f"{num:3.1f}{unit}{suffix}"
            return s if num == orig_num else s + suffix_bytes
        num /= 1024.0
    s = f"{num:.1f}Yi{suffix}"
    return s if num == orig_num else s + suffix_bytes

# check number of execs in the past month
def checkExecNumber(queryText, cur):
    cur.execute(
        "SELECT sum(TOTAL_ELAPSED_TIME) / 1000 as TOTAL_TIME_SECONDS, "
        "count(*) as NUMBER_OF_CALLS "
        "from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY "
        "where QUERY_TEXT = ? "
        "and TO_DATE(START_TIME) > DATEADD(month, -1, TO_DATE(CURRENT_TIMESTAMP()))",
        (queryText,))
    row = cur.fetchone()
    if row == None:
        print("It has not been executed at all in the last month.")
        return 0
    else:
        print(f"It has been executed {row[1]} times in the last month, for a total of {row[0]} seconds.")
        return int(row[1])

# among top N most frequently used queries?
def isFrequentQuery(queryText, n, cur):
    cur.execute(
        "with topFrequent as ( "
        "select case when not contains(QUERY_TEXT, '-- Looker Query Context') then QUERY_TEXT "
        "else left(QUERY_TEXT, position('-- Looker Query Context' in QUERY_TEXT)) end as query_text_cut "
        "from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY "
        "where TO_DATE(START_TIME) > DATEADD(month, -1, TO_DATE(CURRENT_TIMESTAMP())) "
        "and TOTAL_ELAPSED_TIME > 0 "
        "group by 1 "
        "having count(*) >= 2 "
        "order by count(*) desc "
        f"LIMIT {n}) "
        "select query_text_cut from topFrequent "
        "where query_text_cut = ?",
        (queryText,))
    row = cur.fetchone()
    if row == None:
        print(f"It is NOT among the top {n} most frequent queries executed in the last month.")
        return False
    else:
        print(f"It is among the top {n} most frequent queries executed in the last month.")
        return True

# among top N longest queries?
def isLongestQuery(queryText, n, cur):
    cur.execute(
        "with topLongest as ( "
        "select QUERY_TEXT "
        "from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY "
        "where TO_DATE(START_TIME) > DATEADD(month, -1, TO_DATE(CURRENT_TIMESTAMP())) "
        "and TOTAL_ELAPSED_TIME > 0 "
        "and ERROR_CODE iS NULL "
        "and PARTITIONS_SCANNED is not null "
        "group by query_text "
        "order by avg(TOTAL_ELAPSED_TIME) desc "
        f"LIMIT {n}) "
        "select query_text from topLongest "
        "where query_text = ?",
        (queryText,))
    row = cur.fetchone()
    if row == None:
        print(f"It is NOT among the top {n} longest queries executed in the last month.")
        return False
    else:
        print(f"It is among the top {n} longest queries executed in the last month.")
        return True

# among top N with most scanned data?
def isHeavyQuery(queryText, n, cur):
    cur.execute(
        "with topScanned as ( "
        "select QUERY_TEXT "
        "from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY "
        "where TO_DATE(START_TIME) > DATEADD(month, -1, TO_DATE(CURRENT_TIMESTAMP())) "
        "and TOTAL_ELAPSED_TIME > 0 "
        "and ERROR_CODE iS NULL "
        "and PARTITIONS_SCANNED is not null "
        "group by query_text "
        "order by avg(BYTES_SCANNED) desc "
        f"LIMIT {n}) "
        "select query_text from topScanned "
        "where query_text = ?",
        (queryText,))
    row = cur.fetchone()
    if row == None:
        print(f"It is NOT among the top {n} queries with most data scanned executed in the last month.")
        return False
    else:
        print(f"It is among the top {n} queries queries with most data scanned executed in the last month.")
        return True

def showQueryHistory(props, isAccountUsage, cur):
    """
    Display info from QUERY_HISTORY, from either ACCOUNT_USAGE or INFORMATION_SCHEMA
    """

    # query SQL statement and ID
    print("=========================================================")
    queryText = props['QUERY_TEXT']
    print(queryText)
    print("=========================================================")
    queryId = props['QUERY_ID']
    print(f"The query ID is {queryId}.")

    # query execution
    if props['EXECUTION_STATUS'] != 'SUCCESS':
        print(f"\nThe query failed with error code {props['ERROR_CODE']}: {props['ERROR_MESSAGE']}")
    else:
        print(
            f"\nThe query has been run successfully in {props['TOTAL_ELAPSED_TIME']:,} ms: "
            f"it compiled in {props['COMPILATION_TIME']:,} ms "
            f"+ it executed in {props['EXECUTION_TIME']:,} ms.")
        print(f"The query run between {props['START_TIME']} and {props['END_TIME']}.")

        if isAccountUsage:
            print()

            # check number of calls in the past month
            if checkExecNumber(queryText, cur) > 1:
                isFrequentQuery(queryText, 10, cur)

            # among top 10 or 100 longest queries?
            if isLongestQuery(queryText, 10, cur) == False:
                isLongestQuery(queryText, 100, cur)

            # among top 10 or 100 with most scanned data?
            if isHeavyQuery(queryText, 10, cur) == False:
                isHeavyQuery(queryText, 100, cur)

    # user/role/database/schema context
    print(f"\nThe query was executed by the {props['USER_NAME']} user, using the {props['ROLE_NAME']} role.")

    if props['DATABASE_NAME'] != None and props['SCHEMA_NAME'] != None:
        print(f"The query was executed within the {props['DATABASE_NAME']}.{props['SCHEMA_NAME']} database and schema context.")

    # warehouse + credit
    wh_size = (f"{props['WAREHOUSE_SIZE']} "
        if props['WAREHOUSE_SIZE'] != None
        else "")
    load_percent = (f"{props['QUERY_LOAD_PERCENT']}% of resources available, "
        if isAccountUsage and 'QUERY_LOAD_PERCENT' in props and props['QUERY_LOAD_PERCENT'] != None
        else "")
    nodes = (f"with {props['CLUSTER_NUMBER']} nodes, "
        if props['CLUSTER_NUMBER'] != None
        else "")
    print(
        f"The query used the {wh_size}{props['WAREHOUSE_NAME']} "
        f"{props['WAREHOUSE_TYPE'].lower()} warehouse, {nodes}{load_percent}"
        f"with {props['CREDITS_USED_CLOUD_SERVICES']} cloud compute credits.")

    # results
    if props['ROWS_PRODUCED'] == None:
        print(f"\nThe query produced no rows.")
    else:
        print(f"\nThe query produced {props['ROWS_PRODUCED']} rows.")

    if isAccountUsage and 'ROWS_INSERTED' in props and int(props['ROWS_INSERTED']) > 0:
        print(f"{props['ROWS_INSERTED']} rows have been inserted, for a total of {sizeof_fmt(props['BYTES_WRITTEN'])}.")
    if isAccountUsage and 'ROWS_DELETED' in props and int(props['ROWS_DELETED']) > 0:
        print(f"{props['ROWS_DELETED']} rows have been deleted, for a total of {sizeof_fmt(props['BYTES_DELETED'])}.")
    if isAccountUsage and 'ROWS_UPDATED' in props and int(props['ROWS_UPDATED']) > 0:
        print(f"{props['ROWS_UPDATED']} rows have been updated.")
    if isAccountUsage and 'ROWS_UNLOADED' in props and int(props['ROWS_UNLOADED']) > 0:
        print(f"{props['ROWS_UNLOADED']} rows have been unloaded.")

    # query queued
    if int(props["QUEUED_PROVISIONING_TIME"]) > 0:
        print(
            f"\nThe query has been queued for {props['QUEUED_PROVISIONING_TIME']} ms, "
            f"waiting for the warehouse to provision, due to the warehouse creation, resume, or resize.")
    if int(props["QUEUED_REPAIR_TIME"]) > 0:
        print(
            f"\nThe query has been queued for {props['QUEUED_REPAIR_TIME']} ms, "
            f"waiting for compute resources in the warehouse to be repaired.")
    if int(props["QUEUED_OVERLOAD_TIME"]) > 0:
        print(
            f"\nThe query has been queued for {props['QUEUED_OVERLOAD_TIME']} ms, "
            f"due to the warehouse being overloaded by the current query workload.")

    if int(props["TRANSACTION_BLOCKED_TIME"]) > 0:
        print(f"\nThe query has been blocked for {props['TRANSACTION_BLOCKED_TIME']} ms by transactions.")

    # bytes spilled
    if isAccountUsage and 'BYTES_SPILLED_TO_LOCAL_STORAGE' in props:
        if int(props["BYTES_SPILLED_TO_LOCAL_STORAGE"]) >= 1000000:
            print(
                f"\nOver 1MB - {sizeof_fmt(props['BYTES_SPILLED_TO_LOCAL_STORAGE'])} - spilled to local storage. "
                "\nThis could mean that your warehouse nodes do not have enough RAM. "
                "They have to do swap with the local SSD disk too frequently. "
                "\nHint: You may need a larger warehouse.")
        elif int(props["BYTES_SPILLED_TO_LOCAL_STORAGE"]) == 0:
            print("\nNothing spilled to local storage, which is good. "
            "\nThis often means that the warehouse node(s) had enough memory to process it all in RAM.")
        else:
            print(f"\n{sizeof_fmt(props['BYTES_SPILLED_TO_LOCAL_STORAGE'])} spilled to local storage.")

        if int(props["BYTES_SPILLED_TO_REMOTE_STORAGE"]) >= 1000000:
            print(
                f"\nOver 1MB - {sizeof_fmt(props['BYTES_SPILLED_TO_REMOTE_STORAGE'])} - spilled to remote storage. "
                "\nThis could mean that your warehouse nodes do not have large enough SSD disks. "
                "The query had to access way to frequently the remote S3 or Azure Blog storage. "
                "\nYou may need a larger warehouse.")
        elif int(props["BYTES_SPILLED_TO_REMOTE_STORAGE"]) == 0:
            print("\nNothing spilled to remote storage, which is good. "
            "\nThis often means that the warehouse node(s) had enough RAM and SSD disk space to process it all locally.")
        else:
            print(f"\n{sizeof_fmt(props['BYTES_SPILLED_TO_REMOTE_STORAGE'])} spilled to remote storage.")
        print("See https://community.snowflake.com/s/article/Performance-impact-from-local-and-remote-disk-spilling.")

    # bytes scanned
    print(f"\nThe query scanned a total of {sizeof_fmt(props['BYTES_SCANNED'])}.")
    if int(props['BYTES_SCANNED']) > 10000000:
        print("Hint: Consider reducing the amount of data a query needs to read from the tables.")
        #print("See .")

    # caching
    if isAccountUsage and 'PERCENTAGE_SCANNED_FROM_CACHE' in props:
        if float(props['PERCENTAGE_SCANNED_FROM_CACHE']) == 1.0:
            print(
                "\nAll your data has been served from the result cache, "
                "so the query did not execute again, and did not consume any compute resources. "
                "\nThis is great! The query result will still be here for at least 24 hours."
                "\nSee https://community.snowflake.com/s/article/Understanding-Result-Caching.")
        elif float(props['PERCENTAGE_SCANNED_FROM_CACHE']) > 0.8:
            print(
                f"\nMore than 80% ({props['PERCENTAGE_SCANNED_FROM_CACHE']}) of your data has been found in the result cache. "
                "This is good.")
        elif float(props['PERCENTAGE_SCANNED_FROM_CACHE']) < 0.5:
            print(
                f"\nLess than 5% ({props['PERCENTAGE_SCANNED_FROM_CACHE']}) of your data has been found in the result cache. "
                "\nHint: Look for consecutive queries that could use the query result cache."
                "\nSee https://community.snowflake.com/s/article/Understanding-Result-Caching.")
        else:
            print(
                f"\n{props['PERCENTAGE_SCANNED_FROM_CACHE']} of your data has been found in the result cache."
                "\nSee https://community.snowflake.com/s/article/Understanding-Result-Caching for more info.")

    # partitions and pruning
    if isAccountUsage and 'PARTITIONS_TOTAL' in props and int(props['PARTITIONS_TOTAL']) > 0:
        if int(props['PARTITIONS_SCANNED']) == int(props['PARTITIONS_TOTAL']):
            print(
                f"\nYou had a full table scan, for all {props['PARTITIONS_TOTAL']:,} partitions. "
                "\nHint: Consider improving partition pruning, by eventually adding some cluster key, or a filter.")
        elif int(props['PARTITIONS_SCANNED']) <= 0.2 * int(props['PARTITIONS_TOTAL']):
            print(
                f"\n{props['PARTITIONS_SCANNED']:,} partitions out of a total of {props['PARTITIONS_TOTAL']:,} have been scanned. "
                "\nPartition pruning (and your current cluster keys) seem efficient for this query.")
        else:
            print(
                f"\n{props['PARTITIONS_SCANNED']:,} partitions out of a total of {props['PARTITIONS_TOTAL']:,} have been scanned. "
                "\nHint: Consider improving partition pruning, by eventually adding some cluster key, or a filter.")

        print("See https://community.snowflake.com/s/article/How-to-recognize-unsatisfactory-pruning.")

    # inbound/outbound
    if int(props['INBOUND_DATA_TRANSFER_BYTES']) > 0:
        print(
            f"\nThe query received {sizeof_fmt(props['INBOUND_DATA_TRANSFER_BYTES'])} "
            f"from the {props['INBOUND_DATA_TRANSFER_CLOUD=None']} inbound acount, "
            f"in the {props['INBOUND_DATA_TRANSFER_REGION']} region.")
    if int(props['OUTBOUND_DATA_TRANSFER_BYTES']) > 0:
        print(
            f"\nThe query sent {sizeof_fmt(props['OUTBOUND_DATA_TRANSFER_BYTES'])} "
            f"to the {props['OUTBOUND_DATA_TRANSFER_CLOUD=None']} outbound acount, "
            f"in the {props['OUTBOUND_DATA_TRANSFER_REGION']} region.")

    # external functions
    if int(props['EXTERNAL_FUNCTION_TOTAL_INVOCATIONS']) > 0:
        print(
            f"\nYour query called {props['EXTERNAL_FUNCTION_TOTAL_INVOCATIONS']} times external functions. "
            f"\n{props['EXTERNAL_FUNCTION_TOTAL_SENT_ROWS']} rows have been send, {props['EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS']} rows received. "
            f"\n{sizeof_fmt(props['EXTERNAL_FUNCTION_TOTAL_SENT_BYTES'])} have been send, {sizeof_fmt(props['EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES'])} received.")


def connect(connect_mode, account, user, role, warehouse, database, schema):

    # (a) connect to Snowflake with SSO
    if connect_mode == "SSO":
        return snowflake.connector.connect(
            account = account,
            user = user,
            role = role,
            database = database,
            schema = schema,
            warehouse = warehouse,
            authenticator = "externalbrowser",
            paramstyle = 'qmark'
        )

    # (b) connect to Snowflake with username/password
    if connect_mode == "PWD":
        return snowflake.connector.connect(
            account = account,
            user = user,
            role = role,
            database = database,
            schema = schema,
            warehouse = warehouse,
            password = os.getenv('SNOWFLAKE_PASSWORD'),
            paramstyle = 'qmark'
        )

    # (c) connect to Snowflake with key-pair
    if connect_mode == "KEY-PAIR":
        with open(f"{str(Path.home())}/.ssh/id_rsa_snowflake_demo", "rb") as key:
            p_key= serialization.load_pem_private_key(
                key.read(),
                password = None, # os.environ['SNOWFLAKE_PASSPHRASE'].encode(),
                backend = default_backend()
            )
        pkb = p_key.private_bytes(
            encoding = serialization.Encoding.DER,
            format = serialization.PrivateFormat.PKCS8,
            encryption_algorithm = serialization.NoEncryption())

        return snowflake.connector.connect(
            account = account,
            user = user,
            role = role,
            database = database,
            schema = schema,
            warehouse = warehouse,
            private_key = pkb,
            paramstyle = 'qmark'
        )

def main():
    """
    Main entry point of the CLI
    """

    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', dest='queryId')
    parser.add_argument('--sql', dest='queryText')
    parser.add_argument('--file', dest='queryFile')
    args = parser.parse_args()

    queryText = None
    queryId = None
    if args.queryId != None:
        queryId = args.queryId
        print(f"Getting query by ID...")
        
    elif args.queryText != None:
        queryText = args.queryText
        print(f"Getting query by SQL...")

    elif args.queryFile != None:
        with open(args.queryFile) as f:
            queryText = f.read()
        print(f"Getting query by SQL from {args.queryFile} file...")

    else:
        print(f"Usage: python query-profiler.py [option]\n"
            + "--id queryId              - by the query ID from Snowflake\n"
            + "--sql 'queryText'         - the query is passed inline, between single quotes\n"
            + "--file queryFile          - the query is stored in a text file (usually myquery.sql)\n")
        sys.exit(2)

    # read profiles_db.conf
    parser = configparser.ConfigParser()
    parser.read("profiles_db.conf")
    section = "default"
    account = parser.get(section, "account")
    user = parser.get(section, "user")
    role = parser.get(section, "role")
    warehouse = parser.get(section, "warehouse")
    database = parser.get(section, "database")
    schema = parser.get(section, "schema")

    # change this to connect in a different way: SSO / PWD / KEY-PAIR
    connect_mode = "PWD"
    con = connect(connect_mode, account, user, role, warehouse, database, schema)
    cur = con.cursor()

    # look in account_usage
    props = {}
    isAccountUsage = True
    row = None

    if queryId != None:
        # look in account_usage by query ID
        cur.execute(
            "select top 1 * "
            "from snowflake.account_usage.query_history "
            "where query_id = ?",
            (queryId,))
        row = cur.fetchone()
        if row != None:
            print("The query was found by ID in the ACCOUNT_USAGE schema.")
        else:
            # look in information_schema by query ID
            print("The query (by ID) is not in the ACCOUNT_USAGE schema yet. Trying the INFORMATION_SCHEMA instead...")
            isAccountUsage = False
            cur.execute(
                "select top 1 * "
                "from table(information_schema.query_history()) "
                "where query_id = ?",
                (queryId,))
            row = cur.fetchone()
            if row != None:
                print("The query was found by ID in INFORMATION_SCHEMA.")
            else:
                print("Query not found (by ID) in INFORMATION_SCHEMA. Try the History tab in the Web UI.")
                return

    else:
        # look in account_usage by query SQL
        cur.execute(
            "select top 1 * "
            "from snowflake.account_usage.query_history "
            "where query_text = ?"
            "order by start_time desc",
            (queryText,))
        row = cur.fetchone()
        if row != None:
            print("The query was found by SQL in the ACCOUNT_USAGE schema.")
        else:
            # look in information_schema by query SQL
            print("The query (by SQL) is not in the ACCOUNT_USAGE schema yet. Trying the INFORMATION_SCHEMA instead...")
            cur.execute(
                "select top 1 * "
                "from table(information_schema.query_history()) "
                "where query_text = ?"
                "order by start_time desc",
                (queryText,))
            row = cur.fetchone()
            if row != None:
                print("The query was found by SQL in INFORMATION_SCHEMA.")
            else:
                print("Query not found (by SQL) in INFORMATION_SCHEMA. Running the query...")

                # execute SQL query, and get query ID
                cur.execute(queryText)
                cur.execute("select last_query_id()")
                row = cur.fetchone()
                queryId = row[0]

                # look in information_schema by query ID
                cur.execute(
                    "select top 1 * "
                    "from table(information_schema.query_history()) "
                    "where query_id = ?",
                    (queryId,))
                row = cur.fetchone()

    # collect all row metrics values for this query
    for idx, col in enumerate(cur.description):
        props[col[0]] = row[idx]

    # EXPLAIN plan
    cur.execute(f"explain using text {props['QUERY_TEXT']}")
    row = cur.fetchone()
    explain = row[0]

    # fill-in some properties from the explain plan
    lines = explain.splitlines()
    props["PARTITIONS_TOTAL"] = int(lines[1].split("=")[1].strip())
    props["PARTITIONS_SCANNED"] = int(lines[2].split("=")[1].strip())
    props["BYTES_SCANNED"] = int(lines[3].split("=")[1].strip())

    showQueryHistory(props, isAccountUsage, cur)

    # show the explain plan, in tabular form
    print("=========================================================")
    print("EXPLAIN PLAN:")
    print(explain)

    con.close()

if __name__ == "__main__":
    main()
