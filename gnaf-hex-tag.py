import multiprocessing
import math
import os
import subprocess
import psycopg2
import argparse

from datetime import datetime


def main():
    parser = argparse.ArgumentParser(
        description='Tags GNAF address points with hex PIDs.')
    parser.add_argument(
        '--max-processes', type=int, default=6,
        help='Maximum number of parallel processes to use for the data load. (Set it to the number of cores on the '
             'Postgres server minus 2, limit to 12 if 16+ cores - there is minimal benefit beyond 12). Defaults to 6.')

    # hex options
    parser.add_argument(
        '--min-hex-width', type=float, default=0.3,
        help='Minimum hex width (in km).')
    parser.add_argument(
        '--hex-multiplier', type=int, default=2,
        help='The multiplier used to increase hex size until max width is reached.')
    parser.add_argument(
        '--max-hex-width', type=float, default=200,
        help='Maximum hex width (in km).')

    # PG Options
    parser.add_argument(
        '--pghost',
        help='Host name for Postgres server. Defaults to PGHOST environment variable if set, otherwise localhost.')
    parser.add_argument(
        '--pgport', type=int,
        help='Port number for Postgres server. Defaults to PGPORT environment variable if set, otherwise 5432.')
    parser.add_argument(
        '--pgdb',
        help='Database name for Postgres server. Defaults to PGDATABASE environment variable if set, '
             'otherwise psma_201602.')
    parser.add_argument(
        '--pguser',
        help='Username for Postgres server. Defaults to PGUSER environment variable if set, otherwise postgres.')
    parser.add_argument(
        '--pgpassword',
        help='Password for Postgres server. Defaults to PGPASSWORD environment variable if set, '
             'otherwise \'password\'.')

    # schema names for the raw gnaf, flattened reference and admin boundary tables
    parser.add_argument(
        '--gnaf-schema', default='gnaf',
        help='Schema name of the ready to use GNAF tables. Defaults to \'gnaf\'.')
    parser.add_argument(
        '--hex-schema', default='hex',
        help='Destination schema name to store hex tables in. Defaults to \'hex\'.')

    # # states to load
    # parser.add_argument('--states', nargs='+', choices=["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"],
    #                     default=["ACT", "NSW", "NT", "OT", "QLD", "SA", "TAS", "VIC", "WA"],
    #                     help='List of states to load data for. Defaults to all states.')

    args = parser.parse_args()

    settings = dict()
    settings['min_hex_width'] = args.min_hex_width
    settings['hex_multiplier'] = args.hex_multiplier
    settings['max_hex_width'] = args.max_hex_width

    settings['max_concurrent_processes'] = args.max_processes
    # settings['states_to_load'] = args.states
    settings['gnaf_schema'] = args.gnaf_schema
    settings['hex_schema'] = args.hex_schema

    # create postgres connect string
    settings['pg_host'] = args.pghost or os.getenv("PGHOST", "localhost")
    settings['pg_port'] = args.pgport or os.getenv("PGPORT", 5432)
    settings['pg_db'] = args.pgdb or os.getenv("PGDATABASE", "psma_201602")
    settings['pg_user'] = args.pguser or os.getenv("PGUSER", "postgres")
    settings['pg_password'] = args.pgpassword or os.getenv("PGPASSWORD", "password")

    settings['pg_connect_string'] = "dbname='{0}' host='{1}' port='{2}' user='{3}' password='{4}'".format(
        settings['pg_db'], settings['pg_host'], settings['pg_port'], settings['pg_user'], settings['pg_password'])

    # set postgres script directory
    settings['sql_dir'] = os.path.join(os.path.dirname(os.path.realpath(__file__)), "postgres-scripts")

    full_start_time = datetime.now()

    # connect to Postgres
    try:
        pg_conn = psycopg2.connect(settings['pg_connect_string'])
    except psycopg2.Error:
        print "Unable to connect to database\nACTION: Check your Postgres parameters and/or database security"
        return False

    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # tag GNAF addresses with hex PIDs
    start_time = datetime.now()
    print ""
    print "Start hex tagging points : {0}".format(start_time)
    hex_tag_gnaf(pg_cur, settings)
    print "Points hex tagged: {0}".format(datetime.now() - start_time)

    pg_cur.close()
    pg_conn.close()

    print "Total time : : {0}".format(datetime.now() - full_start_time)


def hex_tag_gnaf(pg_cur, settings):
    start_time = datetime.now()

    pg_cur.execute("CREATE SCHEMA IF NOT EXISTS {0} AUTHORIZATION {1}"
                   .format(settings['hex_schema'], settings['pg_user']))

    # create list of hex temp tables names and widths
    table_list = list()
    curr_width = settings['min_hex_width']
    while curr_width < settings['max_hex_width']:
        # Get table name friendly width
        curr_width_str = str(curr_width)
        curr_width_str = curr_width_str.replace(".", "_")
        curr_width_str = curr_width_str.replace("_0", "")
        curr_width_pid = "hex_pid_{0}".format(curr_width_str)

        table_list.append([curr_width_str, curr_width, curr_width_pid])

        curr_width *= float(settings['hex_multiplier'])

    # # step through each hex width and tag GNAF address points into temp tables
    # pg_cur.execute("DROP TABLE IF EXISTS {0}.address_hexes CASCADE".format("public",))
    # create_table_list = list()
    # create_table_list.append("CREATE TABLE {0}.address_hexes (gid serial NOT NULL,"
    #                          "gnaf_pid character varying(16) NOT NULL,"
    #                          "alias_principal character(1) NOT NULL,"
    #                          "year_added smallint NULL,"
    #                          "latitude numeric(10,8),"
    #                          "longitude numeric(11,8)"
    #                          .format("public",))
    # for table in table_list:
    #     create_table_list.append(", {0} character varying(20)".format(table[2]))
    # create_table_list.append(") WITH (OIDS=FALSE);ALTER TABLE {0}.address_hexes OWNER TO postgres"
    #                          .format("public",))
    # pg_cur.execute("".join(create_table_list))
    #
    # # create insert statement for multiprocessing
    # insert_field_list = list()
    # insert_field_list.append("(gnaf_pid, alias_principal, year_added, latitude, longitude")
    #
    # select_field_list = list()
    # select_field_list.append("SELECT pnts.gnaf_pid, pnts.alias_principal, "
    #                          "extract(year from tab.date_created)::smallint, pnts.latitude, pnts.longitude")
    #
    # for table in table_list:
    #     insert_field_list.append(", {0}".format(table[2]))
    #     select_field_list.append(", get_hex_pid({0}, pnts.longitude, pnts.latitude) ".format(table[1]))
    # insert_field_list.append(") ")
    #
    # insert_statement_list = list()
    # insert_statement_list.append("INSERT INTO {0}.address_hexes ".format(settings['hex_schema'],))
    # insert_statement_list.append("".join(insert_field_list))
    # insert_statement_list.append("".join(select_field_list))
    # insert_statement_list.append("FROM raw_gnaf.address_detail AS tab "
    #                              "INNER JOIN gnaf.address_principals AS pnts "
    #                              "ON tab.address_detail_pid = pnts.gnaf_pid")
    #
    # sql = "".join(insert_statement_list) + ";"
    # sql_list = split_sql_into_list(pg_cur, sql, settings['gnaf_schema'], "address_principals", "pnts", "gid", settings)
    # # print "\n".join(sql_list)
    #
    # multiprocess_list("sql", sql_list, settings)
    #
    # print "\t- Step 1 of 3 : gnaf hex tag table created : {0}".format(datetime.now() - start_time)

    # create count table with hex geoms
    sql_list = list()
    template_sql = open(os.path.join(settings['sql_dir'], "02-address-counts-by-hex-template.sql"), "r").read()
    threshhold = 1
    for table in table_list:
        table_name = "address_counts_{0}".format(table[0],)
        sql_list.append(template_sql.format(table_name, table[0], table[1], threshhold))
        threshhold *= 4

    # print "\n".join(sql_list)

    multiprocess_list("sql", sql_list, settings)


# takes a list of sql queries or command lines and runs them using multiprocessing
def multiprocess_list(mp_type, work_list, settings):
    pool = multiprocessing.Pool(processes=settings['max_concurrent_processes'])

    num_jobs = len(work_list)

    if mp_type == "sql":
        results = pool.imap_unordered(run_sql_multiprocessing, [[w, settings] for w in work_list])
    else:
        results = pool.imap_unordered(run_command_line, work_list)

    pool.close()
    pool.join()

    result_list = list(results)
    num_results = len(result_list)

    if num_jobs > num_results:
        print "\t- A MULTIPROCESSING PROCESS FAILED WITHOUT AN ERROR\nACTION: Check the record counts"

    for result in result_list:
        if result != "SUCCESS":
            print result


def run_sql_multiprocessing(args):
    the_sql = args[0]
    settings = args[1]
    pg_conn = psycopg2.connect(settings['pg_connect_string'])
    pg_conn.autocommit = True
    pg_cur = pg_conn.cursor()

    # # set raw gnaf database schema (it's needed for the primary and foreign key creation)
    # if settings['raw_gnaf_schema'] != "public":
    #     pg_cur.execute("SET search_path = {0}, public, pg_catalog".format(settings['raw_gnaf_schema'],))

    try:
        pg_cur.execute(the_sql)
        result = "SUCCESS"
    except psycopg2.Error, e:
        result = "SQL FAILED! : {0} : {1}".format(the_sql, e.message)

    pg_cur.close()
    pg_conn.close()

    return result


def run_command_line(cmd):
    # run the command line without any output (it'll still tell you if it fails)
    try:
        fnull = open(os.devnull, "w")
        subprocess.call(cmd, shell=True, stdout=fnull, stderr=subprocess.STDOUT)
        result = "SUCCESS"
    except Exception, e:
        result = "COMMAND FAILED! : {0} : {1}".format(cmd, e.message)

    return result


def open_sql_file(file_name, settings):
    sql = open(os.path.join(settings['sql_dir'], file_name), "r").read()
    return prep_sql(sql, settings)


# change schema names in an array of SQL script if schemas not the default
def prep_sql_list(sql_list, settings):
    output_list = []
    for sql in sql_list:
        output_list.append(prep_sql(sql, settings))
    return output_list


# change schema names in the SQL script if not the default
def prep_sql(sql, settings):
    # if settings['raw_gnaf_schema'] != "raw_gnaf":
    #     sql = sql.replace(" raw_gnaf.", " {0}.".format(settings['raw_gnaf_schema'],))
    if settings['gnaf_schema'] != "gnaf":
        sql = sql.replace(" gnaf.", " {0}.".format(settings['gnaf_schema'],))
    # if settings['raw_admin_bdys_schema'] != "raw_admin_bdys":
    #     sql = sql.replace(" raw_admin_bdys.", " {0}.".format(settings['raw_admin_bdys_schema'],))
    # if settings['admin_bdys_schema'] != "admin_bdys":
    #     sql = sql.replace(" admin_bdys.", " {0}.".format(settings['admin_bdys_schema'],))
    if settings['hex_schema'] != "hex":
        sql = sql.replace(" hex.", " {0}.".format(settings['hex_schema'],))
    return sql


def split_sql_into_list(pg_cur, the_sql, table_schema, table_name, table_alias, table_gid, settings):
    # get min max gid values from the table to split
    min_max_sql = "SELECT MIN({2}) AS min, MAX({2}) AS max FROM {0}.{1}".format(table_schema, table_name, table_gid)

    pg_cur.execute(min_max_sql)
    result = pg_cur.fetchone()

    min_pkey = int(result[0])
    max_pkey = int(result[1])
    diff = max_pkey - min_pkey

    # Number of records in each query
    rows_per_request = int(math.floor(float(diff) / float(settings['max_concurrent_processes']))) + 1

    # If less records than processes or rows per request, reduce both to allow for a minimum of 15 records each process
    if float(diff) / float(settings['max_concurrent_processes']) < 10.0:
        rows_per_request = 10
        processes = int(math.floor(float(diff) / 10.0)) + 1
        print "\t\t- running {0} processes (adjusted due to low row count in table to split)".format(processes)
    else:
        processes = settings['max_concurrent_processes']

    # create list of sql statements to run with multiprocessing
    sql_list = []
    start_pkey = min_pkey - 1

    for i in range(0, processes):
        end_pkey = start_pkey + rows_per_request

        where_clause = " WHERE {0}.{3} > {1} AND {0}.{3} <= {2}".format(table_alias, start_pkey, end_pkey, table_gid)

        if "WHERE " in the_sql:
            mp_sql = the_sql.replace(" WHERE ", where_clause + " AND ")
        elif "GROUP BY " in the_sql:
            mp_sql = the_sql.replace("GROUP BY ", where_clause + " GROUP BY ")
        elif "ORDER BY " in the_sql:
            mp_sql = the_sql.replace("ORDER BY ", where_clause + " ORDER BY ")
        else:
            if ";" in the_sql:
                mp_sql = the_sql.replace(";", where_clause + ";")
            else:
                mp_sql = the_sql + where_clause
                print "\t\t- NOTICE: no ; found at the end of the SQL statement"

        sql_list.append(mp_sql)
        start_pkey = end_pkey

    # print '\n'.join(sql_list)
    return sql_list


if __name__ == '__main__':
    main()
