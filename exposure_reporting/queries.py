# queries.py

from datetime import datetime, timedelta


def get_queries(campaign_name, start_date, end_date,
                start_string, end_string, impression_src,
                output_type, report_type, audience_file, pixel_id,
                profile_ids, targeted, report_num,
                s3_bucket, s3_prefix):
    """ Generates all queries and returns as one string
    Returns:
        Query string
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S") + "{}".format(report_num)
    run_date = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
    where_clause = get_where_clause(output_type)
    target_join = get_target_join_targeted(targeted)
    time_range = get_time_range_targeted(targeted, start_date, end_date)
    pixel_where = get_pixel_where_targeted(targeted, profile_ids)
    data_source_id_part = get_data_source_id_part(impression_src)
    start_dates, end_dates = create_weekly_splits(start_date, end_date)
    s3_path = "s3://{s3_bucket}/{s3_prefix}/{campaign_name}".format(s3_bucket=s3_bucket,
                                                                    s3_prefix=s3_prefix,
                                                                    campaign_name=campaign_name)
    if report_type == "Household":
        report_queries = get_household_queries(audience_file,timestamp, data_source_id_part, campaign_name,
                                               start_date, end_date, pixel_id, profile_ids, target_join,
                                               time_range, pixel_where, s3_path, run_date, where_clause, report_num)
    elif report_type == "Individual":
        report_queries = get_individual_queries(audience_file, timestamp, data_source_id_part, campaign_name,
                                                start_date, end_date, pixel_id, profile_ids, s3_path,
                                                run_date, where_clause, report_num)

    weekly_queries = get_weekly_queries(start_dates, end_dates, s3_path, timestamp)
    duplicate_queries = get_duplicate_queries(report_num, campaign_name, timestamp, s3_path)
    return report_queries + weekly_queries + duplicate_queries


def get_where_clause(output_type):
    """ Return query string based on the output_type passed in
    Returns:
        String where statement or empty string
    """
    if output_type == "All":
        return ""
    elif output_type == "Exposed":
        return "WHERE IMPRESSION_TIMESTAMP IS NOT NULL"
    elif output_type == "Unexposed":
        return "WHERE IMPRESSION_TIMESTAMP IS NULL"


def get_target_join_targeted(targeted):
    """ Adds an extra join to query if a targeted campaign
    Returns:
        String join statement or empty string
    """
    if targeted == "Y":
        return """
                INNER JOIN  core_shared.HHID_scores_history c
                ON          b.HHID = c.ID"""
    else:
        return ""


def get_time_range_targeted(targeted, start_date, end_date):
    """ Adds extra time range check on targeted campaign
    Returns:
        String AND statements or empty string
    """
    if targeted == "Y":
        return """
                AND         c.START_DATE <= '{END_DATE}'
                AND         c.END_DATE >= '{START_DATE}'
                """.format(START_DATE=start_date, END_DATE=end_date)
    else:
        return ""


def get_pixel_where_targeted(targeted, profile_ids):
    """ Adds extra profile ID statement on targeted campaign
    Returns:
        String ADD statement or empty string
    """
    if targeted == "Y":
        return """
                AND         c.PROFILE_ID IN ({PROFILE_ID})
                """.format(PROFILE_ID=profile_ids)
    else:
        return ""


def get_data_source_id_part(impression_src):
    """ Returns value based on impression source
    Returns:
        Integer 1 or 6
    """
    if impression_src == "Managed Services":
        return 1
    elif impression_src == "Pixel":
        return 6


def create_weekly_splits(start_date, end_date):
    """ Creates a list of start and end dates for the weekly counts table.
        NOTE: end date + 1 because of the exclusive '<' in
              'IMPRESSION TIMESTAMP < [END DATE]'
    Args:
        start_date: Start date
        end_date: End date string
    Returns:
        Two lists of start and end date strings
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d") + timedelta(days=1)

    start_dates = []
    end_dates = []
    while start < end:
        start_dates.append(start.strftime("%Y-%m-%d %H:%M:%S"))
        start = start + timedelta(days=7)
    end_dates = start_dates[1:]
    end_dates.append(end.strftime("%Y-%m-%d %H:%M:%S"))

    assert(len(start_dates) == len(end_dates))
    return start_dates, end_dates


def get_weekly_queries(start_dates, end_dates, s3_path, timestamp):
    """Returns weekly file queries"""
    queries = """
        DROP TABLE IF EXISTS WEEKLY_COUNTS_{TS};
        CREATE EXTERNAL TABLE WEEKLY_COUNTS_{TS}
            (WEEK_NUMBER         bigint,
             START_DATE          string,
             COUNT_IMPRESSIONS   bigint,
             AVG_IMPS_PER_CUSTID double)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        NULL DEFINED AS ''
        LOCATION '{S3_OUT_PATH}/WEEKLY/';

        INSERT OVERWRITE TABLE WEEKLY_COUNTS_{TS}
            SELECT     1,
                       '{START_STRING}',
                       COUNT(a.CUST_ID) COUNT_IMPRESSIONS,
                       (COUNT(a.CUST_ID)/COUNT(DISTINCT(a.CUST_ID))) AVG_IMPS_PER_CUSTID
            FROM       EXPOSURE_FILE_{TS} a
            WHERE      a.IMPRESSION_TIMESTAMP IS NOT NULL
            AND        a.IMPRESSION_TIMESTAMP >= '{START_DATE}'
            AND        a.IMPRESSION_TIMESTAMP <  '{END_DATE}';
    """.format(TS=timestamp, S3_OUT_PATH=s3_path,
               START_STRING=datetime.strptime(start_dates[0], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y"),
               START_DATE=start_dates[0], END_DATE=end_dates[0])

    for i in range(1, len(start_dates)):
        queries += """
            INSERT INTO TABLE WEEKLY_COUNTS_{TS}
                SELECT     {WEEK_NUM},
                           '{START_STRING}',
                           COUNT(a.CUST_ID) COUNT_IMPRESSIONS,
                           (COUNT(a.CUST_ID)/COUNT(DISTINCT(a.CUST_ID))) AVG_IMPS_PER_CUSTID
                FROM       EXPOSURE_FILE_{TS} a
                WHERE      a.IMPRESSION_TIMESTAMP IS NOT NULL
                AND        a.IMPRESSION_TIMESTAMP >= '{START_DATE}'
                AND        a.IMPRESSION_TIMESTAMP <  '{END_DATE}';
        """.format(WEEK_NUM=i+1, TS=timestamp,
                   START_STRING=datetime.strptime(start_dates[i], "%Y-%m-%d %H:%M:%S").strftime("%m/%d/%Y"),
                   START_DATE=start_dates[i], END_DATE=end_dates[i])
    return queries


def get_duplicate_queries(report_num, campaign_name, ts, s3_path):
    """Returns queries calculating duplicate lines in onramp file"""
    queries = ""
    # Duplication metrics to track -- this can be passed in
    metrics = [(2, '='), (3, '='), (4, '='), (5, '='), (10, '>=')]
    # Join table denotations
    chars = [chr(98+k) for k,v in enumerate(metrics)]
    # Dynamic query strings
    create = ['duplicate{metric} bigint'.format(metric=metric[0]) for metric in metrics]
    select = ['{}.dupes'.format(char) for char in chars]
    innerjoin = ['inner join duplicate{metric}_{ts} {table}\n\t\ton b.join_var = {table}.join_var'.format(metric=metrics[i][0], ts=ts, table=chars[i]) for i in range(1, len(metrics))]

    for metric in metrics:
        queries += """
        drop table if exists duplicate{metric}_{ts};
        create table duplicate{metric}_{ts} as
            select '{report_number}' report_number, count(distinct(s.cust_id)) dupes, 'a' join_var
            from exposure_file_{ts} s
            inner join (
                select cust_id, impression_timestamp, creative_id, count(*) as qty
                from exposure_file_{ts}
                group by cust_id, impression_timestamp, creative_id
                having count(*) {condition} {metric}
                ) t
            on s.cust_id = t.cust_id
            and s.impression_timestamp = t.impression_timestamp
            and s.creative_id = t.creative_id;
        """.format(metric=metric[0],
                   condition=metric[1],
                   ts=ts, report_number=report_num)

    queries += """
        drop table if exists duplicates_{ts};
        create external table duplicates_{ts}
            (report_number string,
             campaign_name string,
             {create},
             percentage    float)
        row format delimited fields terminated by ','
        null defined as ''
        location '{s3_out_path}/DUPLICATES/';

        insert overwrite table duplicates_{ts}
            select     '{report_num}',
                       '{campaign_name}',
                       {select},
                       (({add})/a.exposed_unique_custid) * 100
            from       summary_stats_{ts} a
            inner join duplicate{metric}_{ts} b
            on         a.report_number = b.report_number
                       {innerjoin};
        """.format(ts=ts, report_num=report_num, campaign_name=campaign_name,
                   s3_out_path=s3_path, metric=metrics[0][0],
                   create=',\n\t\t'.join(create),
                   select=',\n\t\t'.join(select),
                   add=' + '.join(select),
                   innerjoin='\n\t\t'.join(innerjoin))
    return queries


def get_household_queries(audience_file, timestamp, data_source_id_part,
                          campaign_name, start_date, end_date, pixel_id,
                          profile_ids, target_join, time_range, pixel_where,
                          s3_path, run_date, where_clause, report_num):
    queries = """
        set hive.map.aggr=false;
        set hive.exec.compress.intermediate=true;
        set hive.on.master=true;
        set mapreduce.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
        set mapreduce.output.compress=true;
        set mapreduce.compress.map.output=true; 
        set mapreduce.task.timeout=7500000;
        set mapreduce.map.java.opts=-Xmx3428m;
        set mapreduce.map.memory.mb=4288;
        set mapreduce.input.fileinputformat.split.minsize=768000000;
        set mapreduce.input.fileinputformat.split.maxsize=768000000;

        DROP TABLE IF EXISTS AUDIENCE_TEMP_{TS};
        CREATE EXTERNAL TABLE AUDIENCE_TEMP_{TS}
            (CUST_ID     string,
             HHID        string,
             ATTRIBUTE_1 string,
             ATTRIBUTE_2 string,
             ATTRIBUTE_3 string,
             ATTRIBUTE_4 string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
        NULL DEFINED AS ''
        LOCATION 's3://dlx-prod-audience-operations-pii/FlatFileOutput/{AUDIENCE_FILE}/AttributeFileInput/';

        DROP TABLE IF EXISTS AUDIENCE_{TS};
        CREATE TABLE AUDIENCE_{TS}
            (CUST_ID     string,
             HHID        string,
             ATTRIBUTE_1 string,
             ATTRIBUTE_2 string,
             ATTRIBUTE_3 string,
             ATTRIBUTE_4 string);

        INSERT OVERWRITE TABLE AUDIENCE_{TS}
            SELECT      CUST_ID,
                        HHID,
                        ATTRIBUTE_1,
                        ATTRIBUTE_2,
                        ATTRIBUTE_3,
                        ATTRIBUTE_4
            FROM        AUDIENCE_TEMP_{TS}
            GROUP BY    CUST_ID,
                        HHID,
                        ATTRIBUTE_1,
                        ATTRIBUTE_2,
                        ATTRIBUTE_3,
                        ATTRIBUTE_4;
        DROP TABLE IF EXISTS IMPSCOUNT_TABLE_{TS};
        CREATE EXTERNAL TABLE IMPSCOUNT_TABLE_{TS}
                (IMPS_COUNT string);


        INSERT OVERWRITE TABLE IMPSCOUNT_TABLE_{TS}
                SELECT  COUNT(*)
                FROM    core_digital.unified_impression
                WHERE   DATA_DATE >= '{START_DATE}'
                AND     DATA_DATE <= '{END_DATE}'
                AND     PIXEL_ID IN ({PIXEL_ID})
                AND     DATA_SOURCE_ID_PART = {DATA_SOURCE_ID_PART}
                AND     SOURCE = "save";
        
        DROP TABLE IF EXISTS STEP1_TABLE_{TS};
        CREATE EXTERNAL TABLE STEP1_TABLE_{TS}
                (HHID                 string,
                 PIXEL_ID             string,
                 IMPRESSION_TIMESTAMP string,
                 CREATIVE_ID          string,
                 PLACEMENT_ID         string);

        INSERT OVERWRITE TABLE STEP1_TABLE_{TS}
                SELECT      b.HHID,
                            a.PIXEL_ID,
                            from_unixtime(unix_timestamp(a.EVENT_TIMESTAMP, "yyyy-MM-dd'T'HH:mm:ss.S'Z'")) IMPRESSION_TIMESTAMP,
                            a.dlx_chpcr,
                            a.dlx_chpth
                FROM        core_digital.unified_impression a
                INNER JOIN  core_digital.best_matched_cookies_history b
                ON          a.NA_GUID_ID = b.GUID
                {TARGET_JOIN}
                WHERE       a.DATA_DATE >= '{START_DATE}' 
                AND         a.DATA_DATE <= '{END_DATE}'
                {TIME_RANGE}
                AND         a.PIXEL_ID IN ({PIXEL_ID})
                {PIXEL_WHERE}
                AND         a.DATA_SOURCE_ID_PART = {DATA_SOURCE_ID_PART}
                AND         a.SOURCE = "save";
        
        DROP TABLE IF EXISTS STEP2_TABLE_{TS};
        CREATE EXTERNAL TABLE STEP2_TABLE_{TS}
                (MAP_HHID             string,
                 PIXEL_ID             string,
                 IMPRESSION_TIMESTAMP string,
                 CREATIVE_ID          string,
                 PLACEMENT_ID         string,
                 CUST_ID              string,
                 HHID                 string,
                 ATTRIBUTE_1          string,
                 ATTRIBUTE_2          string,
                 ATTRIBUTE_3          string,
                 ATTRIBUTE_4          string);

        INSERT OVERWRITE TABLE STEP2_TABLE_{TS}
                SELECT    b.HHID,
                          b.PIXEL_ID,
                          b.IMPRESSION_TIMESTAMP,
                          b.CREATIVE_ID,
                          b.PLACEMENT_ID,
                          a.CUST_ID,
                          a.HHID,
                          a.ATTRIBUTE_1,
                          a.ATTRIBUTE_2,
                          a.ATTRIBUTE_3,
                          a.ATTRIBUTE_4
                FROM      AUDIENCE_{TS} a
                LEFT JOIN STEP1_TABLE_{TS} b
                ON        a.HHID=b.HHID;


        DROP TABLE IF EXISTS EXPOSURE_FILE_{TS};
        CREATE EXTERNAL TABLE EXPOSURE_FILE_{TS}
                (CUST_ID              string,
                 IMPRESSION_TIMESTAMP string,
                 ATTRIBUTE_1          string,
                 ATTRIBUTE_2          string,
                 ATTRIBUTE_3          string,
                 ATTRIBUTE_4          string,
                 CREATIVE_ID          string,
                 PLACEMENT_ID         string);

        INSERT OVERWRITE TABLE EXPOSURE_FILE_{TS}
                SELECT CUST_ID,
                       IMPRESSION_TIMESTAMP,
                       ATTRIBUTE_1,
                       ATTRIBUTE_2,
                       ATTRIBUTE_3,
                       ATTRIBUTE_4,
                       CREATIVE_ID,
                       PLACEMENT_ID
                FROM   STEP2_TABLE_{TS};


        DROP TABLE IF EXISTS ROW_COUNT_{TS};
        DROP TABLE IF EXISTS TOTAL_IMPS_{TS};
        DROP TABLE IF EXISTS INSEGMENT_IMPS_{TS};
        DROP TABLE IF EXISTS IMPS_TO_HH_{TS};
        DROP TABLE IF EXISTS IMPS_IN_FILE_{TS};
        DROP TABLE IF EXISTS CUSTID_IN_TABLE_{TS};
        DROP TABLE IF EXISTS EXP_UNIQUE_CUSTID_{TS};
        DROP TABLE IF EXISTS EXP_START_DATE_{TS};
        DROP TABLE IF EXISTS EXP_END_DATE_{TS};
        DROP TABLE IF EXISTS SUMMARY_STATS_{TS};
        
        CREATE TABLE ROW_COUNT_{TS} (JOIN_VAR VARCHAR(1), ROWS_IN_EXPOSURE_FILE BIGINT);
        INSERT INTO TABLE ROW_COUNT_{TS} SELECT 'A', COUNT(*) FROM EXPOSURE_FILE_{TS};

        CREATE TABLE TOTAL_IMPS_{TS} (JOIN_VAR VARCHAR(1), TOTAL_IMPRESSIONS_SERVED BIGINT);
        INSERT INTO TABLE TOTAL_IMPS_{TS} SELECT 'A', IMPS_COUNT FROM IMPSCOUNT_TABLE_{TS};

        CREATE TABLE INSEGMENT_IMPS_{TS} (JOIN_VAR VARCHAR(1), INSEGMENT_IMPRESSIONS BIGINT);
        INSERT INTO TABLE INSEGMENT_IMPS_{TS} SELECT 'A', COUNT(*) FROM STEP1_TABLE_{TS};

        CREATE TABLE IMPS_TO_HH_{TS} (JOIN_VAR VARCHAR(1), IMPRESSIONS_MATCHED_TO_DLX_HH BIGINT);
        INSERT INTO TABLE IMPS_TO_HH_{TS} SELECT 'A', COUNT(*) FROM STEP1_TABLE_{TS} WHERE HHID != "0";

        CREATE TABLE EXPOSED_UNIQUE_HH_{TS} (JOIN_VAR string, EXPOSED_UNIQUE_HH bigint);
        INSERT INTO TABLE EXPOSED_UNIQUE_HH_{TS} SELECT 'A', COUNT(DISTINCT(HHID)) FROM STEP1_TABLE_{TS} WHERE HHID != "0";

        CREATE TABLE IMPS_IN_FILE_{TS} (JOIN_VAR VARCHAR(1), IMPRESSIONS_IN_FILE BIGINT);
        INSERT INTO TABLE IMPS_IN_FILE_{TS} SELECT 'A', COUNT(CASE WHEN IMPRESSION_TIMESTAMP IS NOT NULL THEN CUST_ID END) FROM EXPOSURE_FILE_{TS};

        CREATE TABLE CUSTID_IN_TABLE_{TS} (JOIN_VAR VARCHAR(1), CUSTOMER_IDS_IN_FILE BIGINT);
        INSERT INTO TABLE CUSTID_IN_TABLE_{TS} SELECT 'A', COUNT(DISTINCT CUST_ID) FROM EXPOSURE_FILE_{TS};

        CREATE TABLE EXP_UNIQUE_CUSTID_{TS} (JOIN_VAR string, EXPOSED_UNIQUE_CUSTID bigint);
        INSERT INTO TABLE EXP_UNIQUE_CUSTID_{TS} SELECT 'A', COUNT(DISTINCT CASE WHEN IMPRESSION_TIMESTAMP IS NOT NULL THEN CUST_ID END) FROM EXPOSURE_FILE_{TS};

        CREATE TABLE EXP_UNIQUE_HH_IN_FILE_{TS} (JOIN_VAR string, EXPOSED_UNIQUE_HH_IN_FILE bigint);
        INSERT INTO TABLE EXP_UNIQUE_HH_IN_FILE_{TS} SELECT 'A', COUNT(DISTINCT(HHID)) FROM STEP2_TABLE_{TS} WHERE IMPRESSION_TIMESTAMP IS NOT NULL AND HHID != "0";

        CREATE TABLE CREATIVE_COUNT_{TS} (JOIN_VAR string, CREATIVE_COUNT bigint);
        INSERT INTO TABLE CREATIVE_COUNT_{TS} SELECT 'A', COUNT(CREATIVE_ID) FROM EXPOSURE_FILE_{TS} WHERE CREATIVE_ID IS NOT NULL AND CREATIVE_ID <> '' AND LENGTH(CREATIVE_ID) > 0;

        CREATE TABLE PLACEMENT_COUNT_{TS} (JOIN_VAR string, PLACEMENT_COUNT bigint);
        INSERT INTO TABLE PLACEMENT_COUNT_{TS} SELECT 'A', COUNT(PLACEMENT_ID) FROM EXPOSURE_FILE_{TS} WHERE PLACEMENT_ID IS NOT NULL AND PLACEMENT_ID <> '' AND LENGTH(PLACEMENT_ID) > 0;

        CREATE TABLE EXP_START_DATE_{TS} (JOIN_VAR VARCHAR(1), EXPOSURE_START_DATE VARCHAR(100));
        INSERT INTO TABLE EXP_START_DATE_{TS} SELECT 'A', MIN(IMPRESSION_TIMESTAMP) FROM EXPOSURE_FILE_{TS};

        CREATE TABLE EXP_END_DATE_{TS} (JOIN_VAR VARCHAR(1), EXPOSURE_END_DATE VARCHAR(100));
        INSERT INTO TABLE EXP_END_DATE_{TS} SELECT 'A', MAX(IMPRESSION_TIMESTAMP) FROM EXPOSURE_FILE_{TS};

        CREATE EXTERNAL TABLE SUMMARY_STATS_{TS}
            (REPORT_NUMBER                 string,
             CAMPAIGN_NAME                 string,
             RUN_DATE                      string,
             ROWS_IN_EXPOSURE_FILE         bigint,
             TOTAL_IMPRESSIONS_SERVED      bigint,
             INSEGMENT_IMPRESSIONS         bigint,
             IMPRESSIONS_MATCHED_TO_DLX_HH bigint,
             EXPOSED_UNIQUE_HH             bigint,
             IMPRESSIONS_IN_FILE           bigint,
             CUSTOMER_IDS_IN_FILE          bigint,
             EXPOSED_UNIQUE_CUSTID         bigint,
             EXPOSED_UNIQUE_HH_IN_FILE     bigint,
             CREATIVE_COUNT                bigint,
             PLACEMENT_COUNT               bigint,
             EXPOSURE_START_DATE           string,
             EXPOSURE_END_DATE             string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        NULL DEFINED AS ''
        LOCATION '{S3_OUT_PATH}/SUMMARY/';

        INSERT OVERWRITE TABLE SUMMARY_STATS_{TS}
            SELECT      '{REPORT_NUMBER}',
                        '{CAMPAIGN_NAME}',
                        '{RUN_DATE}',
                        a.ROWS_IN_EXPOSURE_FILE,
                        b.TOTAL_IMPRESSIONS_SERVED,
                        c.INSEGMENT_IMPRESSIONS,
                        d.IMPRESSIONS_MATCHED_TO_DLX_HH,
                        e.EXPOSED_UNIQUE_HH,
                        f.IMPRESSIONS_IN_FILE,
                        g.CUSTOMER_IDS_IN_FILE,
                        h.EXPOSED_UNIQUE_CUSTID,
                        i.EXPOSED_UNIQUE_HH_IN_FILE,
                        j.CREATIVE_COUNT,
                        k.PLACEMENT_COUNT,
                        l.EXPOSURE_START_DATE,
                        m.EXPOSURE_END_DATE
            FROM        ROW_COUNT_{TS} a
            INNER JOIN  TOTAL_IMPS_{TS} b
            ON          a.JOIN_VAR = b.JOIN_VAR
            INNER JOIN  INSEGMENT_IMPS_{TS} c
            ON          b.JOIN_VAR = c.JOIN_VAR
            INNER JOIN  IMPS_TO_HH_{TS} d
            ON          c.JOIN_VAR = d.JOIN_VAR
            INNER JOIN  EXPOSED_UNIQUE_HH_{TS} e
            ON          d.JOIN_VAR = e.JOIN_VAR
            INNER JOIN  IMPS_IN_FILE_{TS} f
            ON          e.JOIN_VAR = f.JOIN_VAR
            INNER JOIN  CUSTID_IN_TABLE_{TS} g
            ON          f.JOIN_VAR = g.JOIN_VAR
            INNER JOIN  EXP_UNIQUE_CUSTID_{TS} h
            ON          g.JOIN_VAR = h.JOIN_VAR
            INNER JOIN  EXP_UNIQUE_HH_IN_FILE_{TS} i
            ON          h.JOIN_VAR = i.JOIN_VAR
            INNER JOIN  CREATIVE_COUNT_{TS} j
            ON          i.JOIN_VAR = j.JOIN_VAR
            INNER JOIN  PLACEMENT_COUNT_{TS} k
            ON          j.JOIN_VAR = k.JOIN_VAR
            INNER JOIN  EXP_START_DATE_{TS} l
            ON          k.JOIN_VAR = l.JOIN_VAR
            INNER JOIN  EXP_END_DATE_{TS} m
            ON          l.JOIN_VAR = m.JOIN_VAR;

        set hive.optimize.insert.dest.volume=true;
        set hive.map.aggr=false;
        set mapred.reduce.tasks=1000;
        INSERT OVERWRITE DIRECTORY
        "{S3_OUT_PATH}/EXPOSURE/"
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
        NULL DEFINED AS ''
        SELECT *
        FROM EXPOSURE_FILE_{TS}
        {WHERE_CLAUSE};


        DROP TABLE IF EXISTS AUDIENCE_TEMP_{TS};
        DROP TABLE IF EXISTS AUDIENCE_{TS};
        DROP TABLE IF EXISTS IMPSCOUNT_TABLE_{TS};
        DROP TABLE IF EXISTS STEP1_TABLE_{TS};
        DROP TABLE IF EXISTS ROW_COUNT_{TS};
        DROP TABLE IF EXISTS TOTAL_IMPS_{TS};
        DROP TABLE IF EXISTS INSEGMENT_IMPS_{TS};
        DROP TABLE IF EXISTS IMPS_TO_HH_{TS};
        DROP TABLE IF EXISTS IMPS_IN_FILE_{TS};
        DROP TABLE IF EXISTS CUSTID_IN_TABLE_{TS};
        DROP TABLE IF EXISTS EXP_UNIQUE_CUSTID_{TS};
        DROP TABLE IF EXISTS EXP_START_DATE_{TS};
        DROP TABLE IF EXISTS EXP_END_DATE_{TS};
        """.format(AUDIENCE_FILE=audience_file,
                   TS=timestamp,
                   DATA_SOURCE_ID_PART=data_source_id_part,
                   CAMPAIGN_NAME=campaign_name,
                   START_DATE=start_date,
                   END_DATE=end_date,
                   PIXEL_ID=pixel_id,
                   PROFILE_ID=profile_ids,
                   TARGET_JOIN=target_join,
                   TIME_RANGE=time_range,
                   PIXEL_WHERE=pixel_where,
                   S3_OUT_PATH=s3_path,
                   RUN_DATE=run_date,
                   WHERE_CLAUSE=where_clause,
                   REPORT_NUMBER=report_num)
    return queries


def get_individual_queries(audience_file, timestamp, data_source_id_part,
                           campaign_name, start_date, end_date,
                           pixel_id, profile_ids, s3_path,
                           run_date, where_clause, report_num):
    queries = """
        set hive.map.aggr=false;
        set hive.exec.compress.intermediate=true; 
        set hive.on.master=true;
        set mapreduce.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec; 
        set mapreduce.output.compress=true;
        set mapreduce.compress.map.output=true; 
        set mapreduce.task.timeout=7500000;
        set mapreduce.map.java.opts=-Xmx3428m;
        set mapreduce.map.memory.mb=4288;
        set mapreduce.input.fileinputformat.split.minsize=768000000;
        set mapreduce.input.fileinputformat.split.maxsize=768000000;

        DROP TABLE IF EXISTS MAPPING_TABLE_TEMP_{TS};
        CREATE EXTERNAL TABLE MAPPING_TABLE_TEMP_{TS}
            (CUST_ID     string,
             HHID        string,
             GROUP_ID    string,
             ATTRIBUTE_1 string,
             ATTRIBUTE_2 string,
             ATTRIBUTE_3 string,
             ATTRIBUTE_4 string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
        NULL DEFINED AS ''
        LOCATION 's3://dlx-prod-audience-operations-pii/FlatFileOutput/{AUDIENCE_FILE}/IndividualAttributeFileInput/';

        DROP TABLE IF EXISTS MAPPING_TABLE_{TS};
        CREATE TABLE MAPPING_TABLE_{TS}
            (CUST_ID     string,
             HHID        string,
             GROUP_ID    string,
             ATTRIBUTE_1 string,
             ATTRIBUTE_2 string,
             ATTRIBUTE_3 string,
             ATTRIBUTE_4 string);

        INSERT INTO TABLE MAPPING_TABLE_{TS}
            SELECT    CUST_ID,
                      HHID,
                      GROUP_ID,
                      ATTRIBUTE_1,
                      ATTRIBUTE_2,
                      ATTRIBUTE_3,
                      ATTRIBUTE_4
            FROM      MAPPING_TABLE_TEMP_{TS}
            GROUP BY  CUST_ID, HHID, GROUP_ID, ATTRIBUTE_1, ATTRIBUTE_2, ATTRIBUTE_3, ATTRIBUTE_4;


        DROP TABLE IF EXISTS IMPSCOUNT_TABLE_{TS};
        CREATE EXTERNAL TABLE IMPSCOUNT_TABLE_{TS}
            (IMPS_COUNT string);

        INSERT OVERWRITE TABLE IMPSCOUNT_TABLE_{TS}
            SELECT COUNT(*) IMPS_COUNT
            FROM   core_digital.unified_impression
            WHERE  PIXEL_ID IN ({PIXEL_ID})
            AND    DATA_DATE >= '{START_DATE}'
            AND    DATA_DATE <= '{END_DATE}'
            AND    data_source_id_part = {DATA_SOURCE_ID_PART}
            AND    source='save';

        DROP TABLE IF EXISTS STEP1_TABLE_{TS};
        CREATE EXTERNAL TABLE STEP1_TABLE_{TS}
            (GROUP_ID             string,
             PIXEL_ID             string,
             IMPRESSION_TIMESTAMP string,
             CREATIVE_ID          string,
             PLACEMENT_ID         string);

        INSERT OVERWRITE TABLE STEP1_TABLE_{TS}
        SELECT     c.GROUP_ID,
                   a.PIXEL_ID,
                   from_unixtime(unix_timestamp(a.event_timestamp,"yyyy-MM-dd'T'HH:mm:ss.S'Z'")) IMPRESSION_TIMESTAMP,
                   a.dlx_chpcr,
                   a.dlx_chpth
        FROM       core_digital.unified_impression a
        INNER JOIN core_digital.best_matched_cookies_history b
        ON         a.NA_GUID_ID = b.GUID
        INNER JOIN core_shared.individual_consolidated c
        ON         b.INDIVIDUAL_ID = c.INDIVIDUAL_ID
        WHERE      a.PIXEL_ID IN ({PIXEL_ID})
        AND        a.DATA_DATE >= '{START_DATE}'
        AND        a.DATA_DATE <= '{END_DATE}'
        AND        a.data_source_id_part = {DATA_SOURCE_ID_PART}
        AND        a.source='save';

        DROP TABLE IF EXISTS STEP2_TABLE_{TS};
        CREATE EXTERNAL TABLE STEP2_TABLE_{TS}
            (MAP_IND              string,
             PIXEL_ID             string,
             IMPRESSION_TIMESTAMP string,
             CREATIVE_ID          string,
             PLACEMENT_ID         string,
             CUST_ID              string,
             HHID                 string,
             ATTRIBUTE_1          string,
             ATTRIBUTE_2          string,
             ATTRIBUTE_3          string,
             ATTRIBUTE_4          string);

        INSERT OVERWRITE TABLE STEP2_TABLE_{TS}
            SELECT    b.GROUP_ID,
                      b.PIXEL_ID,
                      b.IMPRESSION_TIMESTAMP,
                      b.CREATIVE_ID,
                      b.PLACEMENT_ID,
                      a.CUST_ID,
                      a.HHID,
                      a.ATTRIBUTE_1,
                      a.ATTRIBUTE_2,
                      a.ATTRIBUTE_3,
                      a.ATTRIBUTE_4
            FROM      MAPPING_TABLE_{TS} a
            LEFT JOIN STEP1_TABLE_{TS} b
            ON        a.GROUP_ID = b.GROUP_ID;


        DROP TABLE IF EXISTS EXPOSURE_FILE_{TS};
        CREATE EXTERNAL TABLE EXPOSURE_FILE_{TS}
                  (CUST_ID              string,
                   IMPRESSION_TIMESTAMP string,
                   ATTRIBUTE_1          string,
                   ATTRIBUTE_2          string,
                   ATTRIBUTE_3          string,
                   ATTRIBUTE_4          string,
                   CREATIVE_ID          string,
                   PLACEMENT_ID         string);

        INSERT OVERWRITE TABLE EXPOSURE_FILE_{TS}
            SELECT CUST_ID,
                   IMPRESSION_TIMESTAMP,
                   ATTRIBUTE_1,
                   ATTRIBUTE_2,
                   ATTRIBUTE_3,
                   ATTRIBUTE_4,
                   CREATIVE_ID,
                   PLACEMENT_ID
            FROM   STEP2_TABLE_{TS};


        DROP TABLE IF EXISTS QC_STEP1_{TS};
        CREATE TABLE QC_STEP1_{TS}
            (JOIN_VAR              string,
             ROWS_IN_EXPOSURE_FILE int,
             CUSTOMER_IDS_IN_FILE  int);

        INSERT INTO TABLE QC_STEP1_{TS}
            SELECT 'A' JOIN_VAR,
                   COUNT(*) ROWS_IN_EXPOSURE_FILE,
                   COUNT(DISTINCT CUST_ID) CUSTOMER_IDS_IN_FILE
            FROM   EXPOSURE_FILE_{TS};


        DROP TABLE IF EXISTS QC_STEP2_{TS};
        CREATE TABLE QC_STEP2_{TS}
           (JOIN_VAR                    string,
            IMPRESSIONS_IN_FILE         int,
            EXPOSED_UNIQUE_CUSTOMER_IDS int,
            EXPOSURE_START_DATE         string,
            EXPOSURE_END_DATE           string);

        INSERT INTO TABLE QC_STEP2_{TS}
            SELECT 'A' JOIN_VAR,
                   COUNT(*) IMPRESSIONS_IN_FILE,
                   COUNT(DISTINCT CUST_ID) EXPOSED_UNIQUE_CUSTOMER_IDS,
                   MIN(IMPRESSION_TIMESTAMP) EXPOSURE_START_DATE,
                   MAX(IMPRESSION_TIMESTAMP) EXPOSURE_END_DATE
            FROM   EXPOSURE_FILE_{TS}
            WHERE  IMPRESSION_TIMESTAMP IS NOT NULL
            AND    IMPRESSION_TIMESTAMP NOT LIKE '\n'
            AND    IMPRESSION_TIMESTAMP NOT LIKE '';


        DROP TABLE IF EXISTS QC_STEP3_{TS};
        CREATE TABLE QC_STEP3_{TS}
            (JOIN_VAR              string,
             INSEGMENT_IMPRESSIONS int);

        INSERT INTO TABLE QC_STEP3_{TS}
            SELECT 'A' JOIN_VAR,
                   COUNT(*) INSEGMENT_IMPRESSIONS
            FROM   STEP1_TABLE_{TS};


        DROP TABLE IF EXISTS QC_STEP4_{TS};
        CREATE TABLE QC_STEP4_{TS}
          (JOIN_VAR string,
           TOTAL_IMPRESSIONS_MATCHED int);

        INSERT INTO TABLE QC_STEP4_{TS}
            SELECT 'A' JOIN_VAR,
                   COUNT(*) TOTAL_IMPRESSIONS_MATCHED
            FROM   STEP1_TABLE_{TS}
            WHERE  GROUP_ID != '0'
            AND    GROUP_ID != '-1'
            AND    GROUP_ID IS NOT NULL;


        DROP TABLE IF EXISTS QC_STEP5_{TS};
        CREATE TABLE QC_STEP5_{TS}
          (JOIN_VAR                 string,
           TOTAL_IMPRESSIONS_SERVED int);

        INSERT INTO TABLE QC_STEP5_{TS}
        SELECT 'A' JOIN_VAR,
               IMPS_COUNT
        FROM   IMPSCOUNT_TABLE_{TS};


        DROP TABLE IF EXISTS QC_STEP6_{TS};
        CREATE TABLE QC_STEP6_{TS} (JOIN_VAR string, EXPOSED_UNIQUE_HH bigint);
        INSERT INTO TABLE QC_STEP6_{TS} SELECT 'A', COUNT(DISTINCT(GROUP_ID)) FROM STEP1_TABLE_{TS} WHERE GROUP_ID != "0";


        DROP TABLE IF EXISTS QC_STEP7_{TS};
        CREATE TABLE QC_STEP7_{TS}
          (JOIN_VAR        string,
           CREATIVE_COUNT  bigint);

        INSERT INTO TABLE QC_STEP7_{TS}
            SELECT 'A',
                   COUNT(CREATIVE_ID)
            FROM   EXPOSURE_FILE_{TS}
            WHERE  CREATIVE_ID IS NOT NULL
            AND    CREATIVE_ID <> ''
            AND    LENGTH(CREATIVE_ID) > 0;


        DROP TABLE IF EXISTS QC_STEP8_{TS};
        CREATE TABLE QC_STEP8_{TS}
          (JOIN_VAR        string,
           PLACEMENT_COUNT  bigint);

        INSERT INTO TABLE QC_STEP8_{TS}
            SELECT 'A',
                   COUNT(PLACEMENT_ID)
            FROM   EXPOSURE_FILE_{TS}
            WHERE  PLACEMENT_ID IS NOT NULL
            AND    PLACEMENT_ID <> ''
            AND    LENGTH(PLACEMENT_ID) > 0;

        CREATE TABLE QC_STEP9_{TS} (JOIN_VAR string, EXPOSED_UNIQUE_HH_IN_FILE bigint);
        INSERT INTO TABLE QC_STEP9_{TS} SELECT 'A', COUNT(DISTINCT(MAP_IND)) FROM STEP2_TABLE_{TS} WHERE IMPRESSION_TIMESTAMP IS NOT NULL AND MAP_IND != "0";


        CREATE EXTERNAL TABLE SUMMARY_STATS_{TS}
            (REPORT_NUMBER                 string,
             CAMPAIGN_NAME                 string,
             RUN_DATE                      string,
             ROWS_IN_EXPOSURE_FILE         bigint,
             TOTAL_IMPRESSIONS_SERVED      bigint,
             INSEGMENT_IMPRESSIONS         bigint,
             IMPRESSIONS_MATCHED_TO_DLX_HH bigint,
             EXPOSED_UNIQUE_HH             bigint,
             IMPRESSIONS_IN_FILE           bigint,
             CUSTOMER_IDS_IN_FILE          bigint,
             EXPOSED_UNIQUE_CUSTID         bigint,
             EXPOSED_UNIQUE_HH_IN_FILE     bigint,
             CREATIVE_COUNT                bigint,
             PLACEMENT_COUNT               bigint,
             EXPOSURE_START_DATE           string,
             EXPOSURE_END_DATE             string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        NULL DEFINED AS ''
        LOCATION '{S3_PATH}/SUMMARY/';

        INSERT OVERWRITE TABLE SUMMARY_STATS_{TS}
            SELECT '{REPORT_NUMBER}' REPORT_NUMBER,
                   '{CAMPAIGN_NAME}' CAMPAIGN_NAME, 
                   '{TODAY_STAMP}' RUN_DATE,
                   a.ROWS_IN_EXPOSURE_FILE,
                   e.TOTAL_IMPRESSIONS_SERVED,
                   c.INSEGMENT_IMPRESSIONS,
                   d.TOTAL_IMPRESSIONS_MATCHED,
                   f.EXPOSED_UNIQUE_HH,
                   b.IMPRESSIONS_IN_FILE,
                   a.CUSTOMER_IDS_IN_FILE,
                   b.EXPOSED_UNIQUE_CUSTOMER_IDS,
                   i.EXPOSED_UNIQUE_HH_IN_FILE,
                   g.CREATIVE_COUNT,
                   h.PLACEMENT_COUNT,
                   b.EXPOSURE_START_DATE,
                   b.EXPOSURE_END_DATE
            FROM QC_STEP1_{TS} a
            INNER JOIN QC_STEP2_{TS} b
            ON LOWER(a.JOIN_VAR) = LOWER(b.JOIN_VAR)
            INNER JOIN QC_STEP3_{TS} c
            ON LOWER(a.JOIN_VAR) = LOWER(c.JOIN_VAR)
            INNER JOIN QC_STEP4_{TS} d
            ON LOWER(a.JOIN_VAR) = LOWER(d.JOIN_VAR)
            INNER JOIN QC_STEP5_{TS} e
            ON LOWER(a.JOIN_VAR) = LOWER(e.JOIN_VAR)
            INNER JOIN QC_STEP6_{TS} f
            ON LOWER(a.JOIN_VAR) = LOWER(f.JOIN_VAR)
            INNER JOIN QC_STEP7_{TS} g
            ON LOWER(a.JOIN_VAR) = LOWER(g.JOIN_VAR)
            INNER JOIN QC_STEP8_{TS} h
            ON LOWER(a.JOIN_VAR) = LOWER(h.JOIN_VAR)
            INNER JOIN QC_STEP9_{TS} i
            ON LOWER(a.JOIN_VAR) = LOWER(i.JOIN_VAR);

        set hive.optimize.insert.dest.volume=true;
        set hive.map.aggr=false;
        set mapred.reduce.tasks=200;
        INSERT OVERWRITE DIRECTORY "{S3_PATH}/EXPOSURE/"
        ROW FORMAT DELIMITED fields terminated by '|'
        NULL DEFINED AS ''
            SELECT * 
            FROM EXPOSURE_FILE_{TS} 
            {WHERE_CLAUSE};

        DROP TABLE IF EXISTS MAPPING_TABLE_TEMP_{TS};
        DROP TABLE IF EXISTS MAPPING_TABLE_{TS};
        DROP TABLE IF EXISTS IMPSCOUNT_TABLE_{TS};
        DROP TABLE IF EXISTS STEP1_TABLE_{TS};
        DROP TABLE IF EXISTS QC_STEP1_{TS};
        DROP TABLE IF EXISTS QC_STEP2_{TS};
        DROP TABLE IF EXISTS QC_STEP3_{TS};
        DROP TABLE IF EXISTS QC_STEP4_{TS};
        DROP TABLE IF EXISTS QC_STEP5_{TS};
        """.format(AUDIENCE_FILE=audience_file,
                   TS=timestamp,
                   DATA_SOURCE_ID_PART=data_source_id_part,
                   CAMPAIGN_NAME=campaign_name,
                   START_DATE=start_date,
                   END_DATE=end_date,
                   PIXEL_ID=pixel_id,
                   PROFILE_ID=profile_ids,
                   S3_PATH=s3_path,
                   TODAY_STAMP=run_date,
                   WHERE_CLAUSE=where_clause,
                   REPORT_NUMBER=report_num)
    return queries