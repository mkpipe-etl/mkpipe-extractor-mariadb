import os
import datetime
from urllib.parse import quote_plus
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F

from mkpipe.config import load_config
from mkpipe.utils import log_container, Logger
from mkpipe.functions_db import get_db_connector
from mkpipe.utils.base_class import PipeSettings
from mkpipe.plugins.registry_jar import collect_jars


class MariadbExtractor:
    def __init__(self, config, settings):
        if isinstance(settings, dict):
            self.settings = PipeSettings(**settings)
        else:
            self.settings = settings
        self.connection_params = config['connection_params']
        self.table = config['table']
        self.pass_on_error = config.get('pass_on_error', None)
        self.host = self.connection_params['host']
        self.port = self.connection_params['port']
        self.username = self.connection_params['user']
        self.password = quote_plus(str(self.connection_params['password']))
        self.database = self.connection_params['database']

        self.driver_name = 'mariadb'
        self.driver_jdbc = 'org.mariadb.jdbc.Driver'
        self.settings.driver_name = self.driver_name
        self.jdbc_url = f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.password}&sessionVariables=sql_mode=ANSI_QUOTES'

        config = load_config()
        connection_params = config['settings']['backend']
        db_type = connection_params['database_type']
        self.backend = get_db_connector(db_type)(connection_params)

    def create_spark_session(self):
        jars = collect_jars()
        conf = SparkConf()
        conf.setAppName(__file__)
        conf.setMaster('local[*]')
        conf.set('spark.driver.memory', self.settings.spark_driver_memory)
        conf.set('spark.executor.memory', self.settings.spark_executor_memory)
        conf.set('spark.jars', jars)
        conf.set('spark.driver.extraClassPath', jars)
        conf.set('spark.executor.extraClassPath', jars)
        conf.set('spark.network.timeout', '600s')
        conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
        conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')
        conf.set('spark.sql.parquet.int96RebaseModeInRead', 'CORRECTED')
        conf.set('spark.sql.parquet.int96RebaseModeInWrite', 'CORRECTED')
        conf.set('spark.sql.session.timeZone', self.settings.timezone)
        conf.set(
            'spark.driver.extraJavaOptions', f'-Duser.timezone={self.settings.timezone}'
        )
        conf.set(
            'spark.executor.extraJavaOptions',
            f'-Duser.timezone={self.settings.timezone}',
        )
        conf.set(
            'spark.driver.extraJavaOptions',
            '-XX:ErrorFile=/tmp/java_error%p.log -XX:HeapDumpPath=/tmp',
        )

        return SparkSession.builder.config(conf=conf).getOrCreate()

    def extract_incremental(self, t):
        logger = Logger(__file__)
        spark = self.create_spark_session()

        try:
            name = t['name']
            target_name = t['target_name']
            iterate_column_type = t['iterate_column_type']
            chunk_count_for_partition = t.get(
                'chunk_count_for_partition',
                self.settings.default_chunk_count_for_partition,
            )
            iterate_max_loop = t.get(
                'iterate_max_loop', self.settings.default_iterate_max_loop
            )
            custom_query = t.get('custom_query', None)
            custom_query_file = t.get('custom_query_file', None)
            if custom_query_file:
                custom_query_file_path = os.path.abspath(
                    os.path.join(self.settings.ROOT_DIR, 'sql', custom_query_file)
                )
                with open(custom_query_file_path, 'r') as f:
                    custom_query = f.read()

            custom_partition_count = t.get(
                'partition_count', self.settings.partitions_count
            )
            partitions_column_ = t.get('partitions_column')
            fetchsize = t.get('fetchsize', 100_000)

            partitions_column = partitions_column_.split(' as ')[0].strip()
            p_col_name = partitions_column_.split(' as ')[-1].strip()

            message = dict(table_name=target_name, status='extracting')
            logger.info(message)
            parquet_path = os.path.abspath(
                os.path.join(self.settings.ROOT_DIR, 'artifacts', target_name)
            )

            last_point = self.backend.get_last_point(target_name)
            if last_point:
                write_mode = 'append'
                iterate_query = f"""(SELECT min({partitions_column}) as min_val, max({partitions_column}) as max_val from {name} where {partitions_column} > '{last_point}' ) q"""
            else:
                write_mode = 'overwrite'
                iterate_query = f"""(SELECT min({partitions_column}) as min_val, max({partitions_column}) as max_val from {name}) q"""

            df_itarate_list = (
                spark.read.format('jdbc')
                .option('url', self.jdbc_url)
                .option('dbtable', iterate_query)
                .option('driver', self.driver_jdbc)
                .option('fetchsize', fetchsize)
                .load()
            )
            min_val = df_itarate_list.first()['min_val']
            max_val = df_itarate_list.first()['max_val']

            if min_val is None or max_val is None:
                min_max_tuple = None
            elif iterate_column_type == 'int':
                min_val = int(min_val)
                max_val = int(max_val)

                total_range = max_val - min_val + 1  # inclusive
                step = total_range // chunk_count_for_partition
                remainder = total_range % chunk_count_for_partition

                min_max_tuple = []
                start = min_val
                for _ in range(chunk_count_for_partition):
                    end = start + step - 1
                    if remainder > 0:
                        end += 1
                        remainder -= 1
                    if start <= max_val:
                        min_max_tuple.append((start, min(end, max_val)))
                        start = end + 1

            elif iterate_column_type == 'datetime':
                total_seconds = (
                    int((max_val - min_val).total_seconds()) + 1
                )  # include max_val
                step = total_seconds // chunk_count_for_partition
                remainder = total_seconds % chunk_count_for_partition

                min_max_tuple = []
                start = min_val
                for _ in range(chunk_count_for_partition):
                    step_with_remainder = step
                    if remainder > 0:
                        step_with_remainder += 1
                        remainder -= 1
                    end = start + datetime.timedelta(seconds=step_with_remainder - 1)
                    if start <= max_val:
                        min_max_tuple.append((start, min(end, max_val)))
                        start = end + datetime.timedelta(seconds=1)
            else:
                raise ValueError(
                    f'Unsupported iterate_column_type: {iterate_column_type}'
                )

            if not min_max_tuple:
                if not last_point:
                    # Empty table, need schema fetc
                    return self.extract_full(t)
                else:
                    # Not empt, but no new data, all fetched before
                    data = {
                        'table_name': target_name,
                        'status': 'extracted',
                        'replication_method': 'incremental',
                    }
                    return data

            data = {
                'table_name': target_name,
                'write_mode': write_mode,
                'file_type': 'parquet',
                'partition_count': custom_partition_count,
                'fetchsize': fetchsize,
                'last_point_value': None,  # Initialize as None or the starting value
                'iterate_column_type': iterate_column_type,
                'loop': None,  # This will be updated each loop
                'path': parquet_path,
                'number_of_columns': None,
                'number_of_rows': 0,  # Start with 0 and add to it in each loop
                'pass_on_error': self.pass_on_error,
                'status': 'extracted',
                'replication_method': 'incremental',
            }

            for index, chunk in enumerate(min_max_tuple):
                if iterate_max_loop == index:
                    break

                if index == 0:
                    p_write_mode = 'overwrite'
                else:
                    p_write_mode = 'append'

                if iterate_column_type == 'int':
                    min_filter = int(chunk[0])
                    max_filter = int(chunk[-1])
                    if custom_query:
                        updated_query = custom_query.replace(
                            '{query_filter}',
                            f""" where {partitions_column} >= {min_filter} and {partitions_column} < {max_filter} """,
                        )
                    else:
                        updated_query = f'(SELECT * from {name} where {partitions_column} >= {min_filter} and {partitions_column} < {max_filter}) q'
                elif iterate_column_type == 'datetime':
                    min_filter = chunk[0].strftime('%Y-%m-%d %H:%M:%S')
                    max_filter = chunk[-1].strftime('%Y-%m-%d %H:%M:%S')
                    if custom_query:
                        updated_query = custom_query.replace(
                            '{query_filter}',
                            f""" where {partitions_column} >= '{min_filter}' and {partitions_column} < '{max_filter}' """,
                        )
                    else:
                        updated_query = f"""(SELECT * from {name} where  {partitions_column} >= '{min_filter}' and {partitions_column} < '{max_filter}') q"""
                else:
                    raise ValueError(
                        f'Unsupported iterate_column_type: {iterate_column_type}'
                    )

                df = (
                    spark.read.format('jdbc')
                    .option('url', self.jdbc_url)
                    .option('dbtable', updated_query)
                    .option('driver', self.driver_jdbc)
                    .option('numPartitions', custom_partition_count)
                    .option('partitionColumn', p_col_name)
                    .option('lowerBound', min_val)
                    .option('upperBound', max_val)
                    .option('fetchsize', fetchsize)
                    .load()
                )

                # df.filter(df.cust_ord_id == 285708).select("udate").show(truncate=False)
                # df = df.dropDuplicates() # this process affecting the partition_count be careful
                (
                    df.write.option('compression', self.settings.compression_codec)
                    .mode(p_write_mode)
                    .parquet(parquet_path)
                )

                count_col = len(df.columns)
                count_row = df.count()
                last_point_value = max_filter

                # Update `data` for this iteration
                data['last_point_value'] = last_point_value  # update with the new max
                data['loop'] = index  # update loop index
                data['number_of_columns'] = count_col
                data['number_of_rows'] += (
                    count_row  # add current loop's row count to the cumulative total
                )

                message = dict(
                    table_name=target_name,
                    status='iterrated',
                    meta_data=data,
                )
                logger.info(message)
            logger.info(data)
            return data
        finally:
            # Ensure Spark session is closed
            spark.stop()

    def extract_full(self, t):
        logger = Logger(__file__)
        spark = self.create_spark_session()
        try:
            name = t['name']
            target_name = t['target_name']
            message = dict(table_name=target_name, status='extracting')
            logger.info(message)
            custom_partition_count = t.get(
                'partition_count', self.settings.partitions_count
            )
            fetchsize = t.get('fetchsize', 100_000)
            partitions_column_ = t.get('partitions_column', None)

            custom_query = t.get('custom_query', None)
            custom_query_file = t.get('custom_query_file', None)
            if custom_query_file:
                custom_query_file_path = os.path.abspath(
                    os.path.join(self.settings.ROOT_DIR, 'sql', custom_query_file)
                )
                with open(custom_query_file_path, 'r') as f:
                    custom_query = f.read()

            write_mode = 'overwrite'
            parquet_path = os.path.abspath(
                os.path.join(self.settings.ROOT_DIR, 'artifacts', target_name)
            )

            if not custom_query:
                updated_query = f'(SELECT * from {name}) q'
            else:
                updated_query = custom_query.replace(
                    '{query_filter}',
                    ' where 1=1 ',
                )

            if partitions_column_:
                partitions_column = partitions_column_.split(' as ')[0]
                p_col_name = partitions_column_.split(' as ')[-1]
                query_min_max = f'(SELECT min({partitions_column}), max({partitions_column}) from {name}) q'
                df_min_max = (
                    spark.read.format('jdbc')
                    .option('url', self.jdbc_url)
                    .option('dbtable', query_min_max)
                    .option('driver', self.driver_jdbc)
                    .option('fetchsize', fetchsize)
                    .load()
                )

                min_val = df_min_max.first()['min']
                max_val = df_min_max.first()['max']

                if min_val:
                    # which means table not empt
                    df = (
                        spark.read.format('jdbc')
                        .option('url', self.jdbc_url)
                        .option('dbtable', updated_query)
                        .option('driver', self.driver_jdbc)
                        .option('numPartitions', custom_partition_count)
                        .option('partitionColumn', p_col_name)
                        .option('lowerBound', min_val)
                        .option('upperBound', max_val)
                        .option('fetchsize', fetchsize)
                        .load()
                    )
                else:
                    # empty df, we need schema
                    df = (
                        spark.read.format('jdbc')
                        .option('url', self.jdbc_url)
                        .option('dbtable', updated_query)
                        .option('driver', self.driver_jdbc)
                        .option('fetchsize', fetchsize)
                        .load()
                    )

            else:
                df = (
                    spark.read.format('jdbc')
                    .option('url', self.jdbc_url)
                    .option('dbtable', updated_query)
                    .option('driver', self.driver_jdbc)
                    .option('fetchsize', fetchsize)
                    .load()
                ).repartition(custom_partition_count)

            df.write.parquet(parquet_path, mode=write_mode)

            count_col = len(df.columns)
            count_row = df.count()

            data = {
                'table_name': target_name,
                'path': parquet_path,
                'file_type': 'parquet',
                'number_of_columns': count_col,
                'number_of_rows': count_row,
                'write_mode': write_mode,
                'partition_count': custom_partition_count,
                'fetchsize': fetchsize,
                'pass_on_error': self.pass_on_error,
                'replication_method': 'full',
            }
            message = dict(
                table_name=target_name,
                status='extracted',
                meta_data=data,
            )
            logger.info(message)
            return data
        finally:
            # Ensure Spark session is closed
            spark.stop()

    @log_container(__file__)
    def extract(self):
        extract_start_time = datetime.datetime.now()
        logger = Logger(__file__)
        logger.info({'message': 'Extracting data from MariaDB...'})
        t = self.table
        try:
            target_name = t['target_name']
            replication_method = t.get('replication_method', None)
            if self.backend.get_table_status(target_name) in ['extracting', 'loading']:
                logger.info(
                    {'message': f'Skipping {target_name}, already in progress...'}
                )
                data = {
                    'table_name': target_name,
                    'status': 'completed',
                    'replication_method': 'full',
                }
                return data

            self.backend.manifest_table_update(
                name=target_name,
                value=None,  # Last point remains unchanged
                value_type=None,  # Type remains unchanged
                status='extracting',  # ('completed', 'failed', 'extracting', 'loading')
                replication_method=replication_method,  # ('incremental', 'full')
                error_message='',
            )
            if replication_method == 'incremental':
                return self.extract_incremental(t)
            else:
                return self.extract_full(t)

        except Exception as e:
            message = dict(
                table_name=target_name,
                status='failed',
                type='pipeline',
                error_message=str(e),
                etl_start_time=str(extract_start_time),
            )
            self.backend.manifest_table_update(
                target_name,
                None,
                None,
                status='failed',
                replication_method=replication_method,
                error_message=str(e),
            )
            if self.pass_on_error:
                logger.warning(message)
                return None
            else:
                raise Exception(message) from e
