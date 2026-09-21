// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("query_cache_list_expression_partition") {
    def assertHasCache = { String sqlText ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlText}"
                sleep(10 * 1000)
            }
            check { profileString, exception ->
                if (exception != null) {
                    throw exception
                }
                logger.info("query cache profile: " + profileString)
                assertTrue((profileString =~ /HitCache:\s+1/).find())
                assertFalse((profileString =~ /HitCache:\s+0/).find())
            }
        }
    }

    sql "SET enable_sql_cache = false"
    sql "SET enable_nereids_distribute_planner = true"
    sql "SET parallel_pipeline_task_num = 2"
    sql "SET enable_query_cache = false"

    sql "DROP TABLE IF EXISTS query_cache_list_expr_not_in"
    sql """
        CREATE TABLE query_cache_list_expr_not_in (
            dt DATETIMEV2(0) NOT NULL,
            region VARCHAR(16) NOT NULL,
            id INT NOT NULL,
            value INT NOT NULL
        )
        DUPLICATE KEY(dt, region, id)
        AUTO PARTITION BY LIST(date_trunc(dt, 'day')) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // AUTO partitioning routes intra-day rows through the DATE_TRUNC expression.
    sql """
        INSERT INTO query_cache_list_expr_not_in VALUES
            ('2024-03-15 10:00:00', 'march', 1, 10),
            ('2024-03-16 11:00:00', 'march', 2, 20),
            ('2024-03-20 12:00:00', 'march', 3, 30),
            ('2024-03-21 13:00:00', 'march', 4, 40),
            ('2024-04-15 14:00:00', 'april', 5, 50)
    """

    def firstNotIn = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_not_in
        WHERE dt >= '2024-03-15 09:00:00'
          AND dt < '2024-03-22 00:00:00'
          AND dt NOT IN ('2024-03-15 10:00:00', '2024-03-20 12:00:00')
        GROUP BY region
    """
    def equivalentNotIn = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_not_in
        WHERE dt >= '2024-03-15 09:00:00'
          AND dt < '2024-03-22 00:00:00'
          AND dt NOT IN ('2024-03-20 12:00:00', '2024-03-15 10:00:00', '2024-03-20 12:00:00')
        GROUP BY region
    """
    def secondNotIn = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_not_in
        WHERE dt >= '2024-03-15 09:00:00'
          AND dt < '2024-03-22 00:00:00'
          AND dt NOT IN ('2024-03-16 11:00:00', '2024-03-21 13:00:00')
        GROUP BY region
    """
    order_qt_list_expr_not_in_uncached firstNotIn
    order_qt_list_expr_not_in_equivalent_uncached equivalentNotIn
    order_qt_list_expr_not_in_distinct_uncached secondNotIn
    sql "SET enable_query_cache = true"
    explain {
        sql firstNotIn
        contains "DIGEST"
    }
    order_qt_list_expr_not_in_fill firstNotIn
    // Reordering and duplicating excluded points preserves the effective range.
    assertHasCache(equivalentNotIn)
    order_qt_list_expr_not_in_equivalent_hit equivalentNotIn
    // A different set of excluded points uses the same tablet but a distinct range.
    // Its first execution must return its own baseline, not the previously cached aggregate.
    order_qt_list_expr_not_in_distinct_first secondNotIn

    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS query_cache_list_expr_multi_tuple"
    sql """
        CREATE TABLE query_cache_list_expr_multi_tuple (
            dt DATETIMEV2(0) NOT NULL,
            region VARCHAR(16) NOT NULL,
            id INT NOT NULL,
            value INT NOT NULL
        )
        DUPLICATE KEY(dt, region, id)
        PARTITION BY LIST(date_trunc(dt, 'day'), region) (
            PARTITION p_march VALUES IN (
                ('2024-03-15 00:00:00', 'us'),
                ('2024-03-15 00:00:00', 'eu'),
                ('2024-03-20 00:00:00', 'eu')
            ),
            PARTITION p_april VALUES IN (
                ('2024-04-15 00:00:00', 'us')
            )
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO query_cache_list_expr_multi_tuple VALUES
            ('2024-03-15 00:00:00', 'us', 1, 11),
            ('2024-03-15 00:00:00', 'eu', 2, 100),
            ('2024-03-20 00:00:00', 'eu', 3, 200),
            ('2024-04-15 00:00:00', 'us', 4, 400)
    """

    def usTuple = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_multi_tuple
        WHERE dt >= '2024-03-15 00:00:00'
          AND dt < '2024-03-21 00:00:00'
          AND region = 'us'
        GROUP BY region
    """
    def usTupleEquivalent = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_multi_tuple
        WHERE dt >= '2024-03-15 00:00:00'
          AND dt < '2024-03-21 00:00:00'
          AND region IN ('us')
        GROUP BY region
    """
    def euTuple = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_multi_tuple
        WHERE dt >= '2024-03-15 00:00:00'
          AND dt < '2024-03-21 00:00:00'
          AND region = 'eu'
        GROUP BY region
    """
    def bothTuples = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_multi_tuple
        WHERE dt >= '2024-03-15 00:00:00'
          AND dt < '2024-03-21 00:00:00'
          AND region IN ('us', 'eu')
        GROUP BY region
        ORDER BY region
    """
    def bothTuplesEquivalent = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_multi_tuple
        WHERE dt >= '2024-03-15 00:00:00'
          AND dt < '2024-03-21 00:00:00'
          AND region IN ('eu', 'us', 'us')
        GROUP BY region
        ORDER BY region
    """
    order_qt_list_expr_multi_tuple_us_uncached usTuple
    order_qt_list_expr_multi_tuple_us_equivalent_uncached usTupleEquivalent
    order_qt_list_expr_multi_tuple_eu_uncached euTuple
    order_qt_list_expr_multi_tuple_both_uncached bothTuples
    order_qt_list_expr_multi_tuple_both_equivalent_uncached bothTuplesEquivalent
    sql "SET enable_query_cache = true"
    explain {
        sql usTuple
        contains "DIGEST"
    }
    order_qt_list_expr_multi_tuple_us_fill usTuple
    assertHasCache(usTupleEquivalent)
    order_qt_list_expr_multi_tuple_us_equivalent_hit usTupleEquivalent
    // EU shares the physical tablet with US but must not cross-hit its tuple range.
    order_qt_list_expr_multi_tuple_eu_first euTuple
    // Keep multiple tuples alive to cover multi-tuple serialization and equivalent IN normalization.
    order_qt_list_expr_multi_tuple_both_first bothTuples
    assertHasCache(bothTuplesEquivalent)
    order_qt_list_expr_multi_tuple_both_equivalent_hit bothTuplesEquivalent

    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS query_cache_list_expr_null_partition"
    sql """
        CREATE TABLE query_cache_list_expr_null_partition (
            dt DATETIMEV2(0) NULL,
            region VARCHAR(16) NOT NULL,
            id INT NOT NULL,
            value INT NOT NULL
        )
        DUPLICATE KEY(dt, region, id)
        PARTITION BY LIST(date_trunc(dt, 'day')) (
            PARTITION p_null VALUES IN (NULL),
            PARTITION p_day VALUES IN ('2024-03-15 00:00:00')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO query_cache_list_expr_null_partition VALUES
            (NULL, 'null-value', 1, 1),
            ('2024-03-15 00:00:00', 'day-value', 2, 20)
    """
    def nullPartitionQuery = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_null_partition
        WHERE dt IS NULL
        GROUP BY region
    """
    order_qt_list_expr_null_partition_uncached nullPartitionQuery
    sql "SET enable_query_cache = true"
    explain {
        sql nullPartitionQuery
        notContains "DIGEST"
    }
    order_qt_list_expr_null_partition_first nullPartitionQuery
    order_qt_list_expr_null_partition_second nullPartitionQuery

    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS query_cache_list_expr_default_partition"
    sql """
        CREATE TABLE query_cache_list_expr_default_partition (
            dt DATETIMEV2(0) NOT NULL,
            region VARCHAR(16) NOT NULL,
            id INT NOT NULL,
            value INT NOT NULL
        )
        DUPLICATE KEY(dt, region, id)
        PARTITION BY LIST(date_trunc(dt, 'day')) (
            PARTITION p_day VALUES IN ('2024-03-15 00:00:00'),
            PARTITION p_default
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO query_cache_list_expr_default_partition VALUES
            ('2024-03-15 00:00:00', 'day-value', 1, 20),
            ('2024-04-15 00:00:00', 'default-value', 2, 300)
    """
    def defaultPartitionQuery = """
        SELECT region, SUM(value), COUNT(*)
        FROM query_cache_list_expr_default_partition
        WHERE dt = '2024-04-15 00:00:00'
        GROUP BY region
    """
    order_qt_list_expr_default_partition_uncached defaultPartitionQuery
    sql "SET enable_query_cache = true"
    explain {
        sql defaultPartitionQuery
        notContains "DIGEST"
    }
    order_qt_list_expr_default_partition_first defaultPartitionQuery
    order_qt_list_expr_default_partition_second defaultPartitionQuery
}
