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

suite("test_mtmv_list_expr_partition", "mtmv") {
    String suiteName = "test_mtmv_list_expr_partition"
    String dbName = context.config.getDbNameByFile(context.file)
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    // base table partitioned by list expression: stored boundaries are date_trunc results
    sql """
        CREATE TABLE ${tableName}
        (
            id INT NOT NULL,
            col1 DATETIME NOT NULL,
            region VARCHAR(16) NOT NULL,
            value INT NULL
        )
        DUPLICATE KEY(id)
        AUTO PARTITION BY LIST (DATE_TRUNC(col1, 'day'))
        (
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    // source column values inside the day ranges instead of on partition boundaries
    sql """
        INSERT INTO ${tableName} VALUES
            (1, '2026-07-23 16:00:00', 'east', 10),
            (2, '2026-07-23 10:00:00', 'west', 20),
            (3, '2026-07-24 12:00:00', 'east', 30)
    """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY (col1)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT id, col1, region, value FROM ${tableName}
    """

    def showPartitionsResult = sql """show partitions from ${mvName}"""
    logger.info("showPartitionsResult: " + showPartitionsResult.toString())
    assertTrue(showPartitionsResult.toString().contains("p_20260723000000"))
    assertTrue(showPartitionsResult.toString().contains("p_20260724000000"))

    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_full_refresh "SELECT id, col1, region, value FROM ${mvName} ORDER BY id"

    // incremental refresh: new day partition and new data
    sql """INSERT INTO ${tableName} VALUES (4, '2026-07-25 09:00:00', 'north', 40)"""
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_incremental_refresh "SELECT id, col1, region, value FROM ${mvName} ORDER BY id"
    order_qt_incremental_base "SELECT id, col1, region, value FROM ${tableName} ORDER BY id"

    sql """drop materialized view if exists ${mvName};"""

    // MV with date_trunc(month) expression rollup on a day-granularity LIST expr table
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY (date_trunc(col1, 'month'))
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT id, col1, region, value FROM ${tableName}
    """

    def rollupPartitions = sql """show partitions from ${mvName}"""
    logger.info("rollupPartitions: " + rollupPartitions.toString())
    // LIST rollup groups all three day values into a single MV list partition,
    // whose name is the concatenation of the discrete in-values (not a month boundary).
    assertEquals(1, rollupPartitions.size())
    assertTrue(rollupPartitions.toString().contains("p_20260725000000_20260724000000_20260723000000"))

    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinishedByMvName(mvName)
    order_qt_rollup_month "SELECT id, col1, region, value FROM ${mvName} ORDER BY id"

    sql """drop materialized view if exists ${mvName};"""

    // MV with same granularity (day) as base table should succeed
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        PARTITION BY (date_trunc(col1, 'day'))
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT id, col1, region, value FROM ${tableName}
    """
    def sameGranularityPartitions = sql """show partitions from ${mvName}"""
    assertTrue(sameGranularityPartitions.toString().contains("p_20260723000000"))
    assertTrue(sameGranularityPartitions.toString().contains("p_20260724000000"))
    assertTrue(sameGranularityPartitions.toString().contains("p_20260725000000"))
    sql """drop materialized view if exists ${mvName};"""

    // MV with finer granularity (hour) than base table (day) should fail
    test {
        sql """
            CREATE MATERIALIZED VIEW ${mvName}
            BUILD DEFERRED REFRESH AUTO ON MANUAL
            PARTITION BY (date_trunc(col1, 'hour'))
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES ('replication_num' = '1')
            AS
            SELECT id, col1, region, value FROM ${tableName}
        """
        exception "must not be finer than"
    }
}
