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

suite("test_list_expression_partition", "p0") {
    sql "DROP TABLE IF EXISTS test_list_expression_partition"

    sql """
        CREATE TABLE test_list_expression_partition (
            id INT NOT NULL,
            col1 DATETIME NOT NULL,
            col2 DATETIME NOT NULL,
            region VARCHAR(16) NOT NULL,
            value VARCHAR(32) NULL
        )
        DUPLICATE KEY(id, col1, col2, region)
        PARTITION BY LIST(
            DATE_TRUNC(col1, 'day'),
            DATE_TRUNC(col2, 'day'),
            region
        ) (
            PARTITION p_day_23_east VALUES IN (
                ('2026-07-23 00:00:00', '2025-07-23 00:00:00', 'east')
            ),
            PARTITION p_day_23_west VALUES IN (
                ('2026-07-23 00:00:00', '2025-07-23 00:00:00', 'west')
            ),
            PARTITION p_day_24_east VALUES IN (
                ('2026-07-24 00:00:00', '2025-07-24 00:00:00', 'east')
            ),
            PARTITION p_cross VALUES IN (
                ('2026-07-25 00:00:00', '2025-07-26 00:00:00', 'cross'),
                ('2026-07-26 00:00:00', '2025-07-25 00:00:00', 'cross')
            )
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // The input values are inside each DATE_TRUNC range instead of on partition boundaries.
    sql """
        INSERT INTO test_list_expression_partition VALUES
            (1, '2026-07-23 16:00:00', '2025-07-23 16:00:00', 'east', 'insert_east'),
            (2, '2026-07-23 10:00:00', '2025-07-23 11:00:00', 'west', 'insert_west'),
            (3, '2026-07-24 12:00:00', '2025-07-24 13:00:00', 'east', 'insert_next_day'),
            (4, '2026-07-25 10:00:00', '2025-07-26 10:00:00', 'cross', 'insert_cross_1'),
            (5, '2026-07-26 11:00:00', '2025-07-25 11:00:00', 'cross', 'insert_cross_2')
    """

    sql """
        INSERT INTO test_list_expression_partition PARTITION(p_day_23_east)
        VALUES (6, '2026-07-23 08:00:00', '2025-07-23 09:00:00', 'east', 'explicit_partition')
    """

    order_qt_insert_routing """
        SELECT id, col1, col2, region, value
        FROM test_list_expression_partition
        ORDER BY id
    """

    // A partial-day predicate intersects both partitions whose first expression maps to 2026-07-23.
    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE col1 > '2026-07-23 02:00:00'
              AND col1 < '2026-07-23 18:00:00'
        """
        contains "partitions=2/4 (p_day_23_east,p_day_23_west)"
    }
    order_qt_partial_day """
        SELECT id FROM test_list_expression_partition
        WHERE col1 > '2026-07-23 02:00:00'
          AND col1 < '2026-07-23 18:00:00'
        ORDER BY id
    """

    // A range disjoint from every DATE_TRUNC range should prune all partitions.
    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE col1 > '2026-07-22 02:00:00'
              AND col1 < '2026-07-22 18:00:00'
        """
        contains "VEMPTYSET"
    }

    // Both expression columns participate in pruning.
    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE col1 >= '2026-07-23 02:00:00'
              AND col1 < '2026-07-23 18:00:00'
              AND col2 >= '2025-07-23 03:00:00'
              AND col2 < '2025-07-23 19:00:00'
        """
        contains "partitions=2/4 (p_day_23_east,p_day_23_west)"
    }

    // The ordinary LIST column remains a discrete value while expression columns use ranges.
    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE col1 >= '2026-07-23 02:00:00'
              AND col1 < '2026-07-23 18:00:00'
              AND col2 >= '2025-07-23 03:00:00'
              AND col2 < '2025-07-23 19:00:00'
              AND region = 'east'
        """
        contains "partitions=1/4 (p_day_23_east)"
    }
    order_qt_mixed_expression_and_column """
        SELECT id FROM test_list_expression_partition
        WHERE col1 >= '2026-07-23 02:00:00'
          AND col1 < '2026-07-23 18:00:00'
          AND col2 >= '2025-07-23 03:00:00'
          AND col2 < '2025-07-23 19:00:00'
          AND region = 'east'
        ORDER BY id
    """

    // Each tuple in one LIST partition is an independent OR branch.
    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE col1 >= '2026-07-25 00:00:00'
              AND col1 < '2026-07-26 00:00:00'
              AND col2 >= '2025-07-26 00:00:00'
              AND col2 < '2025-07-27 00:00:00'
              AND region = 'cross'
        """
        contains "partitions=1/4 (p_cross)"
    }
    order_qt_multi_tuple_positive """
        SELECT id FROM test_list_expression_partition
        WHERE col1 >= '2026-07-25 00:00:00'
          AND col1 < '2026-07-26 00:00:00'
          AND col2 >= '2025-07-26 00:00:00'
          AND col2 < '2025-07-27 00:00:00'
          AND region = 'cross'
        ORDER BY id
    """

    // Values from different tuples must not be combined into a synthetic tuple.
    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE col1 >= '2026-07-25 00:00:00'
              AND col1 < '2026-07-26 00:00:00'
              AND col2 >= '2025-07-25 00:00:00'
              AND col2 < '2025-07-26 00:00:00'
              AND region = 'cross'
        """
        contains "VEMPTYSET"
    }

    explain {
        sql """
            SELECT * FROM test_list_expression_partition
            WHERE (col1 = '2026-07-25 10:00:00'
                   AND col2 = '2025-07-26 10:00:00'
                   AND region = 'cross')
               OR (col1 = '2026-07-26 11:00:00'
                   AND col2 = '2025-07-25 11:00:00'
                   AND region = 'cross')
        """
        contains "partitions=1/4 (p_cross)"
    }
    order_qt_simple_or """
        SELECT id FROM test_list_expression_partition
        WHERE (col1 = '2026-07-25 10:00:00'
               AND col2 = '2025-07-26 10:00:00'
               AND region = 'cross')
           OR (col1 = '2026-07-26 11:00:00'
               AND col2 = '2025-07-25 11:00:00'
               AND region = 'cross')
        ORDER BY id
    """

    test {
        sql """
            INSERT INTO test_list_expression_partition
            VALUES (7, '2026-07-30 10:00:00', '2025-07-30 10:00:00', 'east', 'no_partition')
        """
        exception "no partition for this tuple"
    }

    test {
        sql """
            INSERT INTO test_list_expression_partition PARTITION(p_day_24_east)
            VALUES (7, '2026-07-23 10:00:00', '2025-07-23 10:00:00', 'east', 'wrong_partition')
        """
        exception "no partition for this tuple"
    }

    sql """
        ALTER TABLE test_list_expression_partition
        ADD PARTITION p_day_27_east VALUES IN (
            ('2026-07-27 00:00:00', '2025-07-27 00:00:00', 'east')
        )
    """
    sql """
        INSERT INTO test_list_expression_partition
        VALUES (7, '2026-07-27 14:00:00', '2025-07-27 15:00:00', 'east', 'alter_partition')
    """

    test {
        sql """
            ALTER TABLE test_list_expression_partition
            ADD PARTITION p_not_aligned VALUES IN (
                ('2026-07-28 13:00:00', '2025-07-28 00:00:00', 'east')
            )
        """
        exception "is not aligned with"
    }

    streamLoad {
        table "test_list_expression_partition"
        set "column_separator", ","
        file "test_list_expression_partition.csv"
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberLoadedRows)
        }
    }
    sql "SYNC"

    order_qt_final_result """
        SELECT id, col1, col2, region, value
        FROM test_list_expression_partition
        ORDER BY id
    """

    // A WHERE clause makes the load planner prune the destination partitions.
    // The first day's boundary is outside this filter, but its afternoon rows
    // must still be loaded. The ordinary LIST column also participates in pruning.
    streamLoad {
        table "test_list_expression_partition"
        set "column_separator", ","
        set "where", "col1 >= '2026-07-23 12:00:00'" +
                " AND col1 < '2026-07-24 12:00:00' AND region = 'east'"
        file "test_list_expression_partition_where.csv"
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(5, json.NumberTotalRows)
            assertEquals(2, json.NumberLoadedRows)
            assertEquals(3, json.NumberUnselectedRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }
    sql "SYNC"
    order_qt_filtered_stream_load """
        SELECT id, col1, col2, region, value
        FROM test_list_expression_partition
        ORDER BY id
    """

    // Both days contain matching rows. Treating their midnight keys as points
    // would select only the second day and silently leave the first day's rows.
    sql """
        DELETE FROM test_list_expression_partition
        WHERE col1 >= '2026-07-23 12:00:00'
          AND col1 < '2026-07-24 12:00:00'
          AND region = 'east'
    """
    order_qt_delete_cross_day """
        SELECT id, col1, col2, region, value
        FROM test_list_expression_partition
        ORDER BY id
    """

    // Exercise the explicit-partition path and the second expression column.
    // Other rows in this same partition must retain the original row predicate.
    sql """
        DELETE FROM test_list_expression_partition PARTITION p_day_23_east
        WHERE col2 = '2025-07-23 09:45:00'
    """
    order_qt_delete_explicit_partition """
        SELECT id, col1, col2, region, value
        FROM test_list_expression_partition
        ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_list_expression_partition_delete_auto"
    sql """
        CREATE TABLE test_list_expression_partition_delete_auto (
            dt DATETIME NOT NULL,
            id INT NOT NULL,
            value INT
        )
        DUPLICATE KEY(dt, id)
        AUTO PARTITION BY LIST(DATE_TRUNC(dt, 'day')) ()
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_list_expression_partition_delete_auto VALUES
            ('2026-07-23 08:00:00', 1, 10),
            ('2026-07-23 16:00:00', 2, 20),
            ('2026-07-24 08:00:00', 3, 30),
            ('2026-07-24 16:00:00', 4, 40),
            ('2026-07-25 08:00:00', 5, 50)
    """
    order_qt_auto_delete_before """
        SELECT dt, id, value FROM test_list_expression_partition_delete_auto ORDER BY dt, id
    """
    sql """
        DELETE FROM test_list_expression_partition_delete_auto
        WHERE dt >= '2026-07-23 12:00:00' AND dt < '2026-07-24 12:00:00'
    """
    order_qt_auto_delete_cross_day """
        SELECT dt, id, value FROM test_list_expression_partition_delete_auto ORDER BY dt, id
    """
}
