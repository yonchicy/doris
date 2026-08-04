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
}
