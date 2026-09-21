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

suite("test_list_expression_partition_null", "p0") {
    sql "DROP TABLE IF EXISTS test_list_expression_partition_null"
    sql """
        CREATE TABLE test_list_expression_partition_null (
            dt DATETIMEV2(0) NULL,
            region_id INT NOT NULL,
            id INT NOT NULL,
            value VARCHAR(32) NULL
        )
        DUPLICATE KEY(dt, region_id, id)
        PARTITION BY LIST(date_trunc(dt, 'day'), region_id) (
            PARTITION p_mixed VALUES IN (
                (NULL, 1),
                ('2026-07-23 00:00:00', 2)
            ),
            PARTITION p_day_23_region_1 VALUES IN (
                ('2026-07-23 00:00:00', 1)
            ),
            PARTITION p_day_24_region_1 VALUES IN (
                ('2026-07-24 00:00:00', 1)
            ),
            PARTITION p_default
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_list_expression_partition_null VALUES
            (NULL, 1, 1, 'null-explicit'),
            (NULL, 2, 2, 'null-default'),
            ('2026-07-23 16:00:00', 2, 3, 'day-mixed'),
            ('2026-07-23 16:00:00', 1, 4, 'day-explicit'),
            ('2026-07-24 16:00:00', 1, 5, 'other-day'),
            ('2026-07-25 16:00:00', 3, 6, 'default-value')
    """

    // The NULL tuple and a non-NULL DATE_TRUNC tuple share one physical LIST
    // partition. Planning must preserve SQL NULL semantics and tuple correlation.
    explain {
        sql """
            SELECT dt, region_id, id, value
            FROM test_list_expression_partition_null
            WHERE dt IS NULL AND region_id = 1
        """
        contains "p_mixed"
        notContains "p_day_23_region_1"
        notContains "p_day_24_region_1"
    }
    order_qt_null_explicit_tuple """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt IS NULL AND region_id = 1
        ORDER BY id
    """

    // (NULL, 2) is not an explicit tuple and therefore belongs to DEFAULT.
    explain {
        sql """
            SELECT dt, region_id, id, value
            FROM test_list_expression_partition_null
            WHERE dt IS NULL AND region_id = 2
        """
        contains "p_default"
        notContains "p_mixed"
        notContains "p_day_23_region_1"
        notContains "p_day_24_region_1"
    }
    order_qt_null_default_tuple """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt IS NULL AND region_id = 2
        ORDER BY id
    """

    explain {
        sql """
            SELECT dt, region_id, id, value
            FROM test_list_expression_partition_null
            WHERE dt = '2026-07-23 16:00:00' AND region_id = 2
        """
        contains "p_mixed"
        notContains "p_day_23_region_1"
        notContains "p_day_24_region_1"
    }
    order_qt_non_null_tuple_in_mixed_partition """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt = '2026-07-23 16:00:00' AND region_id = 2
        ORDER BY id
    """

    explain {
        sql """
            SELECT dt, region_id, id, value
            FROM test_list_expression_partition_null
            WHERE dt = '2026-07-23 16:00:00' AND region_id = 1
        """
        contains "p_day_23_region_1"
        notContains "p_mixed"
        notContains "p_day_24_region_1"
    }
    order_qt_same_boundary_different_tuple """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt = '2026-07-23 16:00:00' AND region_id = 1
        ORDER BY id
    """

    // An unmatched non-NULL tuple is routed to DEFAULT.
    explain {
        sql """
            SELECT dt, region_id, id, value
            FROM test_list_expression_partition_null
            WHERE dt = '2026-07-25 16:00:00' AND region_id = 3
        """
        contains "p_default"
        notContains "p_mixed"
        notContains "p_day_23_region_1"
        notContains "p_day_24_region_1"
    }
    order_qt_default_non_null_tuple """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt = '2026-07-25 16:00:00' AND region_id = 3
        ORDER BY id
    """

    // IN/NOT IN with a NULL option exercises three-valued logic while pruning
    // DATE_TRUNC LIST ranges. NOT IN (..., NULL) is never TRUE for any row.
    order_qt_in_with_null_option """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt IN ('2026-07-23 16:00:00', NULL)
        ORDER BY id
    """

    explain {
        sql """
            SELECT dt, region_id, id, value
            FROM test_list_expression_partition_null
            PARTITION (p_mixed, p_day_23_region_1, p_day_24_region_1)
            WHERE dt NOT IN ('2026-07-23 16:00:00', NULL)
        """
        contains "VEMPTYSET"
    }
    order_qt_not_in_with_null_option """
        SELECT dt, region_id, id, value
        FROM test_list_expression_partition_null
        WHERE dt NOT IN ('2026-07-23 16:00:00', NULL)
        ORDER BY id
    """
}
