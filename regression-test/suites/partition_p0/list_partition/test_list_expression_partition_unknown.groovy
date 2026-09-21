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

suite("test_list_expression_partition_unknown", "p0") {
    sql "DROP TABLE IF EXISTS test_list_expression_partition_unknown"
    sql """
        CREATE TABLE test_list_expression_partition_unknown (
            dt DATETIMEV2(0) NOT NULL,
            id INT NOT NULL
        )
        DUPLICATE KEY(dt, id)
        AUTO PARTITION BY LIST(date_trunc(dt, 'day')) (
            PARTITION p_day_23 VALUES IN ('2026-07-23 00:00:00'),
            PARTITION p_day_24 VALUES IN ('2026-07-24 00:00:00')
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        INSERT INTO test_list_expression_partition_unknown VALUES
            ('2026-07-23 08:00:00', 1),
            ('2026-07-23 11:59:59', 2),
            ('2026-07-23 12:00:00', 3),
            ('2026-07-23 16:00:00', 4),
            ('2026-07-24 00:00:00', 5),
            ('2026-07-24 08:00:00', 6)
    """

    // On July 23, IN is UNKNOWN. UNKNOWN AND TRUE remains UNKNOWN in the
    // morning, while UNKNOWN AND FALSE becomes FALSE from noon onward.
    // Keep this predicate in the projection as well as WHERE to expose its
    // three-valued result independently of partition predicate pruning.
    order_qt_nested_unknown_truth_values """
        SELECT id, dt,
               dt IN ('2026-07-24 00:00:00', NULL) AS in_result,
               (dt IN ('2026-07-24 00:00:00', NULL)
                   AND dt < '2026-07-23 12:00:00') AS and_result,
               (dt IN ('2026-07-24 00:00:00', NULL)
                   AND dt < '2026-07-23 12:00:00') IS NULL AS matches
        FROM test_list_expression_partition_unknown
        ORDER BY id
    """

    // The IN points do not intersect July 23, but NULL makes its result
    // UNKNOWN instead of FALSE. An empty range attached to UNKNOWN would
    // make the enclosing AND look contradictory and incorrectly prune p_day_23.
    explain {
        sql """
            SELECT id, dt
            FROM test_list_expression_partition_unknown
            WHERE (
                dt IN ('2026-07-24 00:00:00', NULL)
                AND dt < '2026-07-23 12:00:00'
            ) IS NULL
        """
        contains "p_day_23"
        notContains "VEMPTYSET"
    }
    order_qt_nested_unknown_and_is_null """
        SELECT id, dt
        FROM test_list_expression_partition_unknown
        WHERE (
            dt IN ('2026-07-24 00:00:00', NULL)
            AND dt < '2026-07-23 12:00:00'
        ) IS NULL
        ORDER BY id
    """
}
