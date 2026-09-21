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

suite("test_list_expression_partition_query_paths", "p0") {
    def savedVariables = [:]
    [
        "enable_runtime_filter_partition_prune",
        "enable_runtime_filter_prune",
        "runtime_filter_type",
        "runtime_filter_max_in_num",
        "runtime_filter_mode",
        "runtime_filter_wait_infinitely",
        "disable_join_reorder",
        "enable_short_circuit_query",
        "enable_sql_cache"
    ].each { name ->
        savedVariables[name] = sql("SHOW VARIABLES LIKE '${name}'")[0][1]
    }

    try {
        // Both sides of each session-switch comparison must execute their scan path.
        sql "SET enable_sql_cache = false"
        sql "DROP TABLE IF EXISTS list_expr_query_rf_probe"
        sql "DROP TABLE IF EXISTS list_expr_query_rf_build"
        sql "DROP TABLE IF EXISTS list_expr_query_point"
        sql "DROP TABLE IF EXISTS list_expr_query_point_control"

        sql """
            CREATE TABLE list_expr_query_rf_probe (
                dt DATETIMEV2(0) NOT NULL,
                id INT NOT NULL,
                value VARCHAR(32) NOT NULL
            )
            DUPLICATE KEY(dt, id)
            PARTITION BY LIST(date_trunc(dt, 'day')) (
                PARTITION p_day_23 VALUES IN ('2026-07-23 00:00:00'),
                PARTITION p_day_24 VALUES IN ('2026-07-24 00:00:00'),
                PARTITION p_day_25 VALUES IN ('2026-07-25 00:00:00')
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """
            CREATE TABLE list_expr_query_rf_build (
                dt DATETIMEV2(0) NOT NULL
            )
            DUPLICATE KEY(dt)
            DISTRIBUTED BY HASH(dt) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        sql """
            INSERT INTO list_expr_query_rf_probe VALUES
                ('2026-07-23 16:00:00', 1, 'first_match'),
                ('2026-07-23 16:00:00', 2, 'duplicate_time_match'),
                ('2026-07-23 08:00:00', 3, 'same_day_non_match'),
                ('2026-07-24 08:00:00', 4, 'second_day_match'),
                ('2026-07-25 12:00:00', 5, 'other_day_non_match')
        """
        // Use a real build table with no literal predicate: the join keys must reach
        // the probe through a runtime filter, not through static predicate inference.
        sql """
            INSERT INTO list_expr_query_rf_build VALUES
                ('2026-07-23 16:00:00'), ('2026-07-24 08:00:00')
        """
        // Small INSERT-only tables have no analyzed statistics. Keep their RF and
        // wait for the exact IN set before scanning so the partition guard is exercised.
        sql "SET enable_runtime_filter_prune = false"
        sql "SET runtime_filter_type = 'IN'"
        sql "SET runtime_filter_max_in_num = 1024"
        sql "SET runtime_filter_mode = 'GLOBAL'"
        sql "SET runtime_filter_wait_infinitely = true"
        sql "SET disable_join_reorder = true"

        def joinQuery = """
            SELECT probe.dt, probe.id, probe.value
            FROM list_expr_query_rf_probe probe
            JOIN [broadcast] list_expr_query_rf_build build ON probe.dt = build.dt
        """
        sql "SET enable_runtime_filter_partition_prune = false"
        explain {
            sql joinQuery
            contains "RF000[in]"
            contains "partitions=3/3 (p_day_23,p_day_24,p_day_25)"
        }
        order_qt_rf_partition_prune_disabled joinQuery

        // Midnight LIST keys are not the input column's point values. Enabling
        // partition pruning must not discard the two partitions containing RF hits.
        sql "SET enable_runtime_filter_partition_prune = true"
        explain {
            sql joinQuery
            contains "RF000[in]"
            contains "partitions=3/3 (p_day_23,p_day_24,p_day_25)"
        }
        order_qt_rf_partition_prune_enabled joinQuery

        sql """
            CREATE TABLE list_expr_query_point (
                dt DATETIMEV2(0) NOT NULL,
                id INT NOT NULL,
                value VARCHAR(32) NOT NULL
            )
            UNIQUE KEY(dt, id)
            PARTITION BY LIST(date_trunc(dt, 'day')) (
                PARTITION p_day_23 VALUES IN ('2026-07-23 00:00:00'),
                PARTITION p_day_24 VALUES IN ('2026-07-24 00:00:00')
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true"
            )
        """
        sql """
            INSERT INTO list_expr_query_point VALUES
                ('2026-07-23 16:00:00', 1, 'point_match'),
                ('2026-07-23 16:00:00', 2, 'other_key'),
                ('2026-07-24 16:00:00', 1, 'other_day')
        """
        def pointQuery = """
            SELECT dt, id, value FROM list_expr_query_point
            WHERE dt = '2026-07-23 16:00:00' AND id = 1
        """
        sql "SET enable_short_circuit_query = false"
        order_qt_point_normal_path pointQuery

        sql "SET enable_short_circuit_query = true"
        explain {
            sql pointQuery
            notContains "SHORT-CIRCUIT"
            contains "partitions=1/2 (p_day_23)"
        }
        order_qt_point_expression_partition pointQuery
        order_qt_point_expression_partition_other_day """
            SELECT dt, id, value FROM list_expr_query_point
            WHERE dt = '2026-07-24 16:00:00' AND id = 1
        """

        // A table with the same key and row-store properties but no partition
        // function still takes the short-circuit path: the guard is expression-specific.
        sql """
            CREATE TABLE list_expr_query_point_control (
                dt DATETIMEV2(0) NOT NULL,
                id INT NOT NULL,
                value VARCHAR(32) NOT NULL
            )
            UNIQUE KEY(dt, id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "store_row_column" = "true"
            )
        """
        sql """
            INSERT INTO list_expr_query_point_control VALUES
                ('2026-07-23 16:00:00', 1, 'control_match')
        """
        explain {
            sql """
                SELECT dt, id, value FROM list_expr_query_point_control
                WHERE dt = '2026-07-23 16:00:00' AND id = 1
            """
            contains "SHORT-CIRCUIT"
        }
        order_qt_point_control """
            SELECT dt, id, value FROM list_expr_query_point_control
            WHERE dt = '2026-07-23 16:00:00' AND id = 1
        """
    } finally {
        savedVariables.each { name, value ->
            sql "SET ${name} = '${value}'"
        }
    }
}
