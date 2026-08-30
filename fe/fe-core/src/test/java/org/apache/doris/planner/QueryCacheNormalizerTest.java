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

package org.apache.doris.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TRuntimeFilterMode;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryCacheNormalizerTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(QueryCacheNormalizerTest.class);

    @Override
    protected void runBeforeAll() throws Exception {
        // Create database `db1`.
        createDatabase("db1");

        useDatabase("db1");

        // Create tables.
        String nonPart = "create table db1.non_part("
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(k1, k2, k3)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String part1 = "create table db1.part1("
                + "  dt date,\n"
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(dt, k1, k2, k3)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "  PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "  PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String part2 = "create table db1.part2("
                + "  dt date,\n"
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(dt, k1, k2, k3)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "  PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "  PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String multiLeveParts = "create table db1.multi_level_parts("
                + "  k1 int,\n"
                + "  dt date,\n"
                + "  hour int,\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "PARTITION BY RANGE(dt, hour)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01', '0'), ('2024-03-01', '1')),\n"
                + "  PARTITION p202404 VALUES [('2024-03-01', '1'), ('2024-03-01', '2')),\n"
                + "  PARTITION p202405 VALUES [('2024-03-01', '2'), ('2024-03-01', '3'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String variantTable = "create table db1.variant_tbl("
                + "  k1 int,\n"
                + "  data variant)\n"
                + "DUPLICATE KEY(k1)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String uniqueMowTable = "create table db1.uniq_mow("
                + "  k1 int,\n"
                + "  v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true')";

        String uniqueMorTable = "create table db1.uniq_mor("
                + "  k1 int,\n"
                + "  v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'false')";

        String listRegionTable = "create table db1.list_region("
                + "  region varchar(32),\n"
                + "  k1 int,\n"
                + "  v1 int)\n"
                + "DUPLICATE KEY(region, k1)\n"
                + "PARTITION BY LIST(region)\n"
                + "(\n"
                + "  PARTITION p_us VALUES IN (\"us\"),\n"
                + "  PARTITION p_eu VALUES IN (\"eu\"),\n"
                + "  PARTITION p_ap VALUES IN (\"ap\")\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String listMonthTable = "create table db1.list_month("
                + "  dt datetime,\n"
                + "  k1 int,\n"
                + "  v1 int)\n"
                + "DUPLICATE KEY(dt, k1)\n"
                + "PARTITION BY LIST(date_trunc(dt, 'month'))\n"
                + "(\n"
                + "  PARTITION p202403 VALUES IN ('2024-03-01 00:00:00'),\n"
                + "  PARTITION p202404 VALUES IN ('2024-04-01 00:00:00'),\n"
                + "  PARTITION p202405 VALUES IN ('2024-05-01 00:00:00')\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String listMultiTable = "create table db1.list_multi("
                + "  dt datetime,\n"
                + "  region varchar(32),\n"
                + "  k1 int,\n"
                + "  v1 int)\n"
                + "DUPLICATE KEY(dt, region, k1)\n"
                + "PARTITION BY LIST(date_trunc(dt, 'month'), region)\n"
                + "(\n"
                + "  PARTITION p_us_03 VALUES IN (('2024-03-01 00:00:00','us')),\n"
                + "  PARTITION p_eu_03 VALUES IN (('2024-03-01 00:00:00','eu')),\n"
                + "  PARTITION p_us_04 VALUES IN (('2024-04-01 00:00:00','us'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String aggTable = "create table db1.agg_tbl("
                + "  k1 int,\n"
                + "  v1 int sum)\n"
                + "AGGREGATE KEY(k1)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        createTables(nonPart, part1, part2, multiLeveParts, variantTable, uniqueMowTable,
                uniqueMorTable, aggTable, listRegionTable, listMonthTable, listMultiTable);

        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableQueryCache(true);
        connectContext.getSessionVariable().parallelPipelineTaskNum = 2;
    }

    @Test
    public void testNormalize() throws Exception {
        String digest1 = getDigest("select k1 as k, sum(v1) as v from db1.non_part group by 1");
        String digest2 = getDigest("select sum(v1) as v1, k1 as k from db1.non_part group by 2");
        Assertions.assertEquals(64, digest1.length());
        Assertions.assertEquals(digest1, digest2);

        String digest3 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 between 1 and 10 group by 1");
        Assertions.assertNotEquals(digest1, digest3);

        String digest4 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 >= 1 and v1 <= 10 group by 1");
        Assertions.assertEquals(digest3, digest4);
        Assertions.assertEquals(64, digest3.length());

        String digest5 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 >= 1 and v1 < 11 group by 1");
        Assertions.assertNotEquals(digest3, digest5);
        Assertions.assertEquals(64, digest5.length());
    }

    @Test
    public void testProjectOnOlapScan() throws Exception {
        String digest1 = getDigest("select k1 + 1, k2, sum(v1), sum(v2) as v from db1.non_part group by 1, 2");
        String digest2 = getDigest("select sum(v2), k2, sum(v1), k1 + 1 from db1.non_part group by 2, 4");
        Assertions.assertEquals(digest1, digest2);
        Assertions.assertEquals(64, digest1.length());

        String digest3 = getDigest("select k1 + 1, k2, sum(v1 + 1), sum(v2) as v from db1.non_part group by 1, 2");
        Assertions.assertNotEquals(digest1, digest3);
        Assertions.assertEquals(64, digest3.length());
    }

    // after add hint control agg phase, this case will be reopen
    @Test
    public void testProjectOnAggregate() throws Exception {
        connectContext.getSessionVariable()
                .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setAggPhase(1);
        try {
            String digest1 = getDigest(
                    "select k1 + 1, k2 + 2, sum(v1) + 3, sum(v2) + 4 as v from db1.non_part group by k1, k2"
            );
            String digest2 = getDigest(
                    "select sum(v2) + 4, k2 + 2, sum(v1) + 3, k1 + 1 as v from db1.non_part group by k2, k1"
            );
            Assertions.assertEquals(digest1, digest2);
            Assertions.assertEquals(64, digest1.length());
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
            connectContext.getSessionVariable().setAggPhase(0);
        }
    }

    @Test
    public void testPartitionTable() throws Throwable {
        TQueryCacheParam queryCacheParam1 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 group by 1");
        TQueryCacheParam queryCacheParam2 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2025-01-01' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam2.digest);
        Assertions.assertEquals(32, queryCacheParam1.digest.remaining());
        Assertions.assertEquals(queryCacheParam1.tablet_to_range, queryCacheParam2.tablet_to_range);

        TQueryCacheParam queryCacheParam3 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2024-05-20' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam3.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                Lists.newArrayList(queryCacheParam3.tablet_to_range.values())
        );
        Assertions.assertEquals(32, queryCacheParam3.digest.remaining());

        TQueryCacheParam queryCacheParam4 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2024-04-20' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam4.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                Lists.newArrayList(queryCacheParam4.tablet_to_range.values())
        );
        Assertions.assertEquals(32, queryCacheParam4.digest.remaining());
    }

    @Test
    public void testMultiLevelPartitionTable() throws Throwable {
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select k1, sum(v1) as v from db1.multi_level_parts group by 1");
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    @Test
    public void testHaving() throws Throwable {
        List<TNormalizedPlanNode> normalizedPlanNodes = phaseAgg(1, () -> normalizePlans(
                "select k1, sum(v1) as v from db1.part1 where dt='2024-05-01' group by 1 having v > 10"));
        Assertions.assertEquals(2, normalizedPlanNodes.size());
        Assertions.assertTrue(
                normalizedPlanNodes.get(0).getOlapScanNode() != null
                        && CollectionUtils.isEmpty(normalizedPlanNodes.get(0).getConjuncts())
        );
        Assertions.assertTrue(
                normalizedPlanNodes.get(1).getAggregationNode() != null
                && !CollectionUtils.isEmpty(normalizedPlanNodes.get(1).getConjuncts())
        );
    }

    @Test
    public void testRuntimeFilter() throws Throwable {
        connectContext.getSessionVariable().setRuntimeFilterMode(TRuntimeFilterMode.GLOBAL.toString());
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.IN_OR_BLOOM.getValue());
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select * from (select k1, count(*) from db1.part1 where k1 < 15 group by k1)a\n"
                        + "join (select k1, count(*) from db1.part1 where k1 < 10 group by k1)b\n"
                        + "on a.k1 = b.k1");

        // only non target side can use query cache
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    @Test
    public void testSelectFromWhereNoGroupBy() throws Throwable {
        List<TNormalizedPlanNode> plans1 = normalizePlans("select sum(v1) from db1.part1");
        List<TNormalizedPlanNode> plans2 = normalizePlans("select sum(v1) as v from db1.part1");
        Assertions.assertEquals(plans1, plans2);

        List<TNormalizedPlanNode> plans3 = normalizePlans("select sum(v1) from db1.part1 where dt < '2025-01-01'");
        Assertions.assertEquals(plans1, plans3);

        List<TNormalizedPlanNode> plans4 = normalizePlans("select sum(v1) from db1.part1 where dt < '2024-05-01'");
        Assertions.assertEquals(plans1, plans4);

        TQueryCacheParam queryCacheParam1 = getQueryCacheParam("select sum(v1) from db1.part1");
        TQueryCacheParam queryCacheParam2 = getQueryCacheParam("select sum(v1) from db1.part1 where dt <= '2024-04-20'");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam2.digest);
        Assertions.assertEquals(32, queryCacheParam1.digest.remaining());
        Assertions.assertNotEquals(
                Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                Lists.newArrayList(queryCacheParam2.tablet_to_range.values())
        );

        Assertions.assertEquals(
                queryCacheParam1.tablet_to_range.values().stream().sorted().collect(Collectors.toList()),
                ImmutableList.of(
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-05-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-05-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-05-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-05-01]; ..types: [DATEV2]; keys: [2024-06-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-05-01]; ..types: [DATEV2]; keys: [2024-06-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-05-01]; ..types: [DATEV2]; keys: [2024-06-01]; )]"
                )
        );

        Assertions.assertEquals(
                queryCacheParam2.tablet_to_range.values().stream().sorted().collect(Collectors.toList()),
                ImmutableList.of(
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-04-21]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-04-21]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-04-21]; )]"
                )
        );

        TQueryCacheParam queryCacheParam3 = getQueryCacheParam(
                "select sum(v1), sum(v1 + 1), sum(v2), sum(v2 + 2) + 2 from db1.part1"
        );
        TQueryCacheParam queryCacheParam4 = getQueryCacheParam(
                "select sum(v2 + 2) + 2, sum(v2), sum(v1 + 1), sum(v1) from db1.part1"
        );
        Assertions.assertEquals(queryCacheParam3.digest, queryCacheParam4.digest);
        Assertions.assertEquals(32, queryCacheParam3.digest.remaining());
        Assertions.assertEquals(queryCacheParam3.tablet_to_range, queryCacheParam4.tablet_to_range);

        TQueryCacheParam queryCacheParam5 = getQueryCacheParam(
                "select sum(v1) from db1.part1 where dt between '2024-04-15' and '2024-04-20' and k1 = 1"
        );
        TQueryCacheParam queryCacheParam6 = getQueryCacheParam(
                "select sum(v1) from db1.part1 where dt between '2024-04-15' and '2024-04-20' and k1 = 2"
        );
        Assertions.assertNotEquals(queryCacheParam5.digest, queryCacheParam6.digest);
        Assertions.assertEquals(32, queryCacheParam5.digest.remaining());
        Assertions.assertEquals(32, queryCacheParam6.digest.remaining());
        Assertions.assertEquals(queryCacheParam5.tablet_to_range, queryCacheParam6.tablet_to_range);
    }

    @Test
    public void testSelectFromGroupBy() {
        phaseAgg(2, () -> {
            TQueryCacheParam queryCacheParam1 = getQueryCacheParam("select k1, sum(v1) from db1.part1 group by k1");
            TQueryCacheParam queryCacheParam2 = getQueryCacheParam("select k1, sum(v1) from db1.part1 group by k1 limit 10");
            Assertions.assertNotEquals(queryCacheParam1.digest, queryCacheParam2.digest);
            Assertions.assertEquals(32, queryCacheParam1.digest.remaining());
            Assertions.assertEquals(32, queryCacheParam2.digest.remaining());

            TQueryCacheParam queryCacheParam3 = getQueryCacheParam("select sum(v1), k1 from db1.part1 group by k1");
            Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam3.digest);
            Assertions.assertEquals(
                    Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                    Lists.newArrayList(queryCacheParam3.tablet_to_range.values())
            );

            List<TNormalizedPlanNode> plans1 = normalizePlans("select k1, sum(v1) from db1.part1 group by k1");
            List<TNormalizedPlanNode> plans2 = normalizePlans(
                    "select sum(v1), k1 from db1.part1 where dt between '2024-04-10' and '2024-05-06' group by k1");
            Assertions.assertEquals(plans1, plans2);
            return null;
        });
    }

    @Test
    public void phasesAgg() {
        List<TNormalizedPlanNode> onePhaseAggPlans = phaseAgg(1, () -> normalizePlans(
                "select sum(v1), k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        List<TNormalizedPlanNode> onePhaseAggPlans2 = phaseAgg(1, () -> normalizePlans(
                "select k1, sum(v1) from db1.part1 where dt = '2024-04-10' group by k1"));
        Assertions.assertEquals(onePhaseAggPlans, onePhaseAggPlans2);

        List<TNormalizedPlanNode> twoPhaseAggPlans = phaseAgg(2, () -> normalizePlans(
                "select sum(v1), k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        Assertions.assertNotEquals(onePhaseAggPlans, twoPhaseAggPlans);
    }

    @Test
    public void phasesDistinctAgg() {
        List<TNormalizedPlanNode> noDistinctPlans = phaseAgg(1, () -> normalizePlans(
                "select k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        List<TNormalizedPlanNode> onePhaseAggPlans = phaseAgg(1, () -> normalizePlans(
                "select distinct k1 from db1.part1 where dt = '2024-04-10'"));
        Assertions.assertEquals(noDistinctPlans, onePhaseAggPlans);
        List<TNormalizedPlanNode> twoPhaseAggPlans = phaseAgg(2, () -> normalizePlans(
                "select distinct k1 from db1.part1 where dt = '2024-04-10'"));
        Assertions.assertNotEquals(onePhaseAggPlans, twoPhaseAggPlans);

        List<TNormalizedPlanNode> threePhaseAggPlans = phaseAgg(3, () -> normalizePlans(
                "select sum(distinct v1), k2 from db1.part1 where dt = '2024-04-10' group by k2"));
        List<TNormalizedPlanNode> fourPhaseAggPlans = phaseAgg(4, () -> normalizePlans(
                "select sum(distinct v1), k2 from db1.part1 where dt = '2024-04-10' group by k2"));
        Assertions.assertEquals(fourPhaseAggPlans, threePhaseAggPlans);
    }

    @Test
    public void testVariantSubColumnDigest() throws Exception {
        // Different variant subcolumns should produce different digests
        String digest1 = getDigest(
                "select cast(data['int_1'] as int), count(*) from db1.variant_tbl group by cast(data['int_1'] as int)");
        String digest2 = getDigest(
                "select cast(data['int_nested'] as int), count(*) from db1.variant_tbl group by cast(data['int_nested'] as int)");
        Assertions.assertNotEquals(digest1, digest2,
                "Queries on different variant subcolumns must have different cache digests");

        // Same variant subcolumn with different aliases should produce same digest
        String digest3 = getDigest(
                "select cast(data['int_1'] as int) as a, count(*) as cnt from db1.variant_tbl group by cast(data['int_1'] as int)");
        Assertions.assertEquals(digest1, digest3);
    }

    @Test
    public void testAllowIncremental() throws Exception {
        // Switch off (the default): never allow incremental merge.
        Assertions.assertFalse(connectContext.getSessionVariable().getEnableQueryCacheIncremental());
        TQueryCacheParam withoutSwitch = getQueryCacheParam(
                "select k2, sum(v1) as v from db1.part1 group by k2");
        Assertions.assertFalse(withoutSwitch.allow_incremental);

        connectContext.getSessionVariable().setEnableQueryCacheIncremental(true);
        try {
            // DUP_KEYS with a two-phase aggregation (group by a non-distribution
            // column): the cache point is the non-finalize first phase, whose
            // output is merged again upstream, so incremental merge is safe.
            TQueryCacheParam dupTwoPhase = getQueryCacheParam(
                    "select k2, sum(v1) as v from db1.part1 group by k2");
            Assertions.assertTrue(dupTwoPhase.allow_incremental);

            // One-phase aggregation finalizes inside the cached fragment: its
            // output has no downstream merge, so emitting cached and delta
            // blocks side by side would duplicate group keys.
            TQueryCacheParam onePhase = phaseAgg(1, () -> getQueryCacheParam(
                    "select k1, sum(v1) as v from db1.non_part group by k1"));
            Assertions.assertFalse(onePhase.allow_incremental);

            // UNIQUE merge-on-write is append-only as long as loads do not
            // touch pre-existing keys; FE grants the capability and BE checks
            // the delete bitmap of the delta window per tablet. Group by a
            // non-distribution column so the aggregation stays two-phase and
            // the decision reaches the keys-type check.
            TQueryCacheParam uniqueMow = getQueryCacheParam(
                    "select v1, count(*) as v from db1.uniq_mow group by v1");
            Assertions.assertTrue(uniqueMow.allow_incremental);

            // UNIQUE merge-on-read resolves duplicates by merging across
            // rowsets at read time: a delta-only scan cannot stand alone.
            TQueryCacheParam uniqueMor = getQueryCacheParam(
                    "select v1, count(*) as v from db1.uniq_mor group by v1");
            Assertions.assertFalse(uniqueMor.allow_incremental);

            // AGG_KEYS merges rows inside the storage layer.
            TQueryCacheParam aggKeys = getQueryCacheParam(
                    "select v1, count(*) as v from db1.agg_tbl group by v1");
            Assertions.assertFalse(aggKeys.allow_incremental);

            // Agg over Agg inside one fragment (distinct aggregation grouped by
            // the distribution column): the cache point is not directly on the
            // scan (and finalizes here as well), so incremental is not allowed.
            TQueryCacheParam distinctAgg = getQueryCacheParam(
                    "select k1, count(distinct k2) from db1.non_part group by k1");
            Assertions.assertFalse(distinctAgg.allow_incremental);

            // Nested cache point: a non-finalize partial agg over a finalized
            // colocate agg over scan. The inner agg would see only the delta
            // rows during an incremental run, so its finalized output is not a
            // mergeable complement of the cached snapshot -- must be rejected
            // even though the cache point itself does not finalize.
            TQueryCacheParam nestedAgg = getQueryCacheParam(
                    "select cnt, count(*) from"
                            + " (select k1, count(*) cnt from db1.non_part group by k1) x"
                            + " group by cnt");
            Assertions.assertFalse(nestedAgg.allow_incremental);
        } finally {
            connectContext.getSessionVariable().setEnableQueryCacheIncremental(false);
        }
    }

    @Test
    public void testListDiscretePartition() throws Throwable {
        // discrete list partition: equivalent predicates on the partition column are extracted
        // out of the digest and encoded into the tablet_to_range, so they share the digest.
        TQueryCacheParam noFilter = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_region group by k1");
        TQueryCacheParam eq = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_region where region = 'us' group by k1");
        TQueryCacheParam in = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_region where region in ('us') group by k1");
        Assertions.assertEquals(noFilter.digest, eq.digest);
        Assertions.assertEquals(eq.digest, in.digest);
        Assertions.assertEquals(eq.tablet_to_range, in.tablet_to_range);

        // a different discrete value keeps the same digest but produces a different range.
        TQueryCacheParam eu = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_region where region = 'eu' group by k1");
        Assertions.assertEquals(eq.digest, eu.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(eq.tablet_to_range.values()),
                Lists.newArrayList(eu.tablet_to_range.values()));

        // a predicate on the non partition column is not extracted and stays in the digest.
        TQueryCacheParam nonPart1 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_region where k1 = 1 group by k1");
        TQueryCacheParam nonPart2 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_region where k1 = 2 group by k1");
        Assertions.assertNotEquals(noFilter.digest, nonPart1.digest);
        Assertions.assertNotEquals(nonPart1.digest, nonPart2.digest);
    }

    @Test
    public void testListDateTruncPartition() throws Throwable {
        // date_trunc list partition: the boundary maps to a closed-open range on the base
        // column. `<= last second` and `< next boundary` are equivalent after closed-open
        // normalization, so they share both the digest and the range.
        TQueryCacheParam le = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_month "
                        + "where dt >= '2024-03-15 00:00:00' and dt <= '2024-03-31 23:59:59' group by k1");
        TQueryCacheParam lt = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_month "
                        + "where dt >= '2024-03-15 00:00:00' and dt < '2024-04-01 00:00:00' group by k1");
        Assertions.assertEquals(le.digest, lt.digest);
        Assertions.assertEquals(le.tablet_to_range, lt.tablet_to_range);

        // a narrower predicate on the same partition shares the digest but narrows the range.
        TQueryCacheParam narrower = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_month "
                        + "where dt >= '2024-03-20 00:00:00' and dt < '2024-04-01 00:00:00' group by k1");
        Assertions.assertEquals(le.digest, narrower.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(le.tablet_to_range.values()),
                Lists.newArrayList(narrower.tablet_to_range.values()));
    }

    @Test
    public void testListMultiColumnPartition() throws Throwable {
        // multi-column list partition with a date_trunc expression and a discrete column:
        // equivalent predicates on both columns share the digest and the range.
        TQueryCacheParam q1 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_multi "
                        + "where region = 'us' and dt >= '2024-03-15 00:00:00' "
                        + "and dt < '2024-04-01 00:00:00' group by k1");
        TQueryCacheParam q2 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_multi "
                        + "where region in ('us') and dt >= '2024-03-15 00:00:00' "
                        + "and dt <= '2024-03-31 23:59:59' group by k1");
        Assertions.assertEquals(q1.digest, q2.digest);
        Assertions.assertEquals(q1.tablet_to_range, q2.tablet_to_range);

        // filtering a different discrete value selects a different tuple and changes the
        // range while the digest stays the same.
        TQueryCacheParam q3 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_multi "
                        + "where region = 'eu' and dt >= '2024-03-15 00:00:00' "
                        + "and dt < '2024-04-01 00:00:00' group by k1");
        Assertions.assertEquals(q1.digest, q3.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(q1.tablet_to_range.values()),
                Lists.newArrayList(q3.tablet_to_range.values()));

        // a predicate only on the non partition column stays in the digest.
        TQueryCacheParam q4 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_multi where k1 = 1 group by k1");
        TQueryCacheParam q5 = getQueryCacheParam(
                "select k1, sum(v1) as v from db1.list_multi where k1 = 2 group by k1");
        Assertions.assertNotEquals(q4.digest, q5.digest);
    }

    private String getDigest(String sql) throws Exception {
        return Hex.encodeHexString(getQueryCacheParam(sql).digest);
    }

    private TQueryCacheParam getQueryCacheParam(String sql) throws Exception {
        List<TQueryCacheParam> queryCaches = normalize(sql);
        Assertions.assertEquals(1, queryCaches.size());
        return queryCaches.get(0);
    }

    private List<TQueryCacheParam> normalize(String sql) throws Exception {
        Planner planner = getSqlStmtExecutor(sql).planner();
        DescriptorTable descTable = planner.getDescTable();
        List<PlanFragment> fragments = planner.getFragments();
        List<TQueryCacheParam> queryCacheParams = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
            Optional<TQueryCacheParam> queryCacheParam = normalizer.normalize(connectContext);
            if (queryCacheParam.isPresent()) {
                queryCacheParams.add(queryCacheParam.get());
            }
        }
        return queryCacheParams;
    }

    private List<TNormalizedPlanNode> normalizePlans(String sql) throws Exception {
        Planner planner = getSqlStmtExecutor(sql).planner();
        DescriptorTable descTable = planner.getDescTable();
        List<PlanFragment> fragments = planner.getFragments();
        List<TNormalizedPlanNode> normalizedPlans = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
            normalizedPlans.addAll(normalizer.normalizePlans(connectContext));
        }
        return normalizedPlans;
    }

    private <T> T phaseAgg(int phase, ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable().setAggPhase(phase);
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable().setAggPhase(0);
        }
    }

    private interface Callback {
        void run() throws Throwable;
    }

    private interface ResultCallback<T> {
        T run() throws Throwable;
    }
}
