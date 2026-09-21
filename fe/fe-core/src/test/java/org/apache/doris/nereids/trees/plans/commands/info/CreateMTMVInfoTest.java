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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateMTMVCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateMTMVInfoTest extends TestWithFeService {
    private static final String BASE_TABLE = "mtmv_list_expr_alias_base";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE " + BASE_TABLE + " ("
                + "id INT NOT NULL, event_time DATETIME NOT NULL) "
                + "DUPLICATE KEY(id) "
                + "AUTO PARTITION BY LIST (date_trunc(event_time, 'day')) () "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void testFollowPartitionExpressionUsesMvOutputAlias() throws Exception {
        CreateMTMVInfo createMTMVInfo = analyzeMv(
                "CREATE MATERIALIZED VIEW mtmv_follow_alias "
                        + "BUILD DEFERRED REFRESH AUTO ON MANUAL "
                        + "PARTITION BY (event_alias) "
                        + "DISTRIBUTED BY RANDOM BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1') "
                        + "AS SELECT id, event_time AS event_alias FROM " + BASE_TABLE);

        assertPhysicalPartitionExpr(createMTMVInfo, "event_alias", "day");
        assertBaseLineageAndMetadata(createMTMVInfo);
    }

    @Test
    public void testExplicitRollupPartitionExpressionUsesMvOutputAlias() throws Exception {
        CreateMTMVInfo createMTMVInfo = analyzeMv(
                "CREATE MATERIALIZED VIEW mtmv_rollup_alias "
                        + "BUILD DEFERRED REFRESH AUTO ON MANUAL "
                        + "PARTITION BY (date_trunc(event_alias, 'month')) "
                        + "DISTRIBUTED BY RANDOM BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1') "
                        + "AS SELECT id, event_time AS event_alias FROM " + BASE_TABLE);

        Expr lineageExpr = createMTMVInfo.getMvPartitionInfo().getExpr();
        assertPhysicalPartitionExpr(createMTMVInfo, "event_alias", "month");
        Assertions.assertEquals("event_time", ((SlotRef) lineageExpr.getChild(0)).getColumnName());
        Assertions.assertNotSame(lineageExpr, createMTMVInfo.getPartitionDesc().getPartitionExprs().get(0));
        assertBaseLineageAndMetadata(createMTMVInfo);
    }

    @Test
    public void testSameNamePartitionExpressionStillUsesMvColumn() throws Exception {
        CreateMTMVInfo createMTMVInfo = analyzeMv(
                "CREATE MATERIALIZED VIEW mtmv_same_name "
                        + "BUILD DEFERRED REFRESH AUTO ON MANUAL "
                        + "PARTITION BY (event_time) "
                        + "DISTRIBUTED BY RANDOM BUCKETS 1 "
                        + "PROPERTIES ('replication_num' = '1') "
                        + "AS SELECT id, event_time FROM " + BASE_TABLE);

        assertPhysicalPartitionExpr(createMTMVInfo, "event_time", "day");
        assertBaseLineageAndMetadata(createMTMVInfo);
    }

    private CreateMTMVInfo analyzeMv(String sql) throws Exception {
        createStatementCtx(sql);
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertInstanceOf(CreateMTMVCommand.class, plan);
        CreateMTMVInfo createMTMVInfo = ((CreateMTMVCommand) plan).getCreateMTMVInfo();
        createMTMVInfo.analyze(connectContext);
        return createMTMVInfo;
    }

    private void assertPhysicalPartitionExpr(
            CreateMTMVInfo createMTMVInfo, String expectedColumn, String expectedTimeUnit) {
        Expr partitionExpr = createMTMVInfo.getPartitionDesc().getPartitionExprs().get(0);
        Assertions.assertInstanceOf(FunctionCallExpr.class, partitionExpr);
        Assertions.assertInstanceOf(SlotRef.class, partitionExpr.getChild(0));
        Assertions.assertEquals(expectedColumn, ((SlotRef) partitionExpr.getChild(0)).getColumnName());
        Assertions.assertEquals(expectedTimeUnit, ((StringLiteral) partitionExpr.getChild(1)).getStringValue());
    }

    private void assertBaseLineageAndMetadata(CreateMTMVInfo createMTMVInfo) throws Exception {
        Assertions.assertEquals("event_time",
                createMTMVInfo.getMvPartitionInfo().getPctInfos().get(0).getColName());
        OlapTable baseTable = (OlapTable) Env.getCurrentEnv().getInternalCatalog()
                .getDbOrAnalysisException("test").getTableOrAnalysisException(BASE_TABLE);
        Expr basePartitionExpr = baseTable.getPartitionInfo().getPartitionExprs().get(0);
        Assertions.assertEquals("event_time", ((SlotRef) basePartitionExpr.getChild(0)).getColumnName());
    }
}
