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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Optional;

public class MTMVPartitionExprDateTruncTest {

    private FunctionCallExpr buildDateTruncExpr(String timeUnit) {
        return new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, "dt"), new StringLiteral(timeUnit)), true);
    }

    private MTMVPartitionInfo buildMvPartitionInfo(MTMVRelatedTableIf relatedTable) {
        MTMVPartitionInfo mvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        BaseTableInfo baseTableInfo = Mockito.mock(BaseTableInfo.class);
        BaseColInfo baseColInfo = new BaseColInfo("dt", baseTableInfo);
        Mockito.when(mvPartitionInfo.getPctInfos()).thenReturn(Lists.newArrayList(baseColInfo));
        try {
            Mockito.when(mvPartitionInfo.getPctColPos(relatedTable)).thenReturn(0);
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
        return mvPartitionInfo;
    }

    private OlapTable buildOlapTableWithListExprPartition(String baseTimeUnit) {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        ListPartitionInfo partitionInfo = Mockito.mock(ListPartitionInfo.class);
        Mockito.when(olapTable.getPartitionInfo()).thenReturn(partitionInfo);
        try {
            Mockito.when(olapTable.getPartitionType(Mockito.<Optional<MvccSnapshot>>any()))
                    .thenReturn(PartitionType.LIST);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ArrayList<org.apache.doris.analysis.Expr> partitionExprs = Lists.newArrayList();
        if (baseTimeUnit != null) {
            partitionExprs.add(buildDateTruncExpr(baseTimeUnit));
        } else {
            partitionExprs.add(new SlotRef(null, "dt"));
        }
        Mockito.when(partitionInfo.getPartitionExprs()).thenReturn(partitionExprs);
        return olapTable;
    }

    @Test
    public void testListPartitionGranularityCoarser() throws AnalysisException {
        // base: day, mv: month -> should pass
        OlapTable baseTable = buildOlapTableWithListExprPartition("day");
        MTMVPartitionInfo mvPartitionInfo = buildMvPartitionInfo(baseTable);

        try (MockedStatic<MTMVUtil> mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtilStatic =
                        Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtilStatic.when(() -> MTMVUtil.getRelatedTable(Mockito.any()))
                    .thenReturn(baseTable);
            partitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.any(), Mockito.anyString())).thenReturn(Type.DATETIME);

            MTMVPartitionExprDateTrunc service = new MTMVPartitionExprDateTrunc(
                    buildDateTruncExpr("month"));
            service.analyze(mvPartitionInfo);
        }
    }

    @Test
    public void testListPartitionGranularitySame() throws AnalysisException {
        // base: day, mv: day -> should pass
        OlapTable baseTable = buildOlapTableWithListExprPartition("day");
        MTMVPartitionInfo mvPartitionInfo = buildMvPartitionInfo(baseTable);

        try (MockedStatic<MTMVUtil> mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtilStatic =
                        Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtilStatic.when(() -> MTMVUtil.getRelatedTable(Mockito.any()))
                    .thenReturn(baseTable);
            partitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.any(), Mockito.anyString())).thenReturn(Type.DATETIME);

            MTMVPartitionExprDateTrunc service = new MTMVPartitionExprDateTrunc(
                    buildDateTruncExpr("day"));
            service.analyze(mvPartitionInfo);
        }
    }

    @Test
    public void testListPartitionGranularityFiner() throws AnalysisException {
        // base: day, mv: hour -> should fail
        OlapTable baseTable = buildOlapTableWithListExprPartition("day");
        MTMVPartitionInfo mvPartitionInfo = buildMvPartitionInfo(baseTable);

        try (MockedStatic<MTMVUtil> mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtilStatic =
                        Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtilStatic.when(() -> MTMVUtil.getRelatedTable(Mockito.any()))
                    .thenReturn(baseTable);
            partitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.any(), Mockito.anyString())).thenReturn(Type.DATETIME);

            MTMVPartitionExprDateTrunc service = new MTMVPartitionExprDateTrunc(
                    buildDateTruncExpr("hour"));
            try {
                service.analyze(mvPartitionInfo);
                Assert.fail("Should throw AnalysisException when mv granularity is finer than base table");
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("must not be finer than"));
            }
        }
    }

    @Test
    public void testListPartitionNonExprColumn() throws AnalysisException {
        // plain list partition (no date_trunc expression) -> no granularity check, should pass
        OlapTable baseTable = buildOlapTableWithListExprPartition(null);
        MTMVPartitionInfo mvPartitionInfo = buildMvPartitionInfo(baseTable);

        try (MockedStatic<MTMVUtil> mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtilStatic =
                        Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtilStatic.when(() -> MTMVUtil.getRelatedTable(Mockito.any()))
                    .thenReturn(baseTable);
            partitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.any(), Mockito.anyString())).thenReturn(Type.DATETIME);

            MTMVPartitionExprDateTrunc service = new MTMVPartitionExprDateTrunc(
                    buildDateTruncExpr("hour"));
            service.analyze(mvPartitionInfo);
        }
    }

    @Test
    public void testRangePartitionStillSupported() throws AnalysisException {
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        try {
            Mockito.when(baseTable.getPartitionType(Mockito.<Optional<MvccSnapshot>>any()))
                    .thenReturn(PartitionType.RANGE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MTMVPartitionInfo mvPartitionInfo = buildMvPartitionInfo(baseTable);

        try (MockedStatic<MTMVUtil> mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtilStatic =
                        Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtilStatic.when(() -> MTMVUtil.getRelatedTable(Mockito.any()))
                    .thenReturn(baseTable);
            partitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.any(), Mockito.anyString())).thenReturn(Type.DATETIME);

            MTMVPartitionExprDateTrunc service = new MTMVPartitionExprDateTrunc(
                    buildDateTruncExpr("month"));
            service.analyze(mvPartitionInfo);
        }
    }

    @Test
    public void testUnsupportedPartitionType() throws AnalysisException {
        OlapTable baseTable = Mockito.mock(OlapTable.class);
        try {
            Mockito.when(baseTable.getPartitionType(Mockito.<Optional<MvccSnapshot>>any()))
                    .thenReturn(PartitionType.UNPARTITIONED);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        MTMVPartitionInfo mvPartitionInfo = buildMvPartitionInfo(baseTable);

        try (MockedStatic<MTMVUtil> mtmvUtilStatic = Mockito.mockStatic(MTMVUtil.class);
                MockedStatic<MTMVPartitionUtil> partitionUtilStatic =
                        Mockito.mockStatic(MTMVPartitionUtil.class)) {
            mtmvUtilStatic.when(() -> MTMVUtil.getRelatedTable(Mockito.any()))
                    .thenReturn(baseTable);
            partitionUtilStatic.when(() -> MTMVPartitionUtil.getPartitionColumnType(
                    Mockito.any(), Mockito.anyString())).thenReturn(Type.DATETIME);

            MTMVPartitionExprDateTrunc service = new MTMVPartitionExprDateTrunc(
                    buildDateTruncExpr("month"));
            try {
                service.analyze(mvPartitionInfo);
                Assert.fail("Should throw AnalysisException for unsupported partition type");
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("range/list"));
            }
        }
    }
}
