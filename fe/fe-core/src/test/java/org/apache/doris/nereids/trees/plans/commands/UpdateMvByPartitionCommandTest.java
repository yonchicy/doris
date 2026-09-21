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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

class UpdateMvByPartitionCommandTest {
    @Test
    void testFirstPartWithoutLowerBound() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)),
                ImmutableList.of(column));
        Range<PartitionKey> range1 = Range.lessThan(upper);
        RangePartitionItem item1 = new RangePartitionItem(range1);

        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(item1), "s");
        Assertions.assertEquals("OR[(s < 1),s IS NULL]", predicates.iterator().next().toSql());

    }

    @Test
    void testMaxMin() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(PartitionValue.MAX_VALUE),
                ImmutableList.of(column));
        PartitionKey lower = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)),
                ImmutableList.of(column));
        Range<PartitionKey> range = Range.closedOpen(lower, upper);
        RangePartitionItem rangePartitionItem = new RangePartitionItem(range);
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(rangePartitionItem),
                "s");
        Expression expr = predicates.iterator().next();
        System.out.println(expr.toSql());
        Assertions.assertEquals("(s >= 1)", expr.toSql());
    }

    @Test
    void testNull() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey v = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem), "s")
                .iterator().next();
        Assertions.assertTrue(expr instanceof IsNull);

        PartitionKey v1 = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        PartitionKey v2 = PartitionKey.createListPartitionKeyWithTypes(ImmutableList.of(new PartitionValue("1", false)),
                ImmutableList.of(column.getType()), false);
        listPartitionItem = new ListPartitionItem(ImmutableList.of(v1, v2));
        expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem), "s").iterator()
                .next();
        Assertions.assertEquals("OR[s IS NULL,s IN (1)]", expr.toSql());
    }

    @Test
    void testRefreshListRollupUsesMvMonthExprInsteadOfBaseDayExpr() throws AnalysisException {
        ListPartitionItem mvMonthItem = dateListPartitionItem("2024-01-01");
        MTMV mv = mockMvWithListPartition(
                "p_202401", mvMonthItem, dateTruncMonthExpr(), dateTruncDayExpr());
        OlapTable baseTable = mockBaseListTable(dateTruncDayExpr());

        Map<TableIf, Set<Expression>> predicates = UpdateMvByPartitionCommand.constructTableWithPredicates(
                mv, Sets.newHashSet("p_202401"), ImmutableMap.of(baseTable, "s"));

        Assertions.assertEquals("AND[(s >= '2024-01-01'),(s < '2024-02-01')]",
                predicates.get(baseTable).iterator().next().toSql());
    }

    @Test
    void testRefreshPlainListRollupUsesMvMonthExprInsteadOfInPredicate() throws AnalysisException {
        ListPartitionItem mvMonthItem = dateListPartitionItem("2024-01-01");
        MTMV mv = mockMvWithListPartition(
                "p_202401", mvMonthItem, dateTruncMonthExpr(), dateTruncMonthExpr());
        OlapTable baseTable = mockBaseListTable(new SlotRef(null, "s"));

        Map<TableIf, Set<Expression>> predicates = UpdateMvByPartitionCommand.constructTableWithPredicates(
                mv, Sets.newHashSet("p_202401"), ImmutableMap.of(baseTable, "s"));

        Assertions.assertEquals("AND[(s >= '2024-01-01'),(s < '2024-02-01')]",
                predicates.get(baseTable).iterator().next().toSql());
    }

    @Test
    void testRefreshFollowListExprUsesPhysicalMvExpr() throws AnalysisException {
        ListPartitionItem mvDayItem = dateListPartitionItem("2024-01-15");
        MTMV mv = mockMvWithListPartition("p_20240115", mvDayItem, null, dateTruncDayExpr());
        OlapTable baseTable = mockBaseListTable(dateTruncDayExpr());

        Map<TableIf, Set<Expression>> predicates = UpdateMvByPartitionCommand.constructTableWithPredicates(
                mv, Sets.newHashSet("p_20240115"), ImmutableMap.of(baseTable, "s"));

        Assertions.assertEquals("AND[(s >= '2024-01-15'),(s < '2024-01-16')]",
                predicates.get(baseTable).iterator().next().toSql());
    }

    @Test
    void testListPartitionExprDateTrunc() throws AnalysisException {
        Column column = new Column("s", PrimitiveType.DATE);
        PartitionKey v = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("2024-01-01")), ImmutableList.of(column));
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem),
                new UnboundSlot("s"), Optional.of(dateTruncMonthExpr()), 0).iterator().next();
        Assertions.assertEquals("AND[(s >= '2024-01-01'),(s < '2024-02-01')]", expr.toSql());
    }

    @Test
    void testListPartitionExprDateTruncMultiValues() throws AnalysisException {
        Column column = new Column("s", PrimitiveType.DATE);
        PartitionKey v1 = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("2024-01-01")), ImmutableList.of(column));
        PartitionKey v2 = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("2024-03-01")), ImmutableList.of(column));
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v1, v2));
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem),
                new UnboundSlot("s"), Optional.of(dateTruncMonthExpr()), 0).iterator().next();
        Assertions.assertEquals(
                "OR[AND[(s >= '2024-01-01'),(s < '2024-02-01')],AND[(s >= '2024-03-01'),(s < '2024-04-01')]]",
                expr.toSql());
    }

    @Test
    void testListPartitionExprDateTruncNull() throws AnalysisException {
        Column column = new Column("s", PrimitiveType.DATE);
        PartitionKey v = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem),
                new UnboundSlot("s"), Optional.of(dateTruncMonthExpr()), 0).iterator().next();
        Assertions.assertTrue(expr instanceof IsNull);
    }

    @Test
    void testBasePartitionItemsUseBaseDayExprAndKeyIndex() throws AnalysisException {
        Column col0 = new Column("a", PrimitiveType.INT);
        Column col1 = new Column("s", PrimitiveType.DATE);
        PartitionKey key = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("1"), new PartitionValue("2024-01-15")),
                ImmutableList.of(col0, col1));
        ListPartitionItem baseDayItem = new ListPartitionItem(ImmutableList.of(key));

        Expression expr = UpdateMvByPartitionCommand.constructPredicates(
                Sets.newHashSet(baseDayItem), new UnboundSlot("s"), Optional.of(dateTruncDayExpr()), 1)
                .iterator().next();

        Assertions.assertEquals("AND[(s >= '2024-01-15'),(s < '2024-01-16')]", expr.toSql());
    }

    @Test
    void testListPartitionExprDateTruncKeyIndex() throws AnalysisException {
        Column col0 = new Column("a", PrimitiveType.INT);
        Column col1 = new Column("s", PrimitiveType.DATE);
        PartitionKey v = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("1"), new PartitionValue("2024-01-01")),
                ImmutableList.of(col0, col1));
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        // the pct column is the second list partition column
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem),
                new UnboundSlot("s"), Optional.of(dateTruncMonthExpr()), 1).iterator().next();
        Assertions.assertEquals("AND[(s >= '2024-01-01'),(s < '2024-02-01')]", expr.toSql());
    }

    @Test
    void testListPartitionExprSlotRefKeepsIn() throws AnalysisException {
        Column column = new Column("s", PrimitiveType.INT);
        PartitionKey v1 = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("1")), ImmutableList.of(column));
        PartitionKey v2 = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue("2")), ImmutableList.of(column));
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v1, v2));
        // ordinary list column: the partition expression is a slot ref, keep the IN predicate
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem),
                new UnboundSlot("s"), Optional.of(new SlotRef(null, "s")), 0).iterator().next();
        Assertions.assertEquals("s IN (1, 2)", expr.toSql());
    }

    private static MTMV mockMvWithListPartition(String partitionName, ListPartitionItem partitionItem,
            Expr rollupExpr, Expr physicalPartitionExpr) throws AnalysisException {
        MTMV mv = Mockito.mock(MTMV.class);
        MTMVPartitionInfo mvPartitionInfo = Mockito.mock(MTMVPartitionInfo.class);
        ListPartitionInfo partitionInfo = Mockito.mock(ListPartitionInfo.class);
        Mockito.when(mv.getMvPartitionInfo()).thenReturn(mvPartitionInfo);
        Mockito.when(mvPartitionInfo.getExpr()).thenReturn(rollupExpr);
        Mockito.when(mvPartitionInfo.getPartitionCol()).thenReturn("s");
        Mockito.when(mv.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getPartitionExprs()).thenReturn(Lists.newArrayList(physicalPartitionExpr));
        Mockito.when(partitionInfo.getPartitionColumns())
                .thenReturn(ImmutableList.of(new Column("s", PrimitiveType.DATE)));
        Mockito.when(mv.getPartitionItemOrAnalysisException(partitionName)).thenReturn(partitionItem);
        return mv;
    }

    private static OlapTable mockBaseListTable(Expr partitionExpr) {
        OlapTable table = Mockito.mock(OlapTable.class);
        ListPartitionInfo partitionInfo = Mockito.mock(ListPartitionInfo.class);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionInfo.getPartitionExprs()).thenReturn(Lists.newArrayList(partitionExpr));
        Mockito.when(partitionInfo.getPartitionColumns())
                .thenReturn(ImmutableList.of(new Column("s", PrimitiveType.DATE)));
        return table;
    }

    private static ListPartitionItem dateListPartitionItem(String value) throws AnalysisException {
        Column column = new Column("s", PrimitiveType.DATE);
        PartitionKey key = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue(value)), ImmutableList.of(column));
        return new ListPartitionItem(ImmutableList.of(key));
    }

    private static Expr dateTruncDayExpr() {
        return dateTruncExpr("day");
    }

    private static Expr dateTruncMonthExpr() {
        return dateTruncExpr("month");
    }

    private static Expr dateTruncExpr(String timeUnit) {
        return new FunctionCallExpr("date_trunc",
                ImmutableList.of(new SlotRef(null, "s"), new StringLiteral(timeUnit)), false);
    }
}
