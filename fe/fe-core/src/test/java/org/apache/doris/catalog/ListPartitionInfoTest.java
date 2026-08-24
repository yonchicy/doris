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

package org.apache.doris.catalog;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DateLiteralUtils;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.ListPartitionDesc;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SinglePartitionDesc;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ListPartitionInfoTest {
    private List<Column> partitionColumns;
    private ListPartitionInfo partitionInfo;

    private List<SinglePartitionDesc> singlePartitionDescs;

    @Before
    public void setUp() {
        partitionColumns = new LinkedList<>();
        singlePartitionDescs = new LinkedList<>();
    }

    @Test
    public void testTinyInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.TINYINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-128")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-128", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());

    }

    @Test
    public void testSmallInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.SMALLINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-32768")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-32768", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testInt() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-2147483648")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-2147483648", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testBigInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-9223372036854775808")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-9223372036854775808", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testLargeInt() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.LARGEINT), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("-170141183460469231731687303715884105728")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("-170141183460469231731687303715884105728", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testString() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.CHAR), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("Beijing")));
        inValues.add(Lists.newArrayList(new PartitionValue("Shanghai")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals("Beijing", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
        Assert.assertEquals("Shanghai", ((ListPartitionItem) partitionItem).getItems().get(1).getKeys().get(0).getStringValue());
    }

    @Test
    public void testBoolean() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BOOLEAN), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("true")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));

        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
        Assert.assertEquals(true, ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getRealValue());
    }

    @Test(expected = DdlException.class)
    public void testDuplicateKey() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        partitionColumns.add(k1);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("beijing")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p2",
                PartitionKeyDesc.createIn(inValues), null));


        partitionInfo = new ListPartitionInfo(partitionColumns);
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(1, null);
            partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }
    }

    @Test
    public void testMultiPartitionKeys() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("beijing"), new PartitionValue("100")));
        singlePartitionDescs.add(new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null));


        partitionInfo = new ListPartitionInfo(partitionColumns);
        PartitionItem partitionItem = null;
        for (SinglePartitionDesc singlePartitionDesc : singlePartitionDescs) {
            singlePartitionDesc.analyze(2, null);
            partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
        }

        Assert.assertEquals("beijing", ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getRealValue());
        Assert.assertEquals(100, ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(1).getLongValue());
    }

    @Test
    public void testMultiAutotoSql() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        ArrayList<Expr> partitionExprs = new ArrayList<>();
        SlotRef s1 = new SlotRef(new TableNameInfo("tbl"), "k1");
        SlotRef s2 = new SlotRef(new TableNameInfo("tbl"), "k2");
        partitionExprs.add(s1);
        partitionExprs.add(s2);

        partitionInfo = new ListPartitionInfo(true, partitionExprs, partitionColumns);
        OlapTable table = new OlapTable();

        String sql = partitionInfo.toSql(table, null);

        String expected = "AUTO PARTITION BY LIST (`k1`, `k2`)";
        Assert.assertTrue("got: " + sql + ", should have: " + expected, sql.contains(expected));
    }

    @Test
    public void testDateTruncTimeUnits() throws AnalysisException {
        String[] timeUnits = new String[] {
                "year", "quarter", "month", "week", "day", "hour", "minute", "second"
        };
        for (String timeUnit : timeUnits) {
            FunctionCallExpr dateTrunc = createDateTruncExpr(timeUnit);
            Assert.assertEquals(timeUnit, PartitionExprUtil.getDateTruncTimeUnit(dateTrunc));
        }
        Assert.assertEquals("day", PartitionExprUtil.validateDateTruncTimeUnit("DAY"));
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> PartitionExprUtil.validateDateTruncTimeUnit("millisecond"));
        Assert.assertEquals("Unsupported date_trunc time unit: millisecond", exception.getMessage());
    }

    @Test
    public void testDateTruncRangeEnds() throws AnalysisException {
        assertDateTruncRangeEnd("year", "2026-01-01 00:00:00", "2027-01-01 00:00:00");
        assertDateTruncRangeEnd("quarter", "2026-07-01 00:00:00", "2026-10-01 00:00:00");
        assertDateTruncRangeEnd("month", "2026-07-01 00:00:00", "2026-08-01 00:00:00");
        assertDateTruncRangeEnd("week", "2026-07-20 00:00:00", "2026-07-27 00:00:00");
        assertDateTruncRangeEnd("DAY", "2026-07-23 00:00:00", "2026-07-24 00:00:00");
        assertDateTruncRangeEnd("hour", "2026-07-23 16:00:00", "2026-07-23 17:00:00");
        assertDateTruncRangeEnd("minute", "2026-07-23 16:45:00", "2026-07-23 16:46:00");
        assertDateTruncRangeEnd("second", "2026-07-23 16:45:30", "2026-07-23 16:45:31");
    }

    @Test
    public void testDateTruncPartitionValueBoundaries() throws AnalysisException, DdlException {
        assertDateTruncPartitionValueBoundary(
                "year", "2026-01-01 00:00:00", "2026-04-01 00:00:00", 0);
        assertDateTruncPartitionValueBoundary(
                "quarter", "2026-07-01 00:00:00", "2026-08-01 00:00:00", 0);
        assertDateTruncPartitionValueBoundary(
                "month", "2026-07-01 00:00:00", "2026-07-02 00:00:00", 0);
        assertDateTruncPartitionValueBoundary(
                "week", "2026-07-20 00:00:00", "2026-07-23 00:00:00", 0);
        assertDateTruncPartitionValueBoundary(
                "day", "2026-07-23 00:00:00", "2026-07-23 13:00:00", 0);
        assertDateTruncPartitionValueBoundary(
                "hour", "2026-07-23 16:00:00", "2026-07-23 16:30:00", 0);
        assertDateTruncPartitionValueBoundary(
                "minute", "2026-07-23 16:45:00", "2026-07-23 16:45:30", 0);
        assertDateTruncPartitionValueBoundary(
                "second", "2026-07-23 16:45:30.000000", "2026-07-23 16:45:30.123456", 6);
    }

    @Test
    public void testMixedPartitionExprBoundaryByPosition() throws AnalysisException, DdlException {
        PartitionItem partitionItem = addListPartitionValues(createMixedPartitionInfo(),
                "2026-07-23 00:00:00", "7", "2026-07-23 16:00:00");
        List<LiteralExpr> keys = ((ListPartitionItem) partitionItem).getItems().get(0).getKeys();
        Assert.assertEquals("2026-07-23 00:00:00", keys.get(0).getStringValue());
        Assert.assertEquals(7, keys.get(1).getLongValue());
        Assert.assertEquals("2026-07-23 16:00:00", keys.get(2).getStringValue());

        DdlException dayException = Assert.assertThrows(DdlException.class,
                () -> addListPartitionValues(createMixedPartitionInfo(),
                        "2026-07-23 13:00:00", "7", "2026-07-23 16:00:00"));
        Assert.assertTrue(dayException.getMessage(), dayException.getMessage().contains(
                "is not aligned with date_trunc(`day_key`, 'day')"));

        DdlException hourException = Assert.assertThrows(DdlException.class,
                () -> addListPartitionValues(createMixedPartitionInfo(),
                        "2026-07-23 00:00:00", "7", "2026-07-23 16:30:00"));
        Assert.assertTrue(hourException.getMessage(), hourException.getMessage().contains(
                "is not aligned with date_trunc(`hour_key`, 'hour')"));
    }

    @Test
    public void testPlainListAllowsDateTruncNonBoundary() throws AnalysisException, DdlException {
        Column k1 = new Column("k1", ScalarType.createDatetimeV2Type(0), true, null, "", "");
        partitionColumns.add(k1);
        partitionInfo = new ListPartitionInfo(partitionColumns);

        PartitionItem partitionItem = addListPartitionValue(partitionInfo, "2026-07-23 13:00:00");

        Assert.assertEquals("2026-07-23 13:00:00",
                ((ListPartitionItem) partitionItem).getItems().get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testManualExprSqlAndPartitionDescRemainManual() throws AnalysisException {
        partitionInfo = createDateTruncPartitionInfo("day");
        OlapTable table = new OlapTable();

        String sql = partitionInfo.toSql(table, null);
        ListPartitionDesc partitionDesc = (ListPartitionDesc) partitionInfo.toPartitionDesc(table);

        Assert.assertFalse(sql.startsWith("AUTO"));
        Assert.assertTrue(sql.contains("PARTITION BY LIST (date_trunc(`k1`, 'day'))"));
        Assert.assertFalse(partitionDesc.isAutoCreatePartitions());
        Assert.assertEquals("date_trunc(`k1`, 'day')", partitionDesc.getPartitionExprs().get(0)
                .accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
    }

    private ListPartitionInfo createDateTruncPartitionInfo(String timeUnit) throws AnalysisException {
        return createDateTruncPartitionInfo(timeUnit, 0);
    }

    private ListPartitionInfo createDateTruncPartitionInfo(String timeUnit, int scale) throws AnalysisException {
        Column k1 = new Column("k1", ScalarType.createDatetimeV2Type(scale), true, null, "", "");
        ArrayList<Expr> partitionExprs = new ArrayList<>();
        partitionExprs.add(createDateTruncExpr(timeUnit));
        return new ListPartitionInfo(false, partitionExprs, Lists.newArrayList(k1));
    }

    private void assertDateTruncPartitionValueBoundary(
            String timeUnit, String alignedValue, String nonAlignedValue, int scale)
            throws AnalysisException, DdlException {
        ListPartitionInfo alignedPartitionInfo = createDateTruncPartitionInfo(timeUnit, scale);
        Assert.assertNotNull(addListPartitionValue(alignedPartitionInfo, alignedValue));

        ListPartitionInfo nonAlignedPartitionInfo = createDateTruncPartitionInfo(timeUnit, scale);
        DdlException exception = Assert.assertThrows(DdlException.class,
                () -> addListPartitionValue(nonAlignedPartitionInfo, nonAlignedValue));
        Assert.assertTrue(exception.getMessage(), exception.getMessage().contains(
                "is not aligned with date_trunc(`k1`, '" + timeUnit + "')"));
    }

    private void assertDateTruncRangeEnd(String timeUnit, String lowerValue, String expectedUpperValue)
            throws AnalysisException {
        DateLiteral lower = DateLiteralUtils.createDateLiteral(lowerValue, ScalarType.createDatetimeV2Type(0));
        DateLiteral upper = PartitionExprUtil.getDateTruncRangeEnd(lower, createDateTruncExpr(timeUnit));
        Assert.assertEquals(timeUnit, expectedUpperValue, upper.getStringValue());
    }

    private FunctionCallExpr createDateTruncExpr(String timeUnit) {
        return createDateTruncExpr("k1", timeUnit);
    }

    private FunctionCallExpr createDateTruncExpr(String columnName, String timeUnit) {
        ArrayList<Expr> params = new ArrayList<>();
        params.add(new SlotRef(new TableNameInfo("tbl"), columnName));
        params.add(new StringLiteral(timeUnit));
        return new FunctionCallExpr("date_trunc", params, true);
    }

    private ListPartitionInfo createMixedPartitionInfo() throws AnalysisException {
        List<Column> columns = Lists.newArrayList(
                new Column("day_key", ScalarType.createDatetimeV2Type(0), true, null, "", ""),
                new Column("region_id", new ScalarType(PrimitiveType.INT), true, null, "", ""),
                new Column("hour_key", ScalarType.createDatetimeV2Type(0), true, null, "", ""));
        ArrayList<Expr> partitionExprs = Lists.newArrayList(
                createDateTruncExpr("day_key", "day"),
                new SlotRef(new TableNameInfo("tbl"), "region_id"),
                createDateTruncExpr("hour_key", "hour"));
        return new ListPartitionInfo(false, partitionExprs, columns);
    }

    private PartitionItem addListPartitionValue(ListPartitionInfo listPartitionInfo, String value)
            throws AnalysisException, DdlException {
        return addListPartitionValues(listPartitionInfo, value);
    }

    private PartitionItem addListPartitionValues(ListPartitionInfo listPartitionInfo, String... values)
            throws AnalysisException, DdlException {
        List<List<PartitionValue>> inValues = new ArrayList<>();
        List<PartitionValue> partitionValues = new ArrayList<>();
        for (String value : values) {
            partitionValues.add(new PartitionValue(value));
        }
        inValues.add(partitionValues);
        SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(values.length, null);
        return listPartitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);
    }

    @Test
    public void testListPartitionNullMax() throws AnalysisException, DdlException {
        PartitionItem partitionItem = null;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionInfo = new ListPartitionInfo(partitionColumns);

        List<List<PartitionValue>> inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("", true), PartitionValue.MAX_VALUE));
        SinglePartitionDesc singlePartitionDesc = new SinglePartitionDesc(false, "p1",
                PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((NULL, MAXVALUE))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("", true), new PartitionValue("", true)));
        singlePartitionDesc = new SinglePartitionDesc(false, "p2",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((NULL, NULL))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(PartitionValue.MAX_VALUE, new PartitionValue("", true)));
        singlePartitionDesc = new SinglePartitionDesc(false, "p3",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((MAXVALUE, NULL))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(PartitionValue.MAX_VALUE, PartitionValue.MAX_VALUE));
        singlePartitionDesc = new SinglePartitionDesc(false, "p4",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((MAXVALUE, MAXVALUE))", ((ListPartitionItem) partitionItem).toSql());

        inValues = new ArrayList<>();
        inValues.add(Lists.newArrayList(new PartitionValue("", true), new PartitionValue("", true)));
        inValues.add(Lists.newArrayList(PartitionValue.MAX_VALUE, new PartitionValue("", true)));
        inValues.add(Lists.newArrayList(new PartitionValue("", true), PartitionValue.MAX_VALUE));
        singlePartitionDesc = new SinglePartitionDesc(false, "p5",
        PartitionKeyDesc.createIn(inValues), null);
        singlePartitionDesc.analyze(2, null);
        partitionItem = partitionInfo.handleNewSinglePartitionDesc(singlePartitionDesc, 20000L, false);

        Assert.assertEquals("((NULL, NULL),(MAXVALUE, NULL),(NULL, MAXVALUE))", ((ListPartitionItem) partitionItem).toSql());
    }
}
