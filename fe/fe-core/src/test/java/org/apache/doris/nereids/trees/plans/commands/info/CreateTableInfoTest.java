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

import org.apache.doris.catalog.PartitionType;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CreateTableInfoTest {

    @Test
    public void testManualListDateTruncIsLegal() {
        PartitionTableInfo partitionTableInfo = manualListPartition(dateTrunc("event_time"));
        CreateTableInfo createTableInfo = createTableInfo(partitionTableInfo);

        Assertions.assertDoesNotThrow(
                () -> createTableInfo.checkLegalityOfPartitionExprs(partitionTableInfo));
        Assertions.assertFalse(partitionTableInfo.isAutoPartition());
    }

    @Test
    public void testParserDoesNotInferAutoForListExpression() {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        String manualListSql = "CREATE TABLE test_list_expr (event_time DATETIME) "
                + "PARTITION BY LIST(date_trunc(event_time, 'day')) "
                + "(PARTITION p1 VALUES IN ('2026-07-23 00:00:00')) "
                + "DISTRIBUTED BY HASH(event_time) BUCKETS 1";
        String implicitAutoRangeSql = "CREATE TABLE test_range_expr (event_time DATETIME) "
                + "PARTITION BY RANGE(date_trunc(event_time, 'day')) () "
                + "DISTRIBUTED BY HASH(event_time) BUCKETS 1";
        String explicitAutoListSql = "CREATE TABLE test_auto_list (event_time DATETIME) "
                + "AUTO PARTITION BY LIST(event_time) () "
                + "DISTRIBUTED BY HASH(event_time) BUCKETS 1";

        try {
            CreateTableCommand manualList = (CreateTableCommand) new NereidsParser().parseSingle(manualListSql);
            CreateTableCommand implicitAutoRange = (CreateTableCommand) new NereidsParser()
                    .parseSingle(implicitAutoRangeSql);
            CreateTableCommand explicitAutoList = (CreateTableCommand) new NereidsParser()
                    .parseSingle(explicitAutoListSql);

            Assertions.assertFalse(manualList.getCreateTableInfo().getPartitionTableInfo().isAutoPartition());
            Assertions.assertTrue(implicitAutoRange.getCreateTableInfo().getPartitionTableInfo().isAutoPartition());
            Assertions.assertTrue(explicitAutoList.getCreateTableInfo().getPartitionTableInfo().isAutoPartition());
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testManualListRejectsOtherPartitionFunctions() {
        UnboundFunction dateFormat = new UnboundFunction("date_format", ImmutableList.of(
                new UnboundSlot("event_time"), new StringLiteral("%Y-%m-%d")));
        PartitionTableInfo partitionTableInfo = manualListPartition(dateFormat);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> createTableInfo(partitionTableInfo).checkLegalityOfPartitionExprs(partitionTableInfo));
        Assertions.assertEquals("LIST partition only support date_trunc function expression",
                exception.getMessage());
    }

    @Test
    public void testManualListDateTruncRequiresCanonicalArguments() {
        List<List<Expression>> invalidArguments = ImmutableList.of(
                ImmutableList.of(new UnboundSlot("event_time")),
                ImmutableList.of(
                        new UnboundSlot("event_time"), new StringLiteral("day"), new StringLiteral("extra")),
                ImmutableList.of(new StringLiteral("event_time"), new StringLiteral("day")),
                ImmutableList.of(new UnboundSlot("event_time"), new UnboundSlot("day")),
                ImmutableList.of(new UnboundSlot("event_time"), new IntegerLiteral(1)));
        List<String> expectedErrors = ImmutableList.of(
                "date_trunc params exprs size should be 2.",
                "date_trunc params exprs size should be 2.",
                "date_trunc first param should be slot ref.",
                "date_trunc param of time unit is not string literal.",
                "Unsupported date_trunc time unit: 1");

        for (int i = 0; i < invalidArguments.size(); i++) {
            List<Expression> arguments = invalidArguments.get(i);
            PartitionTableInfo partitionTableInfo = manualListPartition(
                    new UnboundFunction("date_trunc", arguments));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> validatePartitionInfo(partitionTableInfo,
                            column("event_time", DateTimeType.INSTANCE),
                            column("day", DateTimeType.INSTANCE)),
                    "date_trunc arguments should be exactly (slot, string literal): " + arguments);
            Assertions.assertEquals(expectedErrors.get(i), exception.getMessage());
        }
    }

    @Test
    public void testManualListDateTruncRejectsInvalidTimeUnit() {
        PartitionTableInfo partitionTableInfo = manualListPartition(
                dateTrunc("event_time", "invalid_unit"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> validatePartitionInfo(
                        partitionTableInfo, column("event_time", DateTimeType.INSTANCE)));
        Assertions.assertEquals("Unsupported date_trunc time unit: invalid_unit", exception.getMessage());
    }

    @Test
    public void testManualListDateTruncAcceptsDateLikeColumns() {
        List<DataType> dateLikeTypes = ImmutableList.of(
                DateType.INSTANCE,
                DateV2Type.INSTANCE,
                DateTimeType.INSTANCE,
                DateTimeV2Type.SYSTEM_DEFAULT,
                TimeStampTzType.of(6));

        for (DataType dateLikeType : dateLikeTypes) {
            PartitionTableInfo partitionTableInfo = manualListPartition(dateTrunc("event_time"));
            Assertions.assertDoesNotThrow(
                    () -> validatePartitionInfo(partitionTableInfo, column("event_time", dateLikeType)),
                    "date_trunc LIST partition should accept " + dateLikeType);
        }
    }

    @Test
    public void testManualListDateTruncRejectsNonDateColumn() {
        PartitionTableInfo partitionTableInfo = manualListPartition(dateTrunc("event_time"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> validatePartitionInfo(partitionTableInfo, column("event_time", IntegerType.INSTANCE)));
        Assertions.assertTrue(exception.getMessage().contains("partition expr"));
        Assertions.assertTrue(exception.getMessage().contains("date_trunc"));
        Assertions.assertTrue(exception.getMessage().contains("is illegal"));
    }

    @Test
    public void testManualListExpressionColumnOrderAndManualMode() {
        PartitionTableInfo partitionTableInfo = manualListPartition(
                dateTrunc("event_time"),
                new UnboundSlot("region_id"),
                dateTrunc("created_time", "hour"));

        validatePartitionInfo(partitionTableInfo,
                column("event_time", DateTimeType.INSTANCE),
                column("region_id", IntegerType.INSTANCE),
                column("created_time", DateTimeV2Type.SYSTEM_DEFAULT));

        Assertions.assertEquals(
                ImmutableList.of("event_time", "region_id", "created_time"),
                partitionTableInfo.getIdentifierPartitionColumns());
        Assertions.assertFalse(partitionTableInfo.isAutoPartition());
    }

    @Test
    public void testManualListRejectsRepeatedUnderlyingColumn() {
        PartitionTableInfo partitionTableInfo = manualListPartition(
                dateTrunc("event_time"), new UnboundSlot("event_time"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> validatePartitionInfo(
                        partitionTableInfo, column("event_time", DateTimeType.INSTANCE)));

        Assertions.assertEquals("Duplicated partition column event_time", exception.getMessage());
    }

    @Test
    public void testCheckLegalityOfPartitionExprs() {
        UnboundSlot slot1 = new UnboundSlot("col1");
        UnboundSlot slot2 = new UnboundSlot("col1");
        List<Expression> innerExprs = Lists.newArrayList();
        innerExprs.add(new EqualTo(slot1, slot2));
        UnboundFunction unboundFunction = new UnboundFunction("test_func", innerExprs);

        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(unboundFunction);
        PartitionTableInfo partitionTableInfo1 = new PartitionTableInfo(false, null, new ArrayList<>(), partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo1, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo.checkLegalityOfPartitionExprs(partitionTableInfo1),
                "only Auto Range Partition support UnboundFunction");

        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkLegalityOfPartitionExprs(partitionTableInfo2),
                "partition expression test_func has unrecognized parameter in slot 0");

        List<Expression> innerExprs2 = Lists.newArrayList();
        innerExprs2.add(slot1);
        innerExprs2.add(slot2);
        UnboundFunction unboundFunction2 = new UnboundFunction("test_func", innerExprs2);
        List<Expression> partitionFields2 = new ArrayList<>();
        partitionFields2.add(unboundFunction2);
        PartitionTableInfo partitionTableInfo3 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields2);
        CreateTableInfo createTableInfo3 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo3, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo3.checkLegalityOfPartitionExprs(partitionTableInfo3));

        List<Expression> partitionFields3 = new ArrayList<>();
        partitionFields3.add(slot1);
        PartitionTableInfo partitionTableInfo4 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields3);
        CreateTableInfo createTableInfo4 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo4, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo4.checkLegalityOfPartitionExprs(partitionTableInfo4),
                "Auto Range Partition need UnboundFunction");

        PartitionTableInfo partitionTableInfo5 = new PartitionTableInfo(false, "RANGE", new ArrayList<>(), partitionFields3);
        CreateTableInfo createTableInfo5 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo5, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo5.checkLegalityOfPartitionExprs(partitionTableInfo5));

        List<Expression> partitionFields4 = new ArrayList<>();
        partitionFields4.add(new StringLiteral("test"));
        PartitionTableInfo partitionTableInfo6 = new PartitionTableInfo(true, "RANGE", new ArrayList<>(), partitionFields4);
        CreateTableInfo createTableInfo6 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo6, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo6.checkLegalityOfPartitionExprs(partitionTableInfo6),
                "partition expression literal is illegal!");
    }

    // NOTE: the LIVE iceberg v3 reserved-row-lineage-column rejection moved off fe-core into the iceberg
    // connector (IcebergConnectorMetadata.createTable); it is now covered by IcebergConnectorMetadataDdlTest
    // (request-level + catalog table-default/override format-version precedence). CreateTableInfo's
    // validateIcebergRowLineageColumns(int) is no longer on the live path (the engine gate was removed) and
    // survives only for the legacy dead IcebergMetadataOps caller (deleted with it in the deletion phase), so
    // the former fe-core unit tests that drove it directly were dropped.

    @Test
    public void testCheckPartitionNullity1() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        UnboundSlot slot = new UnboundSlot("col2");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", new ArrayList<>(), partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo),
                "Unknown partition column name:col2");

        //partitionDefs is empty
        UnboundSlot slot2 = new UnboundSlot("col1");
        List<Expression> partitionFields2 = new ArrayList<>();
        partitionFields2.add(slot2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", new ArrayList<>(), partitionFields2);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo2.checkPartitionNullity(columnDefs, partitionTableInfo2));
    }

    /**
     * partitionDef instance of InPartition
     */
    @Test
    public void testCheckPartitionNullity2() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<List<Expression>> values = new ArrayList<>();
        List<Expression> innerValues = new ArrayList<>();
        values.add(innerValues);
        StringLiteral expr = new StringLiteral("col1");
        innerValues.add(expr);
        PartitionDefinition inPartition = new InPartition(true, partName, values);
        partitionDefs.add(inPartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<List<Expression>> values2 = new ArrayList<>();
        List<Expression> innerValues2 = new ArrayList<>();
        values2.add(innerValues2);
        NullLiteral expr2 = new NullLiteral();
        innerValues2.add(expr2);
        PartitionDefinition inPartition2 = new InPartition(true, partName, values2);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(inPartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }

    /**
     * partitionDef instance of LessThanPartition
     */
    @Test
    public void testCheckPartitionNullity3() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<Expression> values = new ArrayList<>();
        StringLiteral expr = new StringLiteral("col1");
        values.add(expr);
        PartitionDefinition lessThanPartition = new LessThanPartition(true, partName, values);
        partitionDefs.add(lessThanPartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<Expression> values2 = new ArrayList<>();
        NullLiteral expr2 = new NullLiteral();
        values2.add(expr2);
        PartitionDefinition lessThanPartition2 = new LessThanPartition(true, partName, values2);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(lessThanPartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }


    /**
     * partitionDef instance of FixedRangePartition
     */
    @Test
    public void testCheckPartitionNullity4() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<Expression> lowValues = new ArrayList<>();
        StringLiteral lowExpr = new StringLiteral("col1");
        lowValues.add(lowExpr);

        List<Expression> upperValues = new ArrayList<>();
        StringLiteral upperExpr = new StringLiteral("col1");
        upperValues.add(upperExpr);

        PartitionDefinition fixedRangePartition = new FixedRangePartition(true, partName, lowValues, upperValues);
        partitionDefs.add(fixedRangePartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<Expression> lowValues2 = new ArrayList<>();
        NullLiteral lowExpr2 = new NullLiteral();
        lowValues2.add(lowExpr2);

        List<Expression> upperValues2 = new ArrayList<>();
        NullLiteral upperExpr2 = new NullLiteral();
        upperValues2.add(upperExpr2);

        PartitionDefinition fixedRangePartition2 = new FixedRangePartition(true, partName, lowValues2, upperValues2);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(fixedRangePartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }

    /**
     * partitionDef instance of StepPartition
     */
    @Test
    public void testCheckPartitionNullity5() {
        List<ColumnDefinition> columnDefs = new ArrayList<>();
        //isNullable == true
        ColumnDefinition columnDef = new ColumnDefinition("col1", null, false, null, true, null, null);
        columnDefs.add(columnDef);
        List<PartitionDefinition> partitionDefs = new ArrayList<>();
        String partName = "col1";
        List<Expression> fromValues = new ArrayList<>();
        StringLiteral fromExpr = new StringLiteral("col1");
        fromValues.add(fromExpr);

        List<Expression> toValues = new ArrayList<>();
        StringLiteral toExpr = new StringLiteral("col1");
        toValues.add(toExpr);

        PartitionDefinition stepPartition = new StepPartition(true, partName, fromValues, toValues, 1, null);
        partitionDefs.add(stepPartition);
        UnboundSlot slot = new UnboundSlot("col1");
        List<Expression> partitionFields = new ArrayList<>();
        partitionFields.add(slot);
        PartitionTableInfo partitionTableInfo = new PartitionTableInfo(false, "RANGE", partitionDefs, partitionFields);
        CreateTableInfo createTableInfo = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertDoesNotThrow(() -> createTableInfo.checkPartitionNullity(columnDefs, partitionTableInfo));

        List<ColumnDefinition> columnDefs2 = new ArrayList<>();
        //isNullable == false
        ColumnDefinition columnDef2 = new ColumnDefinition("col1", null, false, null, false, null, null);
        columnDefs2.add(columnDef2);
        List<Expression> fromValues2 = new ArrayList<>();
        NullLiteral fromExpr2 = new NullLiteral();
        fromValues2.add(fromExpr2);

        List<Expression> toValues2 = new ArrayList<>();
        NullLiteral toExpr2 = new NullLiteral();
        toValues2.add(toExpr2);

        PartitionDefinition stepPartition2 = new StepPartition(true, partName, fromValues2, toValues2, 1, null);
        List<PartitionDefinition> partitionDefs2 = new ArrayList<>();
        partitionDefs2.add(stepPartition2);
        PartitionTableInfo partitionTableInfo2 = new PartitionTableInfo(false, "RANGE", partitionDefs2, partitionFields);
        CreateTableInfo createTableInfo2 = new CreateTableInfo(false, false, false, "test_ctl", "test_db", "test_tbl", new ArrayList<>(), new ArrayList<>(), null, null, new ArrayList<>(), null, partitionTableInfo2, null, new ArrayList<>(), new HashMap<>(), new HashMap<>(), new ArrayList<>());
        Assertions.assertThrows(AnalysisException.class, () -> createTableInfo2.checkPartitionNullity(columnDefs2, partitionTableInfo2),
                "Can't have null partition is for NOT NULL partition column in partition expr's index 0");
    }

    private static PartitionTableInfo manualListPartition(Expression... partitionExpressions) {
        return new PartitionTableInfo(
                false,
                PartitionType.LIST.name(),
                new ArrayList<>(),
                ImmutableList.copyOf(partitionExpressions));
    }

    private static UnboundFunction dateTrunc(String columnName) {
        return dateTrunc(columnName, "day");
    }

    private static UnboundFunction dateTrunc(String columnName, String timeUnit) {
        return new UnboundFunction("date_trunc", ImmutableList.of(
                new UnboundSlot(columnName), new StringLiteral(timeUnit)));
    }

    private static ColumnDefinition column(String name, DataType dataType) {
        return new ColumnDefinition(name, dataType, true, null, false, Optional.empty(), "");
    }

    private static void validatePartitionInfo(
            PartitionTableInfo partitionTableInfo, ColumnDefinition... columns) {
        partitionTableInfo.extractPartitionColumns();
        Map<String, ColumnDefinition> columnMap = new HashMap<>();
        for (ColumnDefinition column : columns) {
            columnMap.put(column.getName(), column);
        }
        partitionTableInfo.validatePartitionInfo(
                columnMap, new HashMap<>(), new ConnectContext(), false, false);
    }

    private static CreateTableInfo createTableInfo(PartitionTableInfo partitionTableInfo) {
        return new CreateTableInfo(
                false,
                false,
                false,
                "test_ctl",
                "test_db",
                "test_tbl",
                new ArrayList<>(),
                new ArrayList<>(),
                null,
                null,
                new ArrayList<>(),
                null,
                partitionTableInfo,
                null,
                new ArrayList<>(),
                new HashMap<>(),
                new HashMap<>(),
                new ArrayList<>());
    }
}
