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
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

    private static Expr dateTruncMonthExpr() {
        return new FunctionCallExpr("date_trunc",
                ImmutableList.of(new SlotRef(null, "s"), new StringLiteral("month")), false);
    }
}
