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

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PredicateToRangeTest {
    private static final Column INT_COLUMN = new Column("k", Type.BIGINT, false);

    @Test
    public void testInContainsOnlyListValues() {
        RangeSet<PartitionKey> range = toRange(INT_COLUMN, false, new IntLiteral(2), new IntLiteral(4));

        Assertions.assertFalse(range.contains(intKey(1)));
        Assertions.assertTrue(range.contains(intKey(2)));
        Assertions.assertFalse(range.contains(intKey(3)));
        Assertions.assertTrue(range.contains(intKey(4)));
        Assertions.assertFalse(range.contains(intKey(5)));
        Assertions.assertFalse(range.contains(key(INT_COLUMN, NullLiteral.create(Type.BIGINT))));
        Assertions.assertEquals(range.asRanges(), toRange(INT_COLUMN, false,
                new IntLiteral(4), new IntLiteral(2), new IntLiteral(4)).asRanges());
    }

    @Test
    public void testNotInExcludesEveryListValue() {
        RangeSet<PartitionKey> range = toRange(INT_COLUMN, true, new IntLiteral(2), new IntLiteral(4));

        Assertions.assertTrue(range.contains(intKey(1)));
        Assertions.assertFalse(range.contains(intKey(2)));
        Assertions.assertTrue(range.contains(intKey(3)));
        Assertions.assertFalse(range.contains(intKey(4)));
        Assertions.assertTrue(range.contains(intKey(5)));
        Assertions.assertEquals(range.asRanges(), toRange(INT_COLUMN, true,
                new IntLiteral(4), new IntLiteral(2), new IntLiteral(4)).asRanges());

        RangeSet<PartitionKey> differentRange = toRange(INT_COLUMN, true, new IntLiteral(3), new IntLiteral(5));
        Assertions.assertNotEquals(range.asRanges(), differentRange.asRanges());
        Assertions.assertTrue(differentRange.contains(intKey(2)));
        Assertions.assertFalse(differentRange.contains(intKey(3)));
    }

    @Test
    public void testInIgnoresNullListValues() {
        RangeSet<PartitionKey> range = toRange(INT_COLUMN, false,
                new IntLiteral(2), NullLiteral.create(Type.BIGINT), new IntLiteral(4));

        Assertions.assertEquals(toRange(INT_COLUMN, false, new IntLiteral(2), new IntLiteral(4)).asRanges(),
                range.asRanges());
        Assertions.assertTrue(toRange(INT_COLUMN, false, NullLiteral.create(Type.BIGINT)).isEmpty());
        Assertions.assertTrue(toRange(INT_COLUMN, false,
                NullLiteral.create(Type.BIGINT), NullLiteral.create(Type.BIGINT)).isEmpty());
    }

    @Test
    public void testNotInWithNullHasNoTrueValues() {
        Assertions.assertTrue(toRange(INT_COLUMN, true,
                NullLiteral.create(Type.BIGINT), new IntLiteral(2), new IntLiteral(4)).isEmpty());
        Assertions.assertTrue(toRange(INT_COLUMN, true,
                new IntLiteral(2), new IntLiteral(4), NullLiteral.create(Type.BIGINT)).isEmpty());
        Assertions.assertTrue(toRange(INT_COLUMN, true, NullLiteral.create(Type.BIGINT)).isEmpty());
        Assertions.assertTrue(toRange(INT_COLUMN, true,
                NullLiteral.create(Type.BIGINT), NullLiteral.create(Type.BIGINT)).isEmpty());
    }

    @Test
    public void testNotInIntersectsActualPartitionRange() {
        RangeSet<PartitionKey> range = toRange(INT_COLUMN, true,
                new IntLiteral(-1), new IntLiteral(2), new IntLiteral(4), new IntLiteral(7));
        RangeSet<PartitionKey> clipped = range.subRangeSet(Range.closedOpen(intKey(0), intKey(6)));

        Assertions.assertEquals(ImmutableSet.of(
                Range.closedOpen(intKey(0), intKey(2)),
                Range.open(intKey(2), intKey(4)),
                Range.open(intKey(4), intKey(6))), clipped.asRanges());
        Assertions.assertFalse(clipped.contains(key(INT_COLUMN, NullLiteral.create(Type.BIGINT))));
    }

    @Test
    public void testNotInPreservesFiniteIntegerLimits() {
        RangeSet<PartitionKey> range = toRange(INT_COLUMN, true, new IntLiteral(2), new IntLiteral(4));
        Assertions.assertTrue(range.contains(intKey(Long.MIN_VALUE)));
        Assertions.assertTrue(range.contains(intKey(Long.MAX_VALUE)));

        RangeSet<PartitionKey> excludeLimits = toRange(INT_COLUMN, true,
                new IntLiteral(Long.MIN_VALUE), new IntLiteral(Long.MAX_VALUE));
        Assertions.assertFalse(excludeLimits.contains(intKey(Long.MIN_VALUE)));
        Assertions.assertFalse(excludeLimits.contains(intKey(Long.MAX_VALUE)));
        Assertions.assertTrue(excludeLimits.contains(intKey(Long.MIN_VALUE + 1)));
        Assertions.assertTrue(excludeLimits.contains(intKey(Long.MAX_VALUE - 1)));
    }

    @Test
    public void testNotInPreservesDatetimeV2Microseconds() {
        Type dateType = ScalarType.createDatetimeV2Type(6);
        Column column = new Column("dt", dateType, false);
        DateLiteral first = new DateLiteral(2024, 3, 10, 0, 0, 0, 123456, dateType);
        DateLiteral second = new DateLiteral(2024, 3, 10, 0, 0, 0, 123458, dateType);
        RangeSet<PartitionKey> range = toRange(column, true, first, second);

        Assertions.assertFalse(range.contains(key(column, first)));
        Assertions.assertFalse(range.contains(key(column, second)));
        Assertions.assertTrue(range.contains(key(column,
                new DateLiteral(2024, 3, 10, 0, 0, 0, 123455, dateType))));
        Assertions.assertTrue(range.contains(key(column,
                new DateLiteral(2024, 3, 10, 0, 0, 0, 123457, dateType))));
        Assertions.assertTrue(range.contains(key(column,
                new DateLiteral(2024, 3, 10, 0, 0, 0, 123459, dateType))));
    }

    private RangeSet<PartitionKey> toRange(Column column, boolean isNotIn, Expr... values) {
        InPredicate predicate = new InPredicate(new SlotRef(null, column.getName()),
                ImmutableList.copyOf(values), isNotIn);
        Assertions.assertTrue(PredicateToRange.supportedToRange(predicate));
        return new PredicateToRange(column).exprToRange(predicate);
    }

    private PartitionKey intKey(long value) {
        return key(INT_COLUMN, new IntLiteral(value));
    }

    private PartitionKey key(Column column, LiteralExpr value) {
        PartitionKey key = new PartitionKey();
        key.pushColumn(value, column.getDataType());
        return key;
    }
}
