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

package org.apache.doris.nereids.load;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

class NereidsLoadPlanInfoCollectorTest {

    @Test
    void testLoadPrunesDateTruncListPartitionBySourceRange() throws Exception {
        Column partitionColumn = new Column("dt", ScalarType.createDatetimeV2Type(0));
        ArrayList<Expr> partitionExprs = new ArrayList<>(ImmutableList.of(
                new FunctionCallExpr("date_trunc", ImmutableList.of(
                        new SlotRef(null, "dt"), new StringLiteral("day")), false)));
        ListPartitionInfo partitionInfo = new ListPartitionInfo(false, partitionExprs,
                ImmutableList.of(partitionColumn));
        partitionInfo.setItem(1L, false, createListPartitionItem(partitionColumn, "2026-07-23 00:00:00"));
        partitionInfo.setItem(2L, false, createListPartitionItem(partitionColumn, "2026-07-24 00:00:00"));

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        NereidsLoadTaskInfo taskInfo = Mockito.mock(NereidsLoadTaskInfo.class);
        Mockito.when(taskInfo.getPartitionNamesInfo()).thenReturn(null);
        NereidsLoadPlanInfoCollector collector = new NereidsLoadPlanInfoCollector(
                table, taskInfo, new TUniqueId(1, 2), 3L, TUniqueKeyUpdateMode.UPSERT,
                TPartialUpdateNewRowPolicy.APPEND, new HashSet<>(), new HashMap<>());

        SlotReference partitionSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        setField(collector, "partitionSlots", new ArrayList<>(ImmutableList.of(partitionSlot)));
        setField(collector, "filterPredicate",
                new EqualTo(partitionSlot, new DateTimeV2Literal("2026-07-23 12:00:00")));
        setField(collector, "logicalPlan", Mockito.mock(LogicalPlan.class));

        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        try {
            Assertions.assertEquals(ImmutableList.of(1L), invokeGetAllPartitionIds(collector));
        } finally {
            ConnectContext.remove();
        }
    }

    private List<Long> invokeGetAllPartitionIds(NereidsLoadPlanInfoCollector collector) throws Exception {
        Method method = NereidsLoadPlanInfoCollector.class.getDeclaredMethod("getAllPartitionIds");
        method.setAccessible(true);
        return (List<Long>) method.invoke(collector);
    }

    private void setField(NereidsLoadPlanInfoCollector collector, String fieldName, Object value) throws Exception {
        Field field = NereidsLoadPlanInfoCollector.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(collector, value);
    }

    private ListPartitionItem createListPartitionItem(Column partitionColumn, String value) throws Exception {
        PartitionKey key = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue(value)), ImmutableList.of(partitionColumn));
        return new ListPartitionItem(ImmutableList.of(key));
    }
}
