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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteFromCommandTest {

    @Test
    public void testBuildDeleteFallbackExceptionPreservesBothFailureCauses() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception("initial predicate failure");
        Exception fallbackException = new Exception("fallback execution failure");

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify the merged exception surfaces the fallback failure and keeps the initial failure.
        Assertions.assertEquals(
                "Delete fallback execution failed: fallback execution failure"
                        + ". Initial predicate check failed: initial predicate failure",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testBuildDeleteFallbackExceptionFallsBackToThrowableToString() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception((String) null);
        Exception fallbackException = new Exception((String) null);

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify null messages still produce debuggable text.
        Assertions.assertEquals(
                "Delete fallback execution failed: java.lang.Exception"
                        + ". Initial predicate check failed: java.lang.Exception",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testDeletePrunesDateTruncListPartitionBySourceRange() throws Exception {
        Column partitionColumn = new Column("dt", ScalarType.createDatetimeV2Type(0));
        ArrayList<Expr> partitionExprs = new ArrayList<>(ImmutableList.of(
                new FunctionCallExpr("date_trunc", ImmutableList.of(
                        new SlotRef(null, "dt"), new StringLiteral("day")), false)));
        ListPartitionInfo partitionInfo = new ListPartitionInfo(false, partitionExprs,
                ImmutableList.of(partitionColumn));
        partitionInfo.setItem(1L, false, createListPartitionItem(partitionColumn, "2026-07-23 00:00:00"));
        partitionInfo.setItem(2L, false, createListPartitionItem(partitionColumn, "2026-07-24 00:00:00"));

        Partition dayPartition = mockPartition(1L, "p_day");
        Partition nextPartition = mockPartition(2L, "p_next");
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(table.getPartitionColumns()).thenReturn(ImmutableList.of(partitionColumn));
        Mockito.when(table.getPartition("p_day")).thenReturn(dayPartition);
        Mockito.when(table.getPartition("p_next")).thenReturn(nextPartition);
        Mockito.when(table.getPartition(1L)).thenReturn(dayPartition);
        Mockito.when(table.getPartition(2L)).thenReturn(nextPartition);

        SlotReference partitionSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.getOutput()).thenReturn(ImmutableList.of(partitionSlot));
        Mockito.when(filter.getPredicate()).thenReturn(
                new EqualTo(partitionSlot, new DateTimeV2Literal("2026-07-23 12:00:00")));
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);

        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        try {
            List<Partition> selectedPartitions = invokeGetSelectedPartitions(command, table, filter, scan,
                    new ArrayList<>(ImmutableList.of("p_day", "p_next")));
            Assertions.assertEquals(ImmutableList.of(dayPartition), selectedPartitions);
        } finally {
            ConnectContext.remove();
        }
    }

    // Use reflection to validate the helper without exposing it only for tests.
    private AnalysisException invokeBuildDeleteFallbackException(DeleteFromCommand command,
            Exception initialException, Exception fallbackException)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = DeleteFromCommand.class.getDeclaredMethod("buildDeleteFallbackException",
                Exception.class, Exception.class);
        method.setAccessible(true);
        return (AnalysisException) method.invoke(command, initialException, fallbackException);
    }

    private List<Partition> invokeGetSelectedPartitions(DeleteFromCommand command, OlapTable table,
            PhysicalFilter<?> filter, PhysicalOlapScan scan, List<String> partitionNames)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = DeleteFromCommand.class.getDeclaredMethod("getSelectedPartitions",
                OlapTable.class, PhysicalFilter.class, PhysicalOlapScan.class, List.class);
        method.setAccessible(true);
        return (List<Partition>) method.invoke(command, table, filter, scan, partitionNames);
    }

    private ListPartitionItem createListPartitionItem(Column partitionColumn, String value) throws Exception {
        PartitionKey key = PartitionKey.createListPartitionKey(
                ImmutableList.of(new PartitionValue(value)), ImmutableList.of(partitionColumn));
        return new ListPartitionItem(ImmutableList.of(key));
    }

    private Partition mockPartition(long id, String name) {
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getId()).thenReturn(id);
        Mockito.when(partition.getName()).thenReturn(name);
        return partition;
    }
}
