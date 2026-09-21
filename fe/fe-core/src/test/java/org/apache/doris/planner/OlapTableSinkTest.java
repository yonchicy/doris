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

import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.planner.OlapTableSink.AdaptiveBucketAssignment;
import org.apache.doris.planner.OlapTableSink.AdaptiveIndexBucketAssignment;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TOlapTableIndexTablets;
import org.apache.doris.thrift.TOlapTableLocationParam;
import org.apache.doris.thrift.TOlapTablePartition;
import org.apache.doris.thrift.TOlapTablePartitionParam;
import org.apache.doris.thrift.TTabletLocation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class OlapTableSinkTest {
    @Test
    public void testManualListPartitionFunctionExprsDoNotEnableAutoPartition() throws Exception {
        OlapTable table = Mockito.mock(OlapTable.class);
        Column partitionColumn = new Column("dt", ScalarType.createDatetimeV2Type(0));
        ArrayList<Expr> metadataExprs = Lists.newArrayList(new FunctionCallExpr("date_trunc",
                Lists.newArrayList(new SlotRef(null, "dt"), new StringLiteral("day")), true));
        ListPartitionInfo partitionInfo = new ListPartitionInfo(
                false, metadataExprs, Collections.singletonList(partitionColumn));
        ListPartitionItem partitionItem = Mockito.mock(ListPartitionItem.class);
        DataProperty dataProperty = Mockito.mock(DataProperty.class);
        Partition partition = Mockito.mock(Partition.class);
        MaterializedIndex index = Mockito.mock(MaterializedIndex.class);
        HashDistributionInfo distributionInfo = new HashDistributionInfo(
                1, Collections.singletonList(partitionColumn));
        BoolLiteral analyzedPartitionExpr = new BoolLiteral(true);
        partitionInfo.setItem(1L, false, partitionItem);
        partitionInfo.setDataProperty(1L, dataProperty);

        Mockito.when(dataProperty.isMutable()).thenReturn(true);
        Mockito.when(table.getPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(partitionItem.getItems()).thenReturn(Collections.emptyList());
        Mockito.when(partitionItem.isDefaultPartition()).thenReturn(false);
        Mockito.when(table.getPartition(1L)).thenReturn(partition);
        Mockito.when(partition.getId()).thenReturn(1L);
        Mockito.when(partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL))
                .thenReturn(Collections.singletonList(index));
        Mockito.when(partition.getDistributionInfo()).thenReturn(distributionInfo);
        Mockito.when(index.getId()).thenReturn(10L);
        Mockito.when(index.getTablets()).thenReturn(Collections.emptyList());

        OlapTableSink sink = new OlapTableSink(table, null, Collections.singletonList(1L), false,
                Collections.singletonList(analyzedPartitionExpr), Collections.emptyMap());
        TOlapTablePartitionParam param = sink.createPartition(1L, table);

        Assert.assertTrue(param.isSetPartitionFunctionExprs());
        Assert.assertEquals(1, param.getPartitionFunctionExprsSize());
        Assert.assertTrue(param.isSetEnableAutomaticPartition());
        Assert.assertFalse(param.isEnableAutomaticPartition());
    }

    @Test
    public void testCreateDummyLocationUsesLoadAvailableBackendInCurrentComputeGroup() throws Exception {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend currentComputeGroupBackend = Mockito.mock(Backend.class);
        Backend loadDisabledBackend = Mockito.mock(Backend.class);
        OlapTable table = Mockito.mock(OlapTable.class);

        Mockito.when(currentComputeGroupBackend.getId()).thenReturn(1L);
        Mockito.when(currentComputeGroupBackend.isLoadAvailable()).thenReturn(true);
        Mockito.when(loadDisabledBackend.getId()).thenReturn(2L);
        Mockito.when(loadDisabledBackend.isLoadAvailable()).thenReturn(false);
        Mockito.when(systemInfoService.getBackendsByCurrentCluster())
                .thenReturn(ImmutableMap.of(1L, currentComputeGroupBackend, 2L, loadDisabledBackend));
        Mockito.when(systemInfoService.getAllBackendIds(true)).thenReturn(Collections.singletonList(3L));
        Mockito.when(table.getIndexNumber()).thenReturn(1);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            OlapTableSink sink = new OlapTableSink(table, null, Collections.emptyList(), false);
            List<TOlapTableLocationParam> locationParams = sink.createDummyLocation(table);

            Assert.assertEquals(Collections.singletonList(1L),
                    locationParams.get(0).getTablets().get(0).getNodeIds());
            Mockito.verify(systemInfoService, Mockito.never()).getAllBackendIds(true);
            Mockito.verify(systemInfoService).getBackendsByCurrentCluster();
        }
    }

    @Test
    public void testCreateDummyLocationDoesNotShareBackendCandidatesAcrossIndexes() throws Exception {
        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        Backend currentComputeGroupBackend = Mockito.mock(Backend.class);
        OlapTable table = Mockito.mock(OlapTable.class);

        Mockito.when(currentComputeGroupBackend.getId()).thenReturn(1L);
        Mockito.when(currentComputeGroupBackend.isLoadAvailable()).thenReturn(true);
        Mockito.when(systemInfoService.getBackendsByCurrentCluster())
                .thenReturn(ImmutableMap.of(1L, currentComputeGroupBackend));
        Mockito.when(table.getIndexNumber()).thenReturn(2);

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);

            OlapTableSink sink = new OlapTableSink(table, null, Collections.emptyList(), true);
            List<TOlapTableLocationParam> locationParams = sink.createDummyLocation(table);

            Assert.assertEquals(2, locationParams.get(0).getTabletsSize());
            Assert.assertEquals(Collections.singletonList(1L),
                    locationParams.get(0).getTablets().get(0).getNodeIds());
            Assert.assertEquals(Collections.singletonList(1L),
                    locationParams.get(0).getTablets().get(1).getNodeIds());
        }
    }

    @Test
    public void testAdaptiveRandomBucketAssignmentIsPerIndex() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1000L);
        partition.setNumBuckets(2);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Arrays.asList(100L, 101L)));
        partition.addToIndexes(new TOlapTableIndexTablets(2L, Arrays.asList(200L, 201L)));

        List<TTabletLocation> locations = Arrays.asList(
                new TTabletLocation(100L, Arrays.asList(10L)),
                new TTabletLocation(101L, Arrays.asList(20L)),
                new TTabletLocation(200L, Arrays.asList(20L)),
                new TTabletLocation(201L, Arrays.asList(10L)));

        Map<Long, Map<Long, AdaptiveBucketAssignment>> assignments =
                OlapTableSink.computeAdaptiveRandomBucketAssignments(
                        Arrays.asList(10L, 20L), Arrays.asList(partition), locations, 2);

        AdaptiveBucketAssignment be10Assignment = assignments.get(10L).get(1000L);
        Assert.assertEquals(0, be10Assignment.getLoadTabletIdx());
        Assert.assertEquals(10L, be10Assignment.getBucketBeId());
        Assert.assertEquals(Arrays.asList(0), be10Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be10Assignment, 1L, 10L, Arrays.asList(0));
        assertIndexAssignment(be10Assignment, 2L, 20L, Arrays.asList(0));

        AdaptiveBucketAssignment be20Assignment = assignments.get(20L).get(1000L);
        Assert.assertEquals(1, be20Assignment.getLoadTabletIdx());
        Assert.assertEquals(20L, be20Assignment.getBucketBeId());
        Assert.assertEquals(Arrays.asList(1), be20Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be20Assignment, 1L, 20L, Arrays.asList(1));
        assertIndexAssignment(be20Assignment, 2L, 10L, Arrays.asList(1));

        OlapTableSink.applyAdaptiveRandomBucketAssignments(Arrays.asList(partition), assignments.get(10L));
        Assert.assertEquals(10L, partition.getBucketBeId());
        Assert.assertEquals(Arrays.asList(0), partition.getLocalBucketSeqs());
        Assert.assertEquals(10L, partition.getIndexes().get(0).getBucketBeId());
        Assert.assertEquals(Arrays.asList(0), partition.getIndexes().get(0).getLocalBucketSeqs());
        Assert.assertEquals(20L, partition.getIndexes().get(1).getBucketBeId());
        Assert.assertEquals(Arrays.asList(0), partition.getIndexes().get(1).getLocalBucketSeqs());
    }

    @Test
    public void testAdaptiveRandomBucketAssignmentIsSharedByReceiverPartition() {
        TOlapTablePartition partition = new TOlapTablePartition();
        partition.setId(1001L);
        partition.setNumBuckets(4);
        partition.setLoadTabletIdx(0);
        partition.addToIndexes(new TOlapTableIndexTablets(1L, Arrays.asList(100L, 101L, 102L, 103L)));
        partition.addToIndexes(new TOlapTableIndexTablets(2L, Arrays.asList(200L, 201L, 202L, 203L)));

        List<TTabletLocation> locations = Arrays.asList(
                new TTabletLocation(100L, Arrays.asList(10L)),
                new TTabletLocation(101L, Arrays.asList(10L)),
                new TTabletLocation(102L, Arrays.asList(20L)),
                new TTabletLocation(103L, Arrays.asList(20L)),
                new TTabletLocation(200L, Arrays.asList(30L)),
                new TTabletLocation(201L, Arrays.asList(30L)),
                new TTabletLocation(202L, Arrays.asList(30L)),
                new TTabletLocation(203L, Arrays.asList(30L)));

        Map<Long, Map<Long, AdaptiveBucketAssignment>> assignments =
                OlapTableSink.computeAdaptiveRandomBucketAssignments(
                        Arrays.asList(10L, 20L, 30L, 40L), Arrays.asList(partition), locations, 4);

        AdaptiveBucketAssignment be10Assignment = assignments.get(10L).get(1001L);
        Assert.assertEquals(0, be10Assignment.getLoadTabletIdx());
        Assert.assertEquals(10L, be10Assignment.getBucketBeId());
        Assert.assertEquals(Arrays.asList(0, 1), be10Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be10Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be20Assignment = assignments.get(20L).get(1001L);
        Assert.assertEquals(2, be20Assignment.getLoadTabletIdx());
        Assert.assertEquals(20L, be20Assignment.getBucketBeId());
        Assert.assertEquals(Arrays.asList(2, 3), be20Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be20Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be30Assignment = assignments.get(30L).get(1001L);
        Assert.assertEquals(be10Assignment.getLoadTabletIdx(), be30Assignment.getLoadTabletIdx());
        Assert.assertEquals(be10Assignment.getBucketBeId(), be30Assignment.getBucketBeId());
        Assert.assertEquals(be10Assignment.getLocalBucketSeqs(), be30Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be30Assignment, 1L, 10L, Arrays.asList(0, 1));
        assertIndexAssignment(be30Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));

        AdaptiveBucketAssignment be40Assignment = assignments.get(40L).get(1001L);
        Assert.assertEquals(be20Assignment.getLoadTabletIdx(), be40Assignment.getLoadTabletIdx());
        Assert.assertEquals(be20Assignment.getBucketBeId(), be40Assignment.getBucketBeId());
        Assert.assertEquals(be20Assignment.getLocalBucketSeqs(), be40Assignment.getLocalBucketSeqs());
        assertIndexAssignment(be40Assignment, 1L, 20L, Arrays.asList(2, 3));
        assertIndexAssignment(be40Assignment, 2L, 30L, Arrays.asList(0, 1, 2, 3));
    }

    private void assertIndexAssignment(AdaptiveBucketAssignment assignment, long indexId, long bucketBeId,
            List<Integer> localBucketSeqs) {
        AdaptiveIndexBucketAssignment indexAssignment = assignment.getIndexAssignments().get(indexId);
        Assert.assertNotNull(indexAssignment);
        Assert.assertEquals(indexId, indexAssignment.getIndexId());
        Assert.assertEquals(bucketBeId, indexAssignment.getBucketBeId());
        Assert.assertEquals(localBucketSeqs, indexAssignment.getLocalBucketSeqs());
    }
}
