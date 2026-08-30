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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/PlanFragment.java
// and modified by Doris

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.planner.OlapScanNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** PartitionRangePredicateNormalizer */
public class PartitionRangePredicateNormalizer {
    private final Normalizer normalizer;
    private final OlapScanNode olapScanNode;

    public PartitionRangePredicateNormalizer(Normalizer normalizer, OlapScanNode olapScanNode) {
        this.normalizer = Objects.requireNonNull(normalizer, "normalizer can not be null");
        this.olapScanNode = Objects.requireNonNull(olapScanNode, "olapScanNode can not be null");
    }

    public List<Expr> normalize() {
        NormalizedPartitionPredicates predicates = normalizePredicates();
        normalizer.setNormalizedPartitionPredicates(olapScanNode, predicates);
        return predicates.remainedPredicates;
    }

    private NormalizedPartitionPredicates normalizePredicates() {
        OlapTable olapTable = olapScanNode.getOlapTable();
        List<Column> partitionColumns = olapTable.getPartitionColumns();

        if (partitionColumns.isEmpty()) {
            return cannotIntersectPartitionRange();
        }

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (partitionInfo.getType() == PartitionType.RANGE && partitionColumns.size() == 1) {
            return normalizeSingleRangePartitionColumnPredicates(
                    partitionColumns.get(0), (RangePartitionInfo) partitionInfo);
        }

        if (partitionInfo.getType() == PartitionType.LIST) {
            // list partition supports both single column and multiple columns, and each
            // partition column may be a plain slot or a `date_trunc(slot, unit)` expression.
            return normalizeListPartitionPredicates(
                    partitionColumns, (ListPartitionInfo) partitionInfo);
        }

        // multi-column range partition and other partition types can not intersect range
        return cannotIntersectPartitionRange();
    }

    private NormalizedPartitionPredicates normalizeSingleRangePartitionColumnPredicates(
            Column partitionColumn, RangePartitionInfo rangePartitionInfo) {

        List<Pair<Long, RangeSet<PartitionKey>>> partitionItemRanges
                = rangePartitionInfo.getPartitionItems(olapScanNode.getSelectedPartitionIds())
                .entrySet()
                .stream()
                .map(entry -> {
                    RangeSet<PartitionKey> rangeSet = TreeRangeSet.create();
                    rangeSet.add(entry.getValue().getItems());
                    return Pair.of(entry.getKey(), rangeSet);
                })
                .collect(Collectors.toList());

        List<Expr> conjuncts = extractConjuncts(olapScanNode.getConjuncts());

        ToRangePredicatesExtractor extractor = ToRangePredicatesExtractor.extract(
                conjuncts, olapScanNode, partitionColumn);

        PredicateToRange predicateToRange = new PredicateToRange(partitionColumn);

        for (Expr partitionPredicate : extractor.supportedToRangePredicates) {
            RangeSet<PartitionKey> predicateRanges = predicateToRange.exprToRange(partitionPredicate);
            partitionItemRanges = partitionItemRanges.stream()
                    .map(kv -> {
                        RangeSet<PartitionKey> partitionRangeSet = kv.second;
                        RangeSet<PartitionKey> intersect = TreeRangeSet.create();
                        for (Range<PartitionKey> predicateRange : predicateRanges.asRanges()) {
                            intersect.addAll(partitionRangeSet.subRangeSet(predicateRange));
                        }
                        return Pair.of(kv.first, intersect);
                    }).filter(kv -> !kv.second.isEmpty())
                    .collect(Collectors.toList());
        }

        Map<Long, String> partitionToIntersectRange = partitionItemRanges.stream()
                .map(pair -> Pair.of(pair.first, normalizeRangeSet(pair.second).toString()))
                .collect(Collectors.toMap(Pair::key, Pair::value));

        return new NormalizedPartitionPredicates(extractor.notSupportedToRangePredicates, partitionToIntersectRange);
    }

    /**
     * Normalize the partition predicates for list partition tables.
     *
     * <p>Unlike range partition, a list partition item holds a set of discrete partition key
     * tuples, and the partition columns are independent from each other (no lexicographic
     * coupling between columns as in multi-column range partition). Each partition column is
     * either:
     * <ul>
     *   <li>a plain slot (discrete column): the tuple value is a single point on the base
     *       column, which either survives the predicate as a whole or is eliminated;</li>
     *   <li>a {@code date_trunc(slot, unit)} expression: the tuple value is the truncation
     *       boundary, which maps to a closed-open range {@code [boundary, boundary + 1 unit)}
     *       on the base column, and the predicates may narrow this range just like the range
     *       partition path.</li>
     * </ul>
     *
     * <p>A tuple survives only when every column survives (columns are conjunctive); the
     * surviving tuples of a partition are independent (disjunctive). The surviving result is
     * serialized deterministically as the partition's filter range string.
     */
    private NormalizedPartitionPredicates normalizeListPartitionPredicates(
            List<Column> partitionColumns, ListPartitionInfo listPartitionInfo) {
        int columnNum = partitionColumns.size();
        List<Expr> partitionExprs = listPartitionInfo.getPartitionExprs();
        // Every partition column carries a partition expression (a plain slot for a discrete
        // column, or a `date_trunc(slot, unit)` call); without them the column domains can not be
        // described precisely, so fall back to keeping all predicates in the digest.
        if (partitionExprs.size() != columnNum) {
            return cannotIntersectPartitionRange();
        }

        boolean[] dateTruncColumns = new boolean[columnNum];
        List<Optional<SlotId>> partitionSlotIds = new ArrayList<>(columnNum);
        for (int i = 0; i < columnNum; i++) {
            Expr partitionExpr = partitionExprs.get(i);
            dateTruncColumns[i] = isDateTruncPartitionExpr(partitionExpr);
            partitionSlotIds.add(findPartitionColumnSlotId(olapScanNode, partitionColumns.get(i)));
        }

        List<Expr> conjuncts = extractConjuncts(olapScanNode.getConjuncts());

        // bucket the supported simple predicates by the partition column they bind to;
        // anything else (or predicate, function on partition column, non partition column, ...)
        // stays in the digest.
        List<List<Expr>> predicatesPerColumn = new ArrayList<>(columnNum);
        List<Expr> notSupportedToRangePredicates = new ArrayList<>();
        for (int i = 0; i < columnNum; i++) {
            predicatesPerColumn.add(new ArrayList<>());
        }
        for (Expr conjunct : conjuncts) {
            int matchedColumn = -1;
            for (int i = 0; i < columnNum; i++) {
                Optional<SlotId> slotId = partitionSlotIds.get(i);
                if (slotId.isPresent()
                        && conjunct.isBound(slotId.get())
                        && PredicateToRange.supportedToRange(conjunct)) {
                    matchedColumn = i;
                    break;
                }
            }
            if (matchedColumn >= 0) {
                predicatesPerColumn.get(matchedColumn).add(conjunct);
            } else {
                notSupportedToRangePredicates.add(conjunct);
            }
        }

        Map<Long, String> partitionToIntersectRange = new LinkedHashMap<>();
        for (Long partitionId : olapScanNode.getSelectedPartitionIds()) {
            PartitionItem partitionItem = listPartitionInfo.getItem(partitionId);
            if (!(partitionItem instanceof ListPartitionItem)) {
                throw new IllegalStateException("Expect list partition item for partition: " + partitionId);
            }
            ListPartitionItem listPartitionItem = (ListPartitionItem) partitionItem;
            if (listPartitionItem.isDefaultPartition()) {
                throw new IllegalStateException("Can not compute intersect range for default list partition");
            }

            List<Pair<PartitionKey, String>> survivingTuples = new ArrayList<>();
            for (PartitionKey tuple : listPartitionItem.getItems()) {
                Preconditions.checkState(tuple.getKeys().size() == columnNum);
                StringBuilder tupleRange = new StringBuilder("(");
                boolean tupleAlive = true;
                for (int i = 0; i < columnNum; i++) {
                    LiteralExpr tupleValue = tuple.getKeys().get(i);
                    if (tupleValue instanceof NullLiteral) {
                        throw new IllegalStateException("Can not compute intersect range for null partition key");
                    }
                    Column baseColumn = partitionColumns.get(i);
                    List<Expr> columnPredicates = predicatesPerColumn.get(i);

                    if (i > 0) {
                        tupleRange.append(" | ");
                    }
                    if (dateTruncColumns[i]) {
                        // date_trunc column: the partition boundary maps to a closed-open range on
                        // the base column and the predicates may narrow it, so intersect them as
                        // ranges and normalize to closed-open form.
                        RangeSet<PartitionKey> intersected = intersectDateTruncColumnPredicates(
                                tupleValue, partitionExprs.get(i), baseColumn, columnPredicates);
                        if (intersected.isEmpty()) {
                            tupleAlive = false;
                            break;
                        }
                        tupleRange.append(normalizeRangeSet(intersected));
                    } else {
                        // discrete column: the tuple value is a single point which either satisfies
                        // all predicates as a whole or is eliminated. Evaluate it by literal
                        // comparison instead of building predicate ranges, since non-integral types
                        // (e.g. varchar) have no infinity key for a RangeSet.
                        if (!pointSatisfiesPredicates(tupleValue, baseColumn, columnPredicates)) {
                            tupleAlive = false;
                            break;
                        }
                        tupleRange.append(discretePointRange(tupleValue, baseColumn));
                    }
                }
                if (!tupleAlive) {
                    continue;
                }
                tupleRange.append(")");
                survivingTuples.add(Pair.of(tuple, tupleRange.toString()));
            }

            if (survivingTuples.isEmpty()) {
                // The selected partitions are already pruned by the partition pruner, so a tuple
                // is guaranteed to overlap the predicates. An empty result means the normalization
                // can not describe the filter precisely, bail out to the conservative path.
                throw new IllegalStateException("No list partition tuple survives the predicates: " + partitionId);
            }

            // sort by the partition key to make the range string independent of the
            // tuple insertion order and the evaluation order of the predicates.
            survivingTuples.sort(Comparator.comparing(pair -> pair.first));
            partitionToIntersectRange.put(
                    partitionId,
                    survivingTuples.stream().map(pair -> pair.second).collect(Collectors.joining(" ; ")));
        }

        return new NormalizedPartitionPredicates(notSupportedToRangePredicates, partitionToIntersectRange);
    }

    private RangeSet<PartitionKey> intersectDateTruncColumnPredicates(
            LiteralExpr boundary, Expr partitionExpr, Column baseColumn, List<Expr> partitionPredicates) {
        // the partition key stores the date_trunc boundary, and it maps to the closed-open
        // range [boundary, boundary + 1 unit) on the base column.
        DateLiteral lower = (DateLiteral) boundary;
        DateLiteral upper;
        try {
            upper = PartitionExprUtil.getDateTruncRangeEnd(lower, partitionExpr);
        } catch (AnalysisException e) {
            throw new IllegalStateException("Can not compute date_trunc range end: " + e.getMessage(), e);
        }

        TreeRangeSet<PartitionKey> intersected = TreeRangeSet.create();
        intersected.add(Range.closedOpen(
                toSingleColumnPartitionKey(lower, baseColumn),
                toSingleColumnPartitionKey(upper, baseColumn)));

        if (partitionPredicates.isEmpty()) {
            return intersected;
        }
        PredicateToRange predicateToRange = new PredicateToRange(baseColumn);
        for (Expr partitionPredicate : partitionPredicates) {
            RangeSet<PartitionKey> predicateRanges = predicateToRange.exprToRange(partitionPredicate);
            TreeRangeSet<PartitionKey> step = TreeRangeSet.create();
            for (Range<PartitionKey> predicateRange : predicateRanges.asRanges()) {
                step.addAll(intersected.subRangeSet(predicateRange));
            }
            intersected = step;
        }
        return intersected;
    }

    private boolean pointSatisfiesPredicates(
            LiteralExpr pointValue, Column baseColumn, List<Expr> partitionPredicates) {
        for (Expr partitionPredicate : partitionPredicates) {
            if (!pointSatisfiesPredicate(pointValue, baseColumn, partitionPredicate)) {
                return false;
            }
        }
        return true;
    }

    private boolean pointSatisfiesPredicate(
            LiteralExpr pointValue, Column baseColumn, Expr partitionPredicate) {
        if (partitionPredicate instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) partitionPredicate;
            LiteralExpr right = (LiteralExpr) partitionPredicate.getChild(1);
            int cmp = comparePointWithLiteral(pointValue, right, baseColumn);
            switch (binaryPredicate.getOp()) {
                case EQ:
                    return cmp == 0;
                case NE:
                    return cmp != 0;
                case LT:
                    return cmp < 0;
                case LE:
                    return cmp <= 0;
                case GT:
                    return cmp > 0;
                case GE:
                    return cmp >= 0;
                default:
                    throw new IllegalStateException(
                            "Unsupported binary predicate for list partition: " + partitionPredicate);
            }
        } else if (partitionPredicate instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) partitionPredicate;
            boolean matches = false;
            boolean containsNull = false;
            for (Expr option : inPredicate.getListChildren()) {
                if (option instanceof NullLiteral) {
                    containsNull = true;
                } else if (comparePointWithLiteral(pointValue, (LiteralExpr) option, baseColumn) == 0) {
                    matches = true;
                }
            }
            if (inPredicate.isNotIn()) {
                // x NOT IN (options) is true only when x matches no option and there is no
                // null option, otherwise the predicate evaluates to null/false.
                return !matches && !containsNull;
            }
            // x IN (options) is true only when x matches one option; a null option never makes
            // a non-null point true.
            return matches;
        }
        throw new IllegalStateException("Unsupported predicate for list partition point: " + partitionPredicate);
    }

    private static String discretePointRange(LiteralExpr pointValue, Column baseColumn) {
        // render the surviving point as a closed point range so the format is consistent with
        // the date_trunc range segment; successor() is not needed for a point.
        PartitionKey point = toSingleColumnPartitionKey(pointValue, baseColumn);
        TreeRangeSet<PartitionKey> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closed(point, point));
        return rangeSet.toString();
    }

    private static int comparePointWithLiteral(LiteralExpr pointValue, LiteralExpr literal, Column baseColumn) {
        return toSingleColumnPartitionKey(pointValue, baseColumn)
                .compareTo(toSingleColumnPartitionKey(literal, baseColumn));
    }

    private static PartitionKey toSingleColumnPartitionKey(LiteralExpr literal, Column partitionColumn) {
        PartitionKey partitionKey = new PartitionKey();
        partitionKey.pushColumn(literal, partitionColumn.getDataType());
        return partitionKey;
    }

    private static boolean isDateTruncPartitionExpr(Expr expr) {
        return expr instanceof FunctionCallExpr
                && ((FunctionCallExpr) expr).getFnName().getFunction().equalsIgnoreCase("date_trunc");
    }

    private RangeSet<PartitionKey> normalizeRangeSet(RangeSet<PartitionKey> rangeSet) {
        // normalize range to closeOpened range, the between predicate and less than predicate
        // maybe reuse the same cache to save memory
        RangeSet<PartitionKey> normalized = TreeRangeSet.create();
        for (Range<PartitionKey> range : rangeSet.asRanges()) {
            PartitionKey lowerEndpoint = range.lowerEndpoint();
            PartitionKey upperEndpoint = range.upperEndpoint();

            try {
                if (!lowerEndpoint.isMinValue() && !range.contains(lowerEndpoint)) {
                    lowerEndpoint = lowerEndpoint.successor();
                }
                if (!upperEndpoint.isMaxValue() && range.contains(upperEndpoint)) {
                    upperEndpoint = upperEndpoint.successor();
                }
                normalized.add(Range.closedOpen(lowerEndpoint, upperEndpoint));
            } catch (Throwable t) {
                throw new IllegalStateException("Can not normalize range: " + t.getMessage(), t);
            }
        }
        return normalized;
    }

    private NormalizedPartitionPredicates cannotIntersectPartitionRange() {
        Map<Long, String> canNotComputeIntersectRange = new LinkedHashMap<>();
        for (Long selectedPartitionId : olapScanNode.getSelectedPartitionIds()) {
            canNotComputeIntersectRange.put(selectedPartitionId, "");
        }
        return new NormalizedPartitionPredicates(
                // conjuncts will be used as the part of the digest
                olapScanNode.getConjuncts(),
                // can not compute intersect range
                canNotComputeIntersectRange
        );
    }

    private List<Expr> extractConjuncts(List<Expr> conjuncts) {
        List<Expr> flattenedConjuncts = Lists.newArrayListWithCapacity(conjuncts.size());
        for (Expr conjunct : conjuncts) {
            boolean findChildren = true;
            conjunct.foreachDown(expr -> {
                if (expr instanceof CompoundPredicate && ((CompoundPredicate) expr).getOp() == Operator.AND) {
                    return findChildren;
                } else {
                    flattenedConjuncts.add((Expr) expr);
                    return !findChildren;
                }
            });
        }
        return flattenedConjuncts;
    }

    private static Optional<SlotId> findPartitionColumnSlotId(OlapScanNode olapScanNode, Column partitionColumn) {
        if (partitionColumn == null) {
            return Optional.empty();
        }

        for (SlotDescriptor slot : olapScanNode.getTupleDesc().getSlots()) {
            Column column = slot.getColumn();
            if (column.getName().equalsIgnoreCase(partitionColumn.getName())) {
                return Optional.of(slot.getId());
            }
        }
        return Optional.empty();
    }

    private static class ToRangePredicatesExtractor {
        public final List<Expr> supportedToRangePredicates;
        public final List<Expr> notSupportedToRangePredicates;

        private ToRangePredicatesExtractor(
                List<Expr> simplePartitionPredicates, List<Expr> notSupportedToRangePredicates) {
            this.supportedToRangePredicates = simplePartitionPredicates;
            this.notSupportedToRangePredicates = notSupportedToRangePredicates;
        }

        public static ToRangePredicatesExtractor extract(
                List<Expr> conjuncts, OlapScanNode olapScanNode, Column partitionColumn) {
            List<Expr> supportedPartitionPredicates = Lists.newArrayList();
            List<Expr> otherPredicates = Lists.newArrayList();

            Optional<SlotId> optPartitionId = findPartitionColumnSlotId(olapScanNode, partitionColumn);

            for (Expr conjunct : conjuncts) {
                if (optPartitionId.isPresent()
                        && conjunct.isBound(optPartitionId.get())
                        && PredicateToRange.supportedToRange(conjunct)) {
                    supportedPartitionPredicates.add(conjunct);
                } else {
                    otherPredicates.add(conjunct);
                }
            }

            return new ToRangePredicatesExtractor(supportedPartitionPredicates, otherPredicates);
        }
    }
}
