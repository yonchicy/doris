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

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Sink;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Update mv by partition
 */
public class UpdateMvByPartitionCommand extends InsertOverwriteTableCommand {
    private static final Logger LOG = LogManager.getLogger(UpdateMvByPartitionCommand.class);

    private UpdateMvByPartitionCommand(LogicalPlan logicalQuery) {
        super(logicalQuery, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public boolean isForceDropPartition() {
        // After refreshing the data in MTMV, it will be synchronized with the base table
        // and there is no need to put it in the recycle bin
        return true;
    }

    /**
     * Construct command
     *
     * @param mv materialize view
     * @param partitionNames update partitions in mv and tables
     * @param tableWithPartKey the partitions key for different table
     * @param statementContext statementContext
     * @return command
     */
    public static UpdateMvByPartitionCommand from(MTMV mv, Set<String> partitionNames,
            Map<TableIf, String> tableWithPartKey, StatementContext statementContext) throws UserException {
        NereidsParser parser = new NereidsParser();
        Map<TableIf, Set<Expression>> predicates =
                constructTableWithPredicates(mv, partitionNames, tableWithPartKey);
        List<String> parts = constructPartsForMv(partitionNames);
        Plan plan = parser.parseSingle(mv.getQuerySql());
        if (plan instanceof Sink) {
            plan = plan.child(0);
        }
        LogicalSink<? extends Plan> sink = UnboundTableSinkCreator.createUnboundTableSink(mv.getFullQualifiers(),
                ImmutableList.of(), ImmutableList.of(), parts, plan);
        if (LOG.isDebugEnabled()) {
            LOG.debug("MTMVTask plan for mvName: {}, partitionNames: {}, plan: {}", mv.getName(), partitionNames,
                    sink.treeString());
        }
        statementContext.setMvRefreshPredicates(predicates);
        return new UpdateMvByPartitionCommand(sink);
    }

    private static List<String> constructPartsForMv(Set<String> partitionNames) {
        return Lists.newArrayList(partitionNames);
    }

    private static Map<TableIf, Set<Expression>> constructTableWithPredicates(MTMV mv,
            Set<String> partitionNames, Map<TableIf, String> tableWithPartKey) throws AnalysisException {
        Set<PartitionItem> items = Sets.newHashSet();
        for (String partitionName : partitionNames) {
            PartitionItem partitionItem = mv.getPartitionItemOrAnalysisException(partitionName);
            items.add(partitionItem);
        }
        ImmutableMap.Builder<TableIf, Set<Expression>> builder = new ImmutableMap.Builder<>();
        tableWithPartKey.forEach((table, colName) -> {
            // the partition items of mv only have one partition column, so the key index is 0
            Pair<Optional<Expr>, Integer> pctPartitionInfo =
                    getPctPartitionExprAndPos((MTMVRelatedTableIf) table, colName);
            builder.put(table, constructPredicates(items, new UnboundSlot(colName),
                    pctPartitionInfo.first, 0));
        });
        return builder.build();
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, String colName) {
        UnboundSlot slot = new UnboundSlot(colName);
        return constructPredicates(partitions, slot);
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot) {
        return constructPredicates(partitions, colSlot, Optional.empty(), 0);
    }

    /**
     * construct predicates for partition items, the min key is the min key of range items.
     * For list partition or less than partition items, the min key is null.
     * <p>
     * When the pct column of a LIST partition is a partition expression such as date_trunc, the stored
     * partition value is the truncated boundary, which means the source column range
     * [boundary, rangeEnd), so the predicate is inverted to range comparison instead of
     * `col IN (boundary)` on the raw column.
     *
     * @param pctPartitionExpr the partition expression of the pct column in the base table,
     *                         empty if the column has no expression
     * @param keyIndex the index of the pct column value in the partition key, 0 for mv partition
     *                 items (mv has only one partition column) and the pct column position for
     *                 base table partition items
     */
    @VisibleForTesting
    public static Set<Expression> constructPredicates(Set<PartitionItem> partitions, Slot colSlot,
            Optional<Expr> pctPartitionExpr, int keyIndex) {
        Set<Expression> predicates = new HashSet<>();
        if (partitions.isEmpty()) {
            return Sets.newHashSet(BooleanLiteral.TRUE);
        }
        if (partitions.iterator().next() instanceof ListPartitionItem) {
            for (PartitionItem item : partitions) {
                predicates.add(convertListPartitionToIn(item, colSlot, keyIndex, pctPartitionExpr));
            }
        } else {
            for (PartitionItem item : partitions) {
                predicates.add(convertRangePartitionToCompare(item, colSlot));
            }
        }
        return predicates;
    }

    private static Expression convertPartitionKeyToLiteral(PartitionKey key, int pos) {
        return Literal.fromLegacyLiteral(key.getKeys().get(pos),
                Type.fromPrimitiveType(key.getTypes().get(pos)));
    }

    private static Expression convertListPartitionToIn(PartitionItem item, Slot col, int keyIndex,
            Optional<Expr> pctPartitionExpr) {
        List<PartitionKey> keys = ((ListPartitionItem) item).getItems();
        if (pctPartitionExpr.isPresent() && isDateTruncExpr(pctPartitionExpr.get())) {
            return convertDateTruncListPartitionToRanges(keys, col, keyIndex, pctPartitionExpr.get());
        }
        List<Expression> inValues = keys.stream()
                .map(key -> convertPartitionKeyToLiteral(key, keyIndex))
                .collect(ImmutableList.toImmutableList());
        List<Expression> predicates = new ArrayList<>();
        if (inValues.stream().anyMatch(NullLiteral.class::isInstance)) {
            inValues = inValues.stream()
                    .filter(e -> !(e instanceof NullLiteral))
                    .collect(Collectors.toList());
            Expression isNullPredicate = new IsNull(col);
            predicates.add(isNullPredicate);
        }
        if (!inValues.isEmpty()) {
            predicates.add(new InPredicate(col, inValues));
        }
        if (predicates.isEmpty()) {
            return BooleanLiteral.of(true);
        }
        return ExpressionUtils.or(predicates);
    }

    private static boolean isDateTruncExpr(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        return ((FunctionCallExpr) expr).getFnName().getFunction().equalsIgnoreCase("date_trunc");
    }

    /**
     * Invert date_trunc list partition boundaries to source column ranges. A stored boundary v is
     * aligned to the date_trunc unit (checked when the partition is created), so a row belongs to
     * the partition iff its source column value is in [v, getDateTruncRangeEnd(v)).
     */
    private static Expression convertDateTruncListPartitionToRanges(List<PartitionKey> keys, Slot col,
            int keyIndex, Expr partitionExpr) {
        List<Expression> predicates = new ArrayList<>();
        for (PartitionKey key : keys) {
            LiteralExpr legacyValue = key.getKeys().get(keyIndex);
            if (legacyValue.isNullLiteral()) {
                predicates.add(new IsNull(col));
                continue;
            }
            Preconditions.checkState(legacyValue instanceof DateLiteral,
                    "date_trunc list partition value should be a date literal, value: "
                            + legacyValue.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE));
            DateLiteral begin = (DateLiteral) legacyValue;
            try {
                DateLiteral end = PartitionExprUtil.getDateTruncRangeEnd(begin, partitionExpr);
                predicates.add(ExpressionUtils.and(
                        new GreaterThanEqual(col, Literal.fromLegacyLiteral(begin, begin.getType())),
                        new LessThan(col, Literal.fromLegacyLiteral(end, end.getType()))));
            } catch (AnalysisException e) {
                throw new IllegalStateException("invert date_trunc list partition value failed, value: "
                        + legacyValue.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE), e);
            }
        }
        if (predicates.isEmpty()) {
            return BooleanLiteral.of(true);
        }
        return ExpressionUtils.or(predicates);
    }

    /**
     * Get the partition expression and the column position of the pct column in a LIST-partitioned
     * base table. Ordinary LIST columns are SlotRefs, expression columns are function calls such as
     * date_trunc; a column without any expression yields an empty Optional.
     */
    private static Pair<Optional<Expr>, Integer> getPctPartitionExprAndPos(MTMVRelatedTableIf pctTable,
            String pctColName) {
        if (!(pctTable instanceof OlapTable)) {
            return Pair.of(Optional.empty(), 0);
        }
        PartitionInfo partitionInfo = ((OlapTable) pctTable).getPartitionInfo();
        if (!(partitionInfo instanceof ListPartitionInfo)) {
            return Pair.of(Optional.empty(), 0);
        }
        List<Expr> partitionExprs = partitionInfo.getPartitionExprs();
        List<Column> partitionColumns = partitionInfo.getPartitionColumns();
        for (int i = 0; i < partitionColumns.size(); i++) {
            if (partitionColumns.get(i).getName().equalsIgnoreCase(pctColName)) {
                Expr partitionExpr = i < partitionExprs.size() ? partitionExprs.get(i) : null;
                return Pair.of(Optional.ofNullable(partitionExpr), i);
            }
        }
        return Pair.of(Optional.empty(), 0);
    }

    private static Expression convertRangePartitionToCompare(PartitionItem item, Slot col) {
        Range<PartitionKey> range = item.getItems();
        List<Expression> expressions = new ArrayList<>();
        if (range.hasLowerBound() && !range.lowerEndpoint().isMinValue()) {
            PartitionKey key = range.lowerEndpoint();
            expressions.add(new GreaterThanEqual(col, convertPartitionKeyToLiteral(key, 0)));
        }
        if (range.hasUpperBound() && !range.upperEndpoint().isMaxValue()) {
            PartitionKey key = range.upperEndpoint();
            expressions.add(new LessThan(col, convertPartitionKeyToLiteral(key, 0)));
        }
        if (expressions.isEmpty()) {
            return BooleanLiteral.of(true);
        }
        Expression predicate = ExpressionUtils.and(expressions);
        // The partition without can be the first partition of LESS THAN PARTITIONS
        // The null value can insert into this partition, so we need to add or is null condition
        if (!range.hasLowerBound() || range.lowerEndpoint().isMinValue()) {
            predicate = ExpressionUtils.or(predicate, new IsNull(col));
        }
        return predicate;
    }

    /**
     * Add predicates on base table when mv can partition update, Also support plan that contain cte and view
     */
    public static class PredicateAdder extends DefaultPlanRewriter<PredicateAddContext> {

        // record view and cte name parts, these should be ignored and visit it's actual plan
        public Set<List<String>> virtualRelationNamePartSet = new HashSet<>();

        @Override
        public Plan visitUnboundRelation(UnboundRelation unboundRelation, PredicateAddContext predicates) {

            if (predicates.getPredicates() == null || predicates.getPredicates().isEmpty()) {
                return unboundRelation;
            }
            if (virtualRelationNamePartSet.contains(unboundRelation.getNameParts())) {
                return unboundRelation;
            }
            List<String> tableQualifier = RelationUtil.getQualifierName(ConnectContext.get(),
                    unboundRelation.getNameParts());
            TableIf table = RelationUtil.getTable(tableQualifier, Env.getCurrentEnv(), Optional.empty());
            if (predicates.getPredicates().containsKey(table)) {
                return new LogicalFilter<>(
                        ExpressionUtils.extractConjunctionToSet(
                                ExpressionUtils.or(predicates.getPredicates().get(table))
                        ),
                        unboundRelation
                );
            }
            return unboundRelation;
        }

        @Override
        public Plan visitLogicalCTE(LogicalCTE<? extends Plan> cte, PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return cte;
            }
            List<LogicalSubQueryAlias<Plan>> rewrittenSubQueryAlias = new ArrayList<>();
            for (LogicalSubQueryAlias<Plan> subQueryAlias : cte.getAliasQueries()) {
                List<Plan> subQueryAliasChildren = new ArrayList<>();
                this.virtualRelationNamePartSet.add(subQueryAlias.getQualifier());
                subQueryAlias.children().forEach(subQuery ->
                        subQueryAliasChildren.add(subQuery.accept(this, predicates))
                );
                rewrittenSubQueryAlias.add(subQueryAlias.withChildren(subQueryAliasChildren));
            }
            return super.visitLogicalCTE(new LogicalCTE<>(cte.isRecursive(),
                    rewrittenSubQueryAlias, cte.child()), predicates);
        }

        @Override
        public Plan visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> subQueryAlias,
                PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return subQueryAlias;
            }
            this.virtualRelationNamePartSet.add(subQueryAlias.getQualifier());
            return super.visitLogicalSubQueryAlias(subQueryAlias, predicates);
        }

        @Override
        public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation,
                PredicateAddContext predicates) {
            if (predicates.isEmpty()) {
                return catalogRelation;
            }
            TableIf table = catalogRelation.getTable();
            if (predicates.getPredicates() != null) {
                if (predicates.getPredicates().containsKey(table)) {
                    return new LogicalFilter<>(
                            ExpressionUtils.extractConjunctionToSet(
                                    ExpressionUtils.or(predicates.getPredicates().get(table))
                            ),
                            catalogRelation);
                }
            }
            if (predicates.getPartitions() != null) {
                if (!(table instanceof MTMVRelatedTableIf)) {
                    return catalogRelation;
                }
                for (Map.Entry<BaseColInfo, Set<String>> filterTableEntry : predicates.getPartitions().entrySet()) {
                    BaseColInfo relatedTableColumnInfo = filterTableEntry.getKey();
                    if (!Objects.equals(new BaseTableInfo(table), relatedTableColumnInfo.getTableInfo())) {
                        continue;
                    }
                    Slot partitionSlot = null;
                    for (Slot slot : catalogRelation.getOutput()) {
                        if (slot.getName().equals(relatedTableColumnInfo.getColName())) {
                            partitionSlot = slot;
                            break;
                        }
                    }
                    if (partitionSlot == null) {
                        predicates.setHandleSuccess(false);
                        return catalogRelation;
                    }
                    // if partition has no data, doesn't add filter
                    Set<PartitionItem> partitionHasDataItems = new HashSet<>();
                    MTMVRelatedTableIf targetTable = (MTMVRelatedTableIf) table;
                    Pair<Optional<Expr>, Integer> pctPartitionInfo = getPctPartitionExprAndPos(
                            targetTable, relatedTableColumnInfo.getColName());
                    for (String partitionName : filterTableEntry.getValue()) {
                        if (targetTable instanceof OlapTable) {
                            Partition partition = targetTable.getPartition(partitionName);
                            if (partition == null) {
                                // partition maybe deleted, skip it
                                continue;
                            }
                            if (!((OlapTable) targetTable).selectNonEmptyPartitionIds(
                                    Lists.newArrayList(partition.getId()), Optional.empty()).isEmpty()) {
                                // Add filter only when partition has data when olap table
                                partitionHasDataItems.add(
                                        ((OlapTable) targetTable).getPartitionInfo().getItem(partition.getId()));
                            }
                        }
                        if (targetTable instanceof ExternalTable) {
                            PartitionItem partitionItem = ((ExternalTable) targetTable).getNameToPartitionItems(
                                    MvccUtil.getSnapshotFromContext(targetTable)).get(partitionName);
                            // Add filter only when partition has data when external table
                            if (partitionItem != null) {
                                partitionHasDataItems.add(partitionItem);
                            }
                        }
                    }
                    if (partitionHasDataItems.isEmpty()) {
                        predicates.setNeedAddFilter(false);
                    }
                    if (!partitionHasDataItems.isEmpty()) {
                        return new LogicalFilter<>(
                                ExpressionUtils.extractConjunctionToSet(
                                        ExpressionUtils.or(constructPredicates(partitionHasDataItems, partitionSlot,
                                                pctPartitionInfo.first, pctPartitionInfo.second))
                                ),
                                catalogRelation);
                    }
                }
            }
            return catalogRelation;
        }
    }

    /**
     * Predicate context, which support add predicate by expression or by partition name
     * Add by predicates has high priority
     */
    public static class PredicateAddContext {

        private final Map<TableIf, Set<Expression>> predicates;
        private final Map<BaseColInfo, Set<String>> partitions;
        private boolean handleSuccess = true;
        // when add filter by partition, if partition has no data, doesn't need to add filter. should be false
        private boolean needAddFilter = true;

        public PredicateAddContext(Map<TableIf, Set<Expression>> predicates,
                Map<BaseColInfo, Set<String>> partitions) {
            this.predicates = predicates;
            this.partitions = partitions;
        }

        public Map<TableIf, Set<Expression>> getPredicates() {
            return predicates;
        }

        public Map<BaseColInfo, Set<String>> getPartitions() {
            return partitions;
        }

        public boolean isEmpty() {
            return predicates == null && partitions == null;
        }

        public boolean isHandleSuccess() {
            return handleSuccess;
        }

        public void setHandleSuccess(boolean handleSuccess) {
            this.handleSuccess = handleSuccess;
        }

        public boolean isNeedAddFilter() {
            return needAddFilter;
        }

        public void setNeedAddFilter(boolean needAddFilter) {
            this.needAddFilter = needAddFilter;
        }
    }
}
