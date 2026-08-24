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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

/** OneListPartitionInputs */
public class OneListPartitionEvaluator<K>
        extends DefaultExpressionRewriter<Map<Slot, PartitionSlotInput>> implements OnePartitionEvaluator<K> {
    private final K partitionIdent;
    private final List<Slot> partitionSlots;
    private final ListPartitionItem partitionItem;
    private final ExpressionRewriteContext expressionRewriteContext;
    private final OneRangePartitionEvaluator<K> rangePartitionEvaluator;

    public OneListPartitionEvaluator(K partitionIdent, List<Slot> partitionSlots,
            ListPartitionItem partitionItem, CascadesContext cascadesContext) {
        this(partitionIdent, partitionSlots, partitionItem, ImmutableList.of(), cascadesContext);
    }

    /** Create a LIST evaluator with optional partition expressions. */
    public OneListPartitionEvaluator(K partitionIdent, List<Slot> partitionSlots,
            ListPartitionItem partitionItem, List<Expr> partitionExprs, CascadesContext cascadesContext) {
        this.partitionIdent = partitionIdent;
        this.partitionSlots = Objects.requireNonNull(partitionSlots, "partitionSlots cannot be null");
        this.partitionItem = Objects.requireNonNull(partitionItem, "partitionItem cannot be null");
        this.expressionRewriteContext = new ExpressionRewriteContext(
                Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null"));
        this.rangePartitionEvaluator = partitionExprs.stream().anyMatch(OneListPartitionEvaluator::isDateTrunc)
                ? OneRangePartitionEvaluator.forIndependentRanges(partitionIdent, partitionSlots,
                        getIndependentRangeInputs(partitionExprs), cascadesContext, partitionItem.isDefaultPartition())
                : null;
    }

    @Override
    public K getPartitionIdent() {
        return partitionIdent;
    }

    @Override
    public List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs() {
        if (rangePartitionEvaluator != null) {
            return rangePartitionEvaluator.getOnePartitionInputs();
        }
        if (partitionSlots.size() == 1) {
            // fast path
            return getInputsByOneSlot();
        } else {
            // slow path
            return getInputsByMultiSlots();
        }
    }

    private List<Map<Slot, PartitionSlotInput>> getIndependentRangeInputs(List<Expr> partitionExprs) {
        List<Map<Slot, PartitionSlotInput>> inputs = new ArrayList<>(partitionItem.getItems().size());
        for (PartitionKey item : partitionItem.getItems()) {
            ImmutableMap.Builder<Slot, PartitionSlotInput> inputBuilder = ImmutableMap.builder();
            for (int i = 0; i < partitionSlots.size(); i++) {
                Slot partitionSlot = partitionSlots.get(i);
                LiteralExpr legacyLiteral = item.getKeys().get(i);
                Literal literal = Literal.fromLegacyLiteral(legacyLiteral, legacyLiteral.getType());
                Expr partitionExpr = partitionExprs.get(i);
                if (isDateTrunc(partitionExpr)) {
                    ColumnRange range = getDateTruncRange((DateLiteral) legacyLiteral, partitionExpr);
                    inputBuilder.put(partitionSlot, new PartitionSlotInput(partitionSlot,
                            ImmutableMap.of(partitionSlot, range)));
                } else {
                    inputBuilder.put(partitionSlot, new PartitionSlotInput(literal,
                            ImmutableMap.of(partitionSlot, ColumnRange.singleton(literal))));
                }
            }
            inputs.add(inputBuilder.build());
        }
        return inputs;
    }

    static boolean isDateTrunc(Expr partitionExpr) {
        return partitionExpr instanceof FunctionCallExpr
                && ((FunctionCallExpr) partitionExpr).getFnName().getFunction().equalsIgnoreCase("date_trunc");
    }

    private ColumnRange getDateTruncRange(DateLiteral lower, Expr dateTrunc) {
        try {
            DateLiteral upper = PartitionExprUtil.getDateTruncRangeEnd(lower, dateTrunc);
            Literal lowerLiteral = Literal.fromLegacyLiteral(lower, lower.getType());
            Literal upperLiteral = Literal.fromLegacyLiteral(upper, upper.getType());
            return ColumnRange.range(lowerLiteral, BoundType.CLOSED, upperLiteral, BoundType.OPEN);
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    private List<Map<Slot, PartitionSlotInput>> getInputsByOneSlot() {
        ImmutableList.Builder<Map<Slot, PartitionSlotInput>> inputs
                = ImmutableList.builderWithExpectedSize(partitionItem.getItems().size());
        Slot slot = partitionSlots.get(0);
        for (PartitionKey item : partitionItem.getItems()) {
            LiteralExpr legacy = item.getKeys().get(0);
            inputs.add(ImmutableMap.of(
                    slot,
                    new PartitionSlotInput(Literal.fromLegacyLiteral(legacy, legacy.getType()), ImmutableMap.of()))
            );
        }
        return inputs.build();
    }

    private List<Map<Slot, PartitionSlotInput>> getInputsByMultiSlots() {
        return partitionItem.getItems().stream()
                .map(keys -> {
                    List<Literal> literals = keys.getKeys()
                            .stream()
                            .map(literal -> Literal.fromLegacyLiteral(literal, literal.getType()))
                            .collect(ImmutableList.toImmutableList());

                    return IntStream.range(0, partitionSlots.size())
                            .mapToObj(index -> {
                                Slot partitionSlot = partitionSlots.get(index);
                                // partitionSlot will be replaced to this literal
                                Literal literal = literals.get(index);
                                // list partition don't need to compute the slot's range,
                                // so we pass through an empty map
                                return Pair.of(partitionSlot, new PartitionSlotInput(literal, ImmutableMap.of()));
                            }).collect(ImmutableMap.toImmutableMap(Pair::key, Pair::value));
                }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Map<Slot, PartitionSlotInput> context) {
        if (!inPredicate.optionsAreLiterals()) {
            return super.visitInPredicate(inPredicate, context);
        }

        Expression newCompareExpr = inPredicate.getCompareExpr().accept(this, context);
        if (newCompareExpr.isNullLiteral()) {
            return NullLiteral.BOOLEAN_INSTANCE;
        }

        try {
            // fast path
            boolean contains = inPredicate.getLiteralOptionSet().contains(newCompareExpr);
            if (contains) {
                return BooleanLiteral.TRUE;
            }
            if (inPredicate.optionsContainsNullLiteral()) {
                return NullLiteral.BOOLEAN_INSTANCE;
            }
            return BooleanLiteral.FALSE;
        } catch (Throwable t) {
            // slow path
            return super.visitInPredicate(inPredicate, context);
        }
    }

    @Override
    public Expression visit(Expression expr, Map<Slot, PartitionSlotInput> context) {
        expr = super.visit(expr, context);
        if (!(expr instanceof Literal)) {
            // just forward to fold constant rule
            return FoldConstantRuleOnFE.evaluate(expr, expressionRewriteContext);
        }
        return expr;
    }

    @Override
    public Expression visitSlot(Slot slot, Map<Slot, PartitionSlotInput> context) {
        // replace partition slot to literal
        PartitionSlotInput partitionSlotInput = context.get(slot);
        return partitionSlotInput == null ? slot : partitionSlotInput.result;
    }

    @Override
    public Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs) {
        if (rangePartitionEvaluator != null) {
            return rangePartitionEvaluator.evaluate(expression, currentInputs);
        }
        return expression.accept(this, currentInputs);
    }

    @Override
    public boolean isDefaultPartition() {
        return partitionItem.isDefaultPartition();
    }
}
