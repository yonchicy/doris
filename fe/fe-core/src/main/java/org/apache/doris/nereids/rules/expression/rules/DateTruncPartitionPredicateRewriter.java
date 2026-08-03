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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Rewrite predicates on the source column of a date_trunc list partition to predicates on partition keys.
 *
 * <p>For {@code F = date_trunc(unit)}, the rewritten predicates are:
 * <ul>
 *   <li>{@code col >/>= value} becomes {@code F(col) >= F(value)}</li>
 *   <li>{@code col </<= value} becomes {@code F(col) < F(value) + 1 unit}</li>
 *   <li>{@code col =/<=> value} becomes {@code F(col) =/<=> F(value)}</li>
 *   <li>{@code col IN (values)} becomes {@code F(col) IN (F(values))}</li>
 * </ul>
 *
 * <p>These predicates are supersets of the source predicates. They may select an extra boundary partition, but
 * never prune a partition that can contain a matching row. A negative predicate cannot safely exclude the bucket
 * containing its literal, so predicates below {@link Not} conservatively become {@code TRUE}. Predicates that do
 * not compare the source slot directly, such as predicates on a cast of the slot, also become {@code TRUE}.
 */
public class DateTruncPartitionPredicateRewriter extends DefaultExpressionRewriter<Void> {
    private static final String DATE_TRUNC = "date_trunc";

    private final Map<String, DateTruncInfo> dateTruncInfoByColumn = new HashMap<>();
    private final ExpressionRewriteContext expressionRewriteContext;
    private boolean hasUnsupportedPartitionFunction;

    private DateTruncPartitionPredicateRewriter(
            List<Expr> partitionExprs, CascadesContext cascadesContext) {
        this.expressionRewriteContext = new ExpressionRewriteContext(cascadesContext);
        for (Expr partitionExpr : partitionExprs) {
            if (!(partitionExpr instanceof FunctionCallExpr)) {
                continue;
            }

            FunctionCallExpr function = (FunctionCallExpr) partitionExpr;
            if (!DATE_TRUNC.equalsIgnoreCase(function.getFnName().getFunction())) {
                hasUnsupportedPartitionFunction = true;
                continue;
            }

            List<Expr> arguments = function.getParams().exprs();
            Preconditions.checkState(arguments.size() == 2,
                    "date_trunc partition expression must have two arguments");
            Preconditions.checkState(arguments.get(0) instanceof SlotRef,
                    "the first date_trunc partition expression argument must be a slot");
            Preconditions.checkState(arguments.get(1) instanceof StringLiteral,
                    "the second date_trunc partition expression argument must be a string literal");

            String columnName = ((SlotRef) arguments.get(0)).getColumnName();
            String timeUnit = ((StringLiteral) arguments.get(1)).getStringValue();
            DateTruncInfo previous = dateTruncInfoByColumn.put(
                    normalizeName(columnName), new DateTruncInfo(timeUnit));
            Preconditions.checkState(previous == null,
                    "a partition column can have only one date_trunc partition expression");
        }
    }

    /** Rewrite predicate according to date_trunc partition expressions. */
    public static Expression rewrite(Expression predicate, List<Expr> partitionExprs,
            CascadesContext cascadesContext) {
        DateTruncPartitionPredicateRewriter rewriter =
                new DateTruncPartitionPredicateRewriter(partitionExprs, cascadesContext);
        if (rewriter.hasUnsupportedPartitionFunction) {
            return BooleanLiteral.TRUE;
        }
        return predicate.accept(rewriter, null);
    }

    @Override
    public Expression visit(Expression expression, Void context) {
        Expression rewritten = super.visit(expression, context);
        if (rewritten instanceof And || rewritten instanceof Or) {
            return rewritten;
        }
        return containsDateTruncPartitionSlot(rewritten) ? BooleanLiteral.TRUE : rewritten;
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate comparison, Void context) {
        MatchedSlot left = matchSlot(comparison.left());
        MatchedSlot right = matchSlot(comparison.right());
        if (left == null && right == null) {
            return containsDateTruncPartitionSlot(comparison) ? BooleanLiteral.TRUE : comparison;
        }
        if (left != null && right != null) {
            return BooleanLiteral.TRUE;
        }

        ComparisonPredicate normalizedComparison = comparison;
        MatchedSlot matchedSlot = left;
        if (matchedSlot == null) {
            normalizedComparison = comparison.commute();
            matchedSlot = right;
        }

        Expression value = normalizedComparison.right();
        if (value instanceof NullLiteral) {
            return comparison;
        }
        if (!(value instanceof DateLiteral)) {
            return BooleanLiteral.TRUE;
        }

        Optional<DateLiteral> truncatedValue = matchedSlot.dateTruncInfo.truncate((DateLiteral) value);
        if (!truncatedValue.isPresent()) {
            return BooleanLiteral.TRUE;
        }

        Expression partitionKey = matchedSlot.dateTruncInfo.apply(matchedSlot.slot);
        DateLiteral lowerBound = truncatedValue.get();
        if (normalizedComparison instanceof GreaterThan
                || normalizedComparison instanceof GreaterThanEqual) {
            return new GreaterThanEqual(partitionKey, lowerBound, comparison.isInferred());
        } else if (normalizedComparison instanceof LessThan
                || normalizedComparison instanceof LessThanEqual) {
            Optional<DateLiteral> upperBound = matchedSlot.dateTruncInfo.next(lowerBound);
            return upperBound.<Expression>map(bound ->
                    new LessThan(partitionKey, bound, comparison.isInferred()))
                    .orElse(BooleanLiteral.TRUE);
        } else if (normalizedComparison instanceof EqualTo) {
            return new EqualTo(partitionKey, lowerBound, comparison.isInferred());
        } else if (normalizedComparison instanceof NullSafeEqual) {
            return new NullSafeEqual(partitionKey, lowerBound, comparison.isInferred());
        }
        return BooleanLiteral.TRUE;
    }

    @Override
    public Expression visitInPredicate(InPredicate inPredicate, Void context) {
        MatchedSlot matchedSlot = matchSlot(inPredicate.getCompareExpr());
        if (matchedSlot == null) {
            return containsDateTruncPartitionSlot(inPredicate) ? BooleanLiteral.TRUE : inPredicate;
        }

        ImmutableList.Builder<Expression> options =
                ImmutableList.builderWithExpectedSize(inPredicate.getOptions().size());
        for (Expression option : inPredicate.getOptions()) {
            if (option instanceof NullLiteral) {
                options.add(option);
            } else if (option instanceof DateLiteral) {
                Optional<DateLiteral> truncated = matchedSlot.dateTruncInfo.truncate((DateLiteral) option);
                if (!truncated.isPresent()) {
                    return BooleanLiteral.TRUE;
                }
                options.add(truncated.get());
            } else {
                return BooleanLiteral.TRUE;
            }
        }
        return new InPredicate(
                matchedSlot.dateTruncInfo.apply(matchedSlot.slot), options.build(), inPredicate.isInferred());
    }

    @Override
    public Expression visitNot(Not not, Void context) {
        if (containsDateTruncPartitionSlot(not)) {
            return BooleanLiteral.TRUE;
        }
        return super.visitNot(not, context);
    }

    private MatchedSlot matchSlot(Expression expression) {
        if (!(expression instanceof Slot)) {
            return null;
        }

        Slot slot = (Slot) expression;
        DateTruncInfo dateTruncInfo = dateTruncInfoByColumn.get(normalizeName(slot.getName()));
        return dateTruncInfo == null ? null : new MatchedSlot(slot, dateTruncInfo);
    }

    private boolean containsDateTruncPartitionSlot(Expression expression) {
        return expression.anyMatch(child -> child instanceof Slot
                && dateTruncInfoByColumn.containsKey(normalizeName(((Slot) child).getName())));
    }

    private static String normalizeName(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private class DateTruncInfo {
        private final String timeUnit;
        private final VarcharLiteral timeUnitLiteral;

        private DateTruncInfo(String timeUnit) {
            this.timeUnit = timeUnit.toLowerCase(Locale.ROOT);
            this.timeUnitLiteral = new VarcharLiteral(this.timeUnit);
        }

        private Expression apply(Expression date) {
            return TypeCoercionUtils.processBoundFunction(new DateTrunc(date, timeUnitLiteral));
        }

        private Optional<DateLiteral> truncate(DateLiteral literal) {
            Expression truncated = FoldConstantRuleOnFE.evaluate(
                    apply(literal), expressionRewriteContext);
            return truncated instanceof DateLiteral
                    ? Optional.of((DateLiteral) truncated)
                    : Optional.empty();
        }

        private Optional<DateLiteral> next(DateLiteral literal) {
            try {
                Expression next;
                switch (timeUnit) {
                    case "year":
                        next = literal.plusYears(1);
                        break;
                    case "quarter":
                        next = literal.plusMonths(3);
                        break;
                    case "month":
                        next = literal.plusMonths(1);
                        break;
                    case "week":
                        next = literal.plusWeeks(1);
                        break;
                    case "day":
                        next = literal.plusDays(1);
                        break;
                    case "hour":
                        next = literal instanceof DateTimeLiteral
                                ? ((DateTimeLiteral) literal).plusHours(1) : literal.plusDays(1);
                        break;
                    case "minute":
                        next = literal instanceof DateTimeLiteral
                                ? ((DateTimeLiteral) literal).plusMinutes(1) : literal.plusDays(1);
                        break;
                    case "second":
                        next = literal instanceof DateTimeLiteral
                                ? ((DateTimeLiteral) literal).plusSeconds(1) : literal.plusDays(1);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported date_trunc time unit: " + timeUnit);
                }
                Preconditions.checkState(next instanceof DateLiteral,
                        "the next date_trunc boundary must be a date literal");
                return Optional.of((DateLiteral) next);
            } catch (AnalysisException e) {
                return Optional.empty();
            }
        }
    }

    private static class MatchedSlot {
        private final Slot slot;
        private final DateTruncInfo dateTruncInfo;

        private MatchedSlot(Slot slot, DateTruncInfo dateTruncInfo) {
            this.slot = slot;
            this.dateTruncInfo = dateTruncInfo;
        }
    }
}
