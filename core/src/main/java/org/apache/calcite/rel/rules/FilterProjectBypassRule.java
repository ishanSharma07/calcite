/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Permutation;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that matches a Filter Project Aggregate chain and tries to bypass the project
 * by removing it and directly applying filter on the aggregate columns.
 **/
public class FilterProjectBypassRule
    extends RelRule<FilterProjectBypassRule.Config>
    implements TransformationRule {

  /**Creates a FilterProjectBypassRule.**/
  protected FilterProjectBypassRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final Project projectRel = call.rel(1);
    final Aggregate aggRel = call.rel(2);

    if (!canBypass(call)) {
      return;
    }
    final RexCall oldCondition = (RexCall) filterRel.getCondition();
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();

    RexInputRef origFilterRef = (RexInputRef) ((RexCall) filterRel.getCondition()).
        getOperands().get(0);
    /**Build new condition that takes effect after Project is removed. **/
    Integer targetIndex = projectRel.getMapping().getTarget(origFilterRef.getIndex());
    RexInputRef targetFilterRef = new RexInputRef(targetIndex, origFilterRef.getType());
    List<RexNode> newOperands = new ArrayList<RexNode>();
    newOperands.add(targetFilterRef);
    newOperands.add(oldCondition.getOperands().get(1));
    /**Update the input ref. **/
    newOperands.set(0, targetFilterRef);
    RexCall newCondition = oldCondition.clone(oldCondition.getType(), newOperands);
    /**Build and transform the tree. **/
    final RelBuilder builder = call.builder();
    final Filter newFilter = filterRel.copy(filterRel.getTraitSet(), projectRel.getInput(),
        newCondition);
    RelNode rel = builder.push(newFilter).build();
    call.transformTo(rel);
  }

  private static boolean canBypass(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final Project projectRel = call.rel(1);
    final Aggregate aggRel = call.rel(2);

    final List<RexNode> conditions =
        RelOptUtil.conjunctions(filterRel.getCondition());

    /** Currently only supports a Filter with a single condition where the left operand is an
     * input ref. **/
    if (conditions.size() != 1) {
      return false;
    }
    RexNode filterCondition = filterRel.getCondition();
    if (!(filterCondition instanceof RexCall)) {
      return false;
    }
    RexCall condition = (RexCall) filterCondition;
    if (!(condition.getOperands().get(0) instanceof RexInputRef)) {
      return false;
    }
    /** The project can only be bypassable if and only if it simply permutes the input column
     * and does not add any new column (for instance because of an arithmetic projection and so on
     * and so forth). **/
    Permutation permutation = projectRel.getPermutation();
    if (permutation == null) {
      return false;
    }
    return true;
  }

  /**Rule Configuration. **/
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(Filter.class, Project.class, Aggregate.class);
    @Override default FilterProjectBypassRule toRule() {
      return new FilterProjectBypassRule(this);
    }

    /**Defines an operand tree for the given 3 classes.**/
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends Project> projectClass,
        Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b0 ->
          b0.operand(filterClass).oneInput(b1 ->
              b1.operand(projectClass).oneInput(b2 ->
                  b2.operand(aggregateClass).anyInputs())))
          .as(Config.class);
    }
  }
}
