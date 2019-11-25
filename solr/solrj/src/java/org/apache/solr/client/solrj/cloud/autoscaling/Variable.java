/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.client.solrj.cloud.autoscaling;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.solr.common.cloud.rule.ImplicitSnitch;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableMap;


/**
 * A Variable Type used in Autoscaling policy rules. Each variable type may have unique implementation
 * of functionalities
 */
public interface Variable {
  String NULL = "";
  String coreidxsize = "INDEX.sizeInGB";

  default boolean match(Object inputVal, Operand op, Object val, String name, Row row) {
    return op.match(val, validate(name, inputVal, false)) == Clause.TestStatus.PASS;
  }

  default Object convertVal(Object val) {
    return val;
  }

  default void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector, boolean strictMode) {
  }

  default boolean addViolatingReplicas(Violation.Ctx ctx) {
    return false;
  }

  void getSuggestions(Suggestion.Ctx ctx);

  /**When a non constant value is used in a variable, the actual value needs to be computed at the runtime
   *
   */
  default Object computeValue(Policy.Session session, Condition condition, String collection, String shard, String node) {
    return condition.val;
  }

  default void computeDeviation(Policy.Session session, double[] deviations, ReplicaCount replicaCount, SealedClause sealedClause) {
    if (deviations != null) {
      Number actualCount = replicaCount.getVal(sealedClause.type);
      if(sealedClause.replica.val instanceof RangeVal) {
        Double realDelta = ((RangeVal) sealedClause.replica.val).realDelta(actualCount.doubleValue());
        realDelta = sealedClause.isReplicaZero() ? -1 * realDelta : realDelta;
        deviations[0] += Math.abs(realDelta);
      }
    }
  }

  int compareViolation(Violation v1, Violation v2);

  default void projectRemoveReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector) {
  }

  default String postValidate(Condition condition) {
    return null;
  }

  default Operand getOperand(Operand expected, Object strVal, ComputedType computedType) {
    return expected;
  }

  Object validate(String name, Object val, boolean isRuleVal);

  /**
   * Type details of each variable in policies
   */
  public enum Type implements Variable {
    @Meta(name = "withCollection",
        type = String.class,
        isNodeSpecificVal = true,
        implementation = WithCollectionVariable.class)
    WITH_COLLECTION(),

    @Meta(name = "collection",
        type = String.class)
    COLL,
    @Meta(
        name = "shard",
        type = String.class,
        wildCards = {Policy.EACH, Policy.ANY})
    SHARD(),

    @Meta(name = "replica",
        type = Double.class,
        min = 0, max = -1,
        implementation = ReplicaVariable.class,
        computedValues = {ComputedType.EQUAL, ComputedType.PERCENT, ComputedType.ALL})
    REPLICA,
    @Meta(name = ImplicitSnitch.PORT,
        type = Long.class,
        min = 1,
        max = 65535,
        supportArrayVals = true,
        wildCards = Policy.EACH
    )
    PORT,
    @Meta(name = "ip_1",
        type = Long.class,
        min = 0,
        max = 255,
        supportArrayVals = true,
        wildCards = Policy.EACH)
    IP_1,
    @Meta(name = "ip_2",
        type = Long.class,
        min = 0,
        max = 255,
        supportArrayVals = true,
        wildCards = Policy.EACH)
    IP_2,
    @Meta(name = "ip_3",
        type = Long.class,
        min = 0,
        max = 255,
        supportArrayVals = true,
        wildCards = Policy.EACH)
    IP_3,
    @Meta(name = "ip_4",
        type = Long.class,
        min = 0,
        max = 255,
        supportArrayVals = true,
        wildCards = Policy.EACH)
    IP_4,
    @Meta(name = ImplicitSnitch.DISK,
        type = Double.class,
        min = 0,
        isNodeSpecificVal = true,
        associatedPerReplicaValue = Variable.coreidxsize,
        associatedPerNodeValue = "totaldisk",
        implementation = FreeDiskVariable.class,
        computedValues = ComputedType.PERCENT)
    FREEDISK,

    @Meta(name = "totaldisk",
        type = Double.class,
        isHidden = true, implementation = VariableBase.TotalDiskVariable.class)
    TOTALDISK,

    @Meta(name = Variable.coreidxsize,
        type = Double.class,
        isNodeSpecificVal = true,
        isHidden = true,
        min = 0,
        implementation = VariableBase.CoreIndexSizeVariable.class,
        metricsKey = "INDEX.sizeInBytes")
    CORE_IDX,
    @Meta(name = ImplicitSnitch.NODEROLE,
        type = String.class,
        enumVals = "overseer")
    NODE_ROLE,

    @Meta(name = ImplicitSnitch.CORES,
        type = Double.class,
        min = 0, max = -1,
        computedValues = {ComputedType.EQUAL, ComputedType.PERCENT},
        implementation = CoresVariable.class)
    CORES,

    @Meta(name = ImplicitSnitch.SYSLOADAVG,
        type = Double.class,
        min = 0,
        max = 100,
        isNodeSpecificVal = true)
    SYSLOADAVG,

    @Meta(name = ImplicitSnitch.HEAPUSAGE,
        type = Double.class,
        min = 0,
        isNodeSpecificVal = true)
    HEAPUSAGE,
    @Meta(name = "NUMBER",
        type = Long.class,
        min = 0)
    NUMBER,

    @Meta(name = "host",
        type = String.class,
        wildCards = Policy.EACH,
        supportArrayVals = true)
    HOST,

    @Meta(name = "STRING",
        type = String.class,
        wildCards = Policy.EACH,
        supportArrayVals = true
    )
    SYSPROP,

    @Meta(name = "node",
        type = String.class,
        isNodeSpecificVal = true,
        wildCards = {Policy.ANY, Policy.EACH},
        implementation = NodeVariable.class,
        supportArrayVals = true)
    NODE,

    @Meta(name = "LAZY",
        type = void.class,
        implementation = VariableBase.LazyVariable.class)
    LAZY,

    @Meta(name = ImplicitSnitch.DISKTYPE,
        type = String.class,
        enumVals = {"ssd", "rotational"},
        implementation = VariableBase.DiskTypeVariable.class,
        supportArrayVals = true)
    DISKTYPE;

    public final String tagName;
    @SuppressWarnings("rawtypes")
    public final Class type;
    public Meta meta;

    public final Set<String> vals;
    public final Number min;
    public final Number max;
    public final Boolean additive;
    public final Set<String> wildCards;
    public final String perReplicaValue;
    public final Set<String> associatedPerNodeValues;
    public final String metricsAttribute;
    public final Set<ComputedType> supportedComputedTypes;
    final Variable impl;


    @SuppressWarnings({"unchecked", "rawtypes"})
    Type() {
      try {
        meta = Type.class.getField(name()).getAnnotation(Meta.class);
        if (meta == null) {
          throw new RuntimeException("Invalid type, should have a @Meta annotation " + name());
        }
      } catch (NoSuchFieldException e) {
        //cannot happen
      }
      impl = VariableBase.loadImpl(meta, this);

      this.tagName = meta.name();
      this.type = meta.type();

      this.vals = readSet(meta.enumVals());
      this.max = readNum(meta.max());
      this.min = readNum(meta.min());
      this.perReplicaValue = readStr(meta.associatedPerReplicaValue());
      this.associatedPerNodeValues = readSet(meta.associatedPerNodeValue());
      this.additive = meta.isAdditive();
      this.metricsAttribute = readStr(meta.metricsKey());
      this.supportedComputedTypes = meta.computedValues()[0] == ComputedType.NULL ?
          emptySet() :
          Set.of(meta.computedValues());
      this.wildCards = readSet(meta.wildCards());

    }

    public String getTagName() {
      return meta.name();
    }

    private String readStr(String s) {
      return NULL.equals(s) ? null : s;
    }

    private Number readNum(double v) {
      return v == -1 ? null :
          (Number) validate(null, v, true);
    }

    Set<String> readSet(String[] vals) {
      if (NULL.equals(vals[0])) return emptySet();
      return Set.of(vals);
    }

    @Override
    public void getSuggestions(Suggestion.Ctx ctx) {
      impl.getSuggestions(ctx);
    }

    @Override
    public boolean addViolatingReplicas(Violation.Ctx ctx) {
      return impl.addViolatingReplicas(ctx);
    }

    public Operand getOperand(Operand expected, Object val, ComputedType computedType) {
      return impl.getOperand(expected, val, computedType);
    }


    public Object convertVal(Object val) {
      return impl.convertVal(val);
    }

    public String postValidate(Condition condition) {
      return impl.postValidate(condition);
    }

    public Object validate(String name, Object val, boolean isRuleVal) {
      return impl.validate(name, val, isRuleVal);
    }

    /**
     * Simulate a replica addition to a node in the cluster
     */
    public void projectAddReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector, boolean strictMode) {
      impl.projectAddReplica(cell, ri, opCollector, strictMode);
    }

    public void projectRemoveReplica(Cell cell, ReplicaInfo ri, Consumer<Row.OperationInfo> opCollector) {
      impl.projectRemoveReplica(cell, ri, opCollector);
    }

    @Override
    public int compareViolation(Violation v1, Violation v2) {
      return impl.compareViolation(v1, v2);
    }

    @Override
    public Object computeValue(Policy.Session session, Condition condition, String collection, String shard, String node) {
      return impl.computeValue(session, condition, collection, shard, node);
    }

    @Override
    public void computeDeviation(Policy.Session session, double[] deviations, ReplicaCount replicaCount, SealedClause sealedClause) {
      impl.computeDeviation(session, deviations, replicaCount, sealedClause);
    }


    @Override
    public boolean match(Object inputVal, Operand op, Object val, String name, Row row) {
      return impl.match(inputVal, op, val, name, row);
    }

    private static final Map<String, Type> typeByNameMap;
    static {
      HashMap<String, Type> m = new HashMap<>();
      for (Type t : Type.values()) {
        m.put(t.tagName, t);
      }
      typeByNameMap = unmodifiableMap(m);
    }
    static Type get(String name) {
      return typeByNameMap.get(name);
    }
  }

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  @interface Meta {
    String name();

    @SuppressWarnings("rawtypes")
    Class type();

    String[] associatedPerNodeValue() default NULL;

    String associatedPerReplicaValue() default NULL;

    String[] enumVals() default NULL;

    String[] wildCards() default NULL;

    boolean isNodeSpecificVal() default false;

    boolean isHidden() default false;

    boolean isAdditive() default true;

    double min() default -1d;

    double max() default -1d;

    boolean supportArrayVals() default false;

    String metricsKey() default NULL;

    @SuppressWarnings("rawtypes")
    Class implementation() default void.class;

    ComputedType[] computedValues() default ComputedType.NULL;
  }
}
