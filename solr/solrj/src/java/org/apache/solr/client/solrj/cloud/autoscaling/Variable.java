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
  default Object convertVal(Object val) {
    return val;
  }
  Object validate(String name, Object val, boolean isRuleVal);

  /**
   * Type details of each variable in policies
   */
  enum Type implements Variable {

    @Meta(name = ImplicitSnitch.DISK,
        type = Double.class,
        min = 0,
        associatedPerReplicaValue = Variable.coreidxsize,
        associatedPerNodeValue = "totaldisk",
            implementation = VariableBase.FreeDiskVariable.class
)
    FREEDISK,

    @Meta(name = "totaldisk",
        type = Double.class,
         implementation = VariableBase.TotalDiskVariable.class)
    TOTALDISK,

    @Meta(name = Variable.coreidxsize,
        type = Double.class,
        min = 0,
        implementation = VariableBase.CoreIndexSizeVariable.class,
        metricsKey = "INDEX.sizeInBytes")
    CORE_IDX;

    public final String tagName;
    @SuppressWarnings({"rawtypes"})
    public final Class type;
    public Meta meta;

    public final Number min;
    public final Number max;
    public final String perReplicaValue;
    public final Set<String> associatedPerNodeValues;
    public final String metricsAttribute;
    final Variable impl;


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

      this.max = readNum(meta.max());
      this.min = readNum(meta.min());
      this.perReplicaValue = readStr(meta.associatedPerReplicaValue());
      this.associatedPerNodeValues = readSet(meta.associatedPerNodeValue());
      this.metricsAttribute = readStr(meta.metricsKey());


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

    public Object convertVal(Object val) {
      return impl.convertVal(val);
    }


    public Object validate(String name, Object val, boolean isRuleVal) {
      return impl.validate(name, val, isRuleVal);
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

    @SuppressWarnings({"rawtypes"})
    Class type();

    String[] associatedPerNodeValue() default NULL;

    String associatedPerReplicaValue() default NULL;

    double min() default -1d;

    double max() default -1d;

    String metricsKey() default NULL;

    @SuppressWarnings({"rawtypes"})
    Class implementation() default void.class;

  }
}
