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

import static org.apache.solr.client.solrj.cloud.autoscaling.Variable.Type.FREEDISK;

public class VariableBase implements Variable {
  final Type varType;

  public VariableBase(Type type) {
    this.varType = type;
  }


  static boolean isIntegerEquivalent(Object val) {
    if (val instanceof Number) {
      Number number = (Number) val;
      return Math.ceil(number.doubleValue()) == Math.floor(number.doubleValue());
    } else if (val instanceof String) {
      try {
        double dval = Double.parseDouble((String) val);
        return Math.ceil(dval) == Math.floor(dval);
      } catch (NumberFormatException e) {
        return false;
      }
    } else {
      return false;
    }

  }

  @SuppressWarnings({"unchecked"})
  static Variable loadImpl(Meta meta, Type t) {
    @SuppressWarnings({"rawtypes"})
    Class implementation = meta.implementation();
    if (implementation == void.class) implementation = VariableBase.class;
    try {
      return (Variable) implementation.getConstructor(Type.class).newInstance(t);
    } catch (Exception e) {
      throw new RuntimeException("Unable to instantiate: " + implementation.getName(), e);
    }
  }

  public static Long parseLong(String name, Object val) {
    if (val == null) return null;
    if (val instanceof Long) return (Long) val;
    Number num = null;
    if (val instanceof String) {
      try {
        num = Long.parseLong(((String) val).trim());
      } catch (NumberFormatException e) {
        try {
          num = Double.parseDouble((String) val);
        } catch (NumberFormatException e1) {
          throw new RuntimeException(name + ": " + val + "not a valid number", e);
        }
      }

    } else if (val instanceof Number) {
      num = (Number) val;
    }

    if (num != null) {
      return num.longValue();
    }
    throw new RuntimeException(name + ": " + val + "not a valid number");
  }

  public static Double parseDouble(String name, Object val) {
    if (val == null) return null;

    if (val instanceof Double) return (Double) val;
    Number num = null;
    if (val instanceof String) {
      try {
        num = Double.parseDouble((String) val);
      } catch (NumberFormatException e) {
        throw new RuntimeException(name + ": " + val + "not a valid number", e);
      }

    } else if (val instanceof Number) {
      num = (Number) val;
    }

    if (num != null) {
      return num.doubleValue();
    }
    throw new RuntimeException(name + ": " + val + "not a valid number");
  }

  @Override
  public Object validate(String name, Object val, boolean isRuleVal) {

    if (!isRuleVal && "".equals(val) && this.varType.type != String.class) val = -1;
    if (name == null) name = this.varType.tagName;
    if (varType.type == Double.class) {
      Double num = parseDouble(name, val);
      if (isRuleVal) {
        if (varType.min != null)
          if (Double.compare(num, varType.min.doubleValue()) == -1)
            throw new RuntimeException(name + ": " + val + " must be greater than " + varType.min);
        if (varType.max != null)
          if (Double.compare(num, varType.max.doubleValue()) == 1)
            throw new RuntimeException(name + ": " + val + " must be less than " + varType.max);
      }
      return num;
    } else if (varType.type == Long.class) {
      Long num = parseLong(name, val);
      if (isRuleVal) {
        if (varType.min != null)
          if (num < varType.min.longValue())
            throw new RuntimeException(name + ": " + val + " must be greater than " + varType.min);
        if (varType.max != null)
          if (num > varType.max.longValue())
            throw new RuntimeException(name + ": " + val + " must be less than " + varType.max);
      }
      return num;
    } else if (varType.type == String.class) {
      return val;
    } else {
      throw new RuntimeException("Invalid type ");
    }
  }

  public static class TotalDiskVariable extends VariableBase {
    public TotalDiskVariable(Type type) {
      super(type);
    }

    @Override
    public Object convertVal(Object val) {
      return FREEDISK.convertVal(val);
    }
  }

  public static class CoreIndexSizeVariable extends VariableBase {
    public CoreIndexSizeVariable(Type type) {
      super(type);
    }

    @Override
    public Object convertVal(Object val) {
      return FREEDISK.convertVal(val);
    }
  }

  public static class LazyVariable extends VariableBase {
    public LazyVariable(Type type) {
      super(type);
    }
  }

  public static class DiskTypeVariable extends VariableBase {
    public DiskTypeVariable(Type type) {
      super(type);
    }

  }

  public static class FreeDiskVariable extends VariableBase {

    public FreeDiskVariable(Type type) {
      super(type);
    }

    @Override
    public Object convertVal(Object val) {
      Number value = (Number) super.validate(FREEDISK.tagName, val, false);
      if (value != null) {
        value = value.doubleValue() / 1024.0d / 1024.0d / 1024.0d;
      }
      return value;
    }



  }
}
