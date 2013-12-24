package org.apache.lucene.server.params;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.server.params.PolyType.PolyEntry;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/** Just pairs up the actual request with its type. */
public class Request {

  // Type describing the expected object
  private final StructType type;

  // A particular request:
  private final JSONObject params;

  private final Request parent;
  private final String name;

  public Request(Request parent, String name, JSONObject params, StructType type) {
    this.params = params;
    this.type = type;
    this.parent = parent;
    this.name = name;
  }

  public void clearParams() {
    params.clear();
  }

  public void clearParam(String param) {
    params.remove(param);
  }

  public StructType getType() {
    return type;
  }

  // nocommit remove after full cutover to PolyType
  public Request newType(StructType otherType) {
    return new Request(parent, name, params, otherType);
  }

  public boolean hasParam(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    return params.containsKey(name);
  }

  public Iterator<Map.Entry<String,Object>> getParams() {
    return params.entrySet().iterator();
  }

  public JSONObject getRawParams() {
    return params;
  }

  public String toString() {
    return params.toString();
  }

  public Object getAny(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof AnyType;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return p.defaultValue;
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return false;
      }
    } else {
      params.remove(name);
      p.type.validate(v);
      return v;
    }
  }

  public boolean getBoolean(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof BooleanType: "name \"" + name + "\" is not BooleanType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return ((Boolean) p.defaultValue).booleanValue();
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return false;
      }
    } else {
      try {
        p.type.validate(v);
      } catch (IllegalArgumentException iae) {
        fail(name, iae.getMessage(), iae);
      }
      params.remove(name);
      return ((Boolean) v).booleanValue();
    }
  }

  public float getFloat(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof FloatType: "name \"" + name + "\" is not FloatType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return ((Float) p.defaultValue).floatValue();
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return 0;
      }
    } else {
      try {
        p.type.validate(v);
      } catch (IllegalArgumentException iae) {
        fail(name, iae.getMessage(), iae);
      }
      params.remove(name);
      return ((Number) v).floatValue();
    }
  }

  public int getInt(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof IntType: "name \"" + name + "\" is not IntType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return ((Integer) p.defaultValue).intValue();
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return 0;
      }
    } else if (!(v instanceof Integer)) {
      fail(name, "expected Integer but got: " + v.getClass());
      // Dead code but compiler disagrees:
      return 0;
    } else {
      params.remove(name);
      return ((Integer) v).intValue();
    }
  }

  public long getLong(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof LongType: "name \"" + name + "\" is not LongType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return ((Long) p.defaultValue).longValue();
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return 0L;
      }
    } else if (!(v instanceof Long) && !(v instanceof Integer)) {
      fail(name, "expected Long but got: " + v.getClass());
      // Dead code but compiler disagrees:
      return 0L;
    } else {
      params.remove(name);
      return ((Number) v).longValue();
    }
  }

  public boolean isString(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    //assert p.type instanceof StringType: "name \"" + name + "\" is not StringType: got " + p.type;
    Object v = params.get(name);
    return v instanceof String;
  }

  // nocommit bad that enum values is not strongly typed
  // here ... maybe we need isEnumValue(name, X)?  having
  // else/if chain in the code can hide a sneaky bug

  public String getString(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert !(p.type instanceof EnumType);
    //assert p.type instanceof StringType: "name \"" + name + "\" is not StringType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return (String) p.defaultValue;
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return null;
      }
    } else {
      if (!(v instanceof String)) {
        fail(name, "expected String but got " + v.getClass());
      }
      params.remove(name);
      return (String) v;
    }
  }

  public String getEnum(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof EnumType: "name \"" + name + "\" is not EnumType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return (String) p.defaultValue;
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return null;
      }
    } else {
      try {
        p.type.validate(v);
      } catch (IllegalArgumentException iae) {
        fail(name, iae.getMessage(), iae);
      }
      params.remove(name);
      return (String) v;
    }
  }

  public static class PolyResult {
    public final String name;
    public final Request r;

    PolyResult(String name, Request r) {
      this.name = name;
      this.r = r;
    }
  }

  public PolyResult getPoly(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    assert p.type instanceof PolyType: "name \"" + name + "\" is not PolyType: got " + p.type;
    Object v = params.get(name);
    if (v == null) {
      fail(name, "required parameter \"" + name + "\" is missing");
      // dead code but compiler disagrees:
      return null;
    } else if (!(v instanceof String)) {
      fail(name, "expected String but got " + v);
      // dead code but compiler disagrees:
      return null;
    } else {
      PolyType pt = (PolyType) p.type;
      String value = (String) v;
      PolyEntry sub = pt.types.get(value);
      if (sub == null) {
        fail(name, "unrecognized value \"" + value + "\"; must be one of: " + pt.types.keySet());
      }
      params.remove(name);
      return new PolyResult((String) v, newType(sub.type));
    }
  }

  public Object getRaw(String name) {
    return params.get(name);
  }

  public Request getStruct(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter; valid params=" + type.params.keySet() + "; path=" + getPath();
    Type pType = p.type;
    if (pType instanceof WrapType) {
      pType = ((WrapType) pType).getWrappedType();
    }
    assert pType instanceof StructType: "name \"" + name + "\" is not StructType: got " + type;
    
    Object v = params.get(name);
    if (v == null) {
      fail(name, "required parameter \"" + name + "\" is missing");
      // dead code but compiler disagrees:
      return null;
    } else {
      // If the value is a String, and the type is Struct
      // that has a "class" param, pretend this was a Struct
      // with just the "class" param.  This is so user can
      // just do String (for the class name) instead of
      // struct with only "class" param:
      if (v instanceof String) {
        StructType st = (StructType) pType;
        if (st.params.containsKey("class") && st.params.get("class").type instanceof PolyType) {
          JSONObject o = new JSONObject();
          o.put("class", v);
          v = o;
          params.remove(name);
        }
      }

      if (!(v instanceof JSONObject)) {
        fail(name, "expected Object but got " + v.getClass());
      }

      // Don't remove, so that we can recurse and make sure
      // all structs had all their params visited too
      //params.remove(name);
      return new Request(this, name, (JSONObject) v, (StructType) pType);
    }
  }

  // nocommit hacky
  private StructType findStructType(Type t) {
    if (t instanceof StructType) {
      return (StructType) t;
    } else if (t instanceof ListType) {
      ListType lt = (ListType) t;
      if (lt.subType instanceof StructType) {
        return (StructType) lt.subType;
      } else if (lt.subType instanceof OrType) {
        for(Type t2 : ((OrType) lt.subType).types) {
          if (t2 instanceof StructType) {
            return (StructType) t2;
          }
        }
      }
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  public List<Object> getList(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    // nocommit make this stronger ... check the subTypes of OrType:
    //assert p.type instanceof ListType: "name \"" + name + "\" is not ListType: got " + p.type;
    assert p.type instanceof ListType || p.type instanceof OrType: "name \"" + name + "\" is not ListType: got " + p.type;
    Object v = params.get(name);
    if (v == null) {
      if (p.defaultValue != null) {
        // Fallback to default
        return (List<Object>) p.defaultValue;
      } else {
        fail(name, "required parameter \"" + name + "\" is missing");
        // dead code but compiler disagrees:
        return null;
      }
    } else {
      if (!(v instanceof JSONArray)) {
        fail(name, "expected array but got " + v.getClass());
      }
      List<Object> subs = new ArrayList<Object>();
      Iterator<Object> it = ((JSONArray) v).iterator();
      int idx = 0;
      while(it.hasNext()) {
        Object o = it.next();

        // If the value is a String, and the type is Struct
        // that has a "class" param, pretend this was a Struct
        // with just the "class" param.  This is so user can
        // just do String (for the class name) instead of
        // struct with only "class" param:
        if (o instanceof String && p.type instanceof ListType && ((ListType) p.type).subType instanceof StructType) {
          StructType st = (StructType) ((ListType) p.type).subType;
          if (st.params.containsKey("class") && st.params.get("class").type instanceof PolyType) {
            JSONObject o2 = new JSONObject();
            o2.put("class", o);
            o = o2;
            it.remove();
          }
        }

        if (o instanceof JSONObject) {
          StructType st = findStructType(p.type);
          if (st != null) {
            subs.add(new Request(this, name + "[" + idx + "]", (JSONObject) o, st));
          } else {
            subs.add(o);
          }
        } else {
          // nocommit make this work w/ OrType
          if (p.type instanceof ListType) {
            try {
              ((ListType) p.type).subType.validate(o);
            } catch (IllegalArgumentException iae) {
              fail(name, iae.getMessage(), iae);
            }
          }
          subs.add(o);
        }
        if (!(o instanceof JSONObject)) {
          // nocommit this is O(N^2)!!
          it.remove();
        }
        idx++;
      }
      return subs;
    }
  }

  public static boolean anythingLeft(JSONObject obj) {
    Iterator<Map.Entry<String,Object>> it = obj.entrySet().iterator();
    boolean anything = false;
    while(it.hasNext()) {
      Map.Entry<String,Object> ent = it.next();
      if (ent.getValue() instanceof JSONObject) {
        if (!anythingLeft((JSONObject) ent.getValue())) {
          it.remove();
        } else {
          anything = true;
        }
      } else if (ent.getValue() instanceof JSONArray) {
        Iterator<Object> it2 = ((JSONArray) ent.getValue()).iterator();
        while(it2.hasNext()) {
          Object obj2 = it2.next();
          if (obj2 instanceof JSONObject) {
            if (!anythingLeft((JSONObject) obj2)) {
              it2.remove();
            } else {
              anything = true;
            }
          }
        }
        if (((JSONArray) ent.getValue()).isEmpty()) {
          it.remove();
        } else {
          anything = true;
        }
      } else {
        anything = true;
      }
    }

    return anything;
  }

  private void buildPath(StringBuilder sb) {
    if (parent != null) {
      parent.buildPath(sb);
    }
    if (sb.length() > 0) {
      sb.append(" > ");
    }
    if (name != null) {
      sb.append(name);
    }
  }

  private String getPath() {
    StringBuilder sb = new StringBuilder();
    buildPath(sb);
    return sb.toString();
  }

  public void fail(String message) {
    fail(null, message, null);
  }

  public void fail(String param, String message) {
    fail(param, message, null);
  }

  public void fail(String param, String reason, Throwable cause) {
    StringBuilder sb = new StringBuilder();
    buildPath(sb);
    if (param != null) {
      /*
      Param p = type.params.get(param);
      if (p == null) {
        assert false: "name \"" + param + "\" is not a known parameter";
      } else {
      */
      if (sb.length() > 0) {
        sb.append(" > ");
      }
      sb.append(param);
      //}
    }

    RequestFailedException e = new RequestFailedException(this, param, sb.toString(), reason);
    if (cause != null) {
      e.initCause(cause);
    }

    throw e;
  }
}
