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

// nocommit instead of removing as we getXXX, we could do a
// Set<String> seen?
//   - would close the loophole of foobar={} not being detected
//   - would allow us to lookup same param more than once

/** Pairs up the actual parameters with its type.  For
 *  complex requests, e.g. {@code search}, this is used
 *  recursively.  For example, the top-most Request is
 *  created, but then when a sub-struct parameter is
 *  retrieved with {@link #getStruct}, that returns another
 *  {@code Request} wrapping that value. */

public class Request {

  /** Type describing the expected object. */
  private final StructType type;

  /** The actual request parameters. */
  private final JSONObject params;

  /** Parent, if this is a sub Request, else null for the
   *  top-level request.  This is used for back-trace for
   *  error reporting. */
  private final Request parent;

  /** Our parameter name from our parent, or null if we are
   *  a top request.  This is used for back-trace for error
   *  reporting. */
  private final String name;

  /** Creates this. */
  public Request(Request parent, String name, JSONObject params, StructType type) {
    this.params = params;
    this.type = type;
    this.parent = parent;
    this.name = name;
  }

  /** Creates Request from another Request, changing the
   *  struct type. */
  public Request(Request other, StructType type) {
    this.params = other.params;
    this.type = type;
    this.parent = other.parent;
    this.name = other.name;
  }

  /** Clears all parameters. */
  public void clearParams() {
    params.clear();
  }

  /** Clear a specific parameter. */
  public void clearParam(String param) {
    params.remove(param);
  }

  /** Get the type for this request. */
  public StructType getType() {
    return type;
  }

  /** True if this param was specified. */
  public boolean hasParam(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    return params.containsKey(name);
  }

  /** Returns an iterator over all parameters and their
   *  values. */
  public Iterator<Map.Entry<String,Object>> getParams() {
    return params.entrySet().iterator();
  }

  /** Returns the parameters. */
  public JSONObject getRawParams() {
    return params;
  }

  @Override
  public String toString() {
    return params.toString();
  }

  /** Returns the raw (un type cast) value for this
   *  parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter.*/
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

  /** Retrieve a boolean parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
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

  /** Retrieve a float parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
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

  /** Retrieve a double parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
  public double getDouble(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    // assert p.type instanceof FloatType: "name \"" + name + "\" is not FloatType: got " + p.type;

    Object v = params.get(name);
    if (v == null) {
      // Request didn't provide it:
      if (p.defaultValue != null) {
        // Fallback to default
        return ((Number) p.defaultValue).doubleValue();
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
      return ((Number) v).doubleValue();
    }
  }

  /** Retrieve an int parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
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

  /** Retrieve a long parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
  public long getLong(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    //assert p.type instanceof LongType: "name \"" + name + "\" is not LongType: got " + p.type;

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

  /** True if the parameter is a string value. */
  public boolean isString(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    Object v = params.get(name);
    return v instanceof String;
  }

  /** Retrieve a string parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
  public String getString(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter";
    // Use getEnum instead:
    assert !(p.type instanceof EnumType);

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
      if ((v instanceof String) == false) {
        fail(name, "expected String but got " + toSimpleString(v.getClass()));
      }
      try {
        p.type.validate(v);
      } catch (IllegalArgumentException iae) {
        fail(name, iae.getMessage(), iae);
      }
      params.remove(name);
      return (String) v;
    }
  }

  private static String toSimpleString(Class<?> cl) {
    return cl.getSimpleName();
  }

  /** Retrieve an enum parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
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
      // Make sure the value is valid for this enum:
      try {
        p.type.validate(v);
      } catch (IllegalArgumentException iae) {
        fail(name, iae.getMessage(), iae);
      }
      params.remove(name);
      return (String) v;
    }
  }

  /** A result returned from {@link #getPoly}. */
  public static class PolyResult {
    /** The name of the poly parameter. */
    public final String name;

    /** The new request, cast to the poly sub type */
    public final Request r;

    /** Sole constructor. */
    PolyResult(String name, Request r) {
      this.name = name;
      this.r = r;
    }
  }

  /** Retrieve a poly typed parameter.  Once this is called
   *  for a given parameter it cannot be called again on
   *  that parameter. */
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
      return new PolyResult((String) v, new Request(parent, name, params, sub.type));
    }
  }

  /** Retrieve the raw object for a parameter, or null if
   *  the parameter was not specified.  This can be called
   *  multiple types for a given parameter. */
  public Object getRaw(String name) {
    return params.get(name);
  }

  /** Returns the raw object, and removes the binding. */
  public Object getAndRemoveRaw(String name) {
    Object o = params.get(name);
    params.remove(name);
    return o;
  }

  /** Retrieve a struct parameter.  This can be called
   *  multiple times for a given parameter name. */
  public Request getStruct(String name) {
    Param p = type.params.get(name);
    assert p != null: "name \"" + name + "\" is not a known parameter; valid params=" + type.params.keySet() + "; path=" + getPath();
    Type pType = p.type;
    if (pType instanceof WrapType) {
      pType = ((WrapType) pType).getWrappedType();
    }
    if (pType instanceof StructType == false) {
      pType = findStructType(pType);
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

      // nocommit does this mean we fail to detect when a
      // whole extra struct was specified

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
    } else if (t instanceof OrType) {
      OrType ot = (OrType) t;
      for(Type t2 : ot.types) {
        if (t2 instanceof StructType) {
          return (StructType) t2;
        }
      }
    }

    return null;
  }

  /** Retrieve a list parameter.  Once this is called for a
   *  given parameter it cannot be called again on that
   *  parameter. */
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
              fail(name + "[" + idx + "]", iae.getMessage(), iae);
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

  /** True if this request has any bindings, excluding
   *  empty containers. */
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

  /** Throws a {@link RequestFailedException} with the
   *  provided message. */
  public void fail(String message) {
    fail(null, message, null);
  }

  /** Throws a {@link RequestFailedException} with the
   *  provided parameter and message. */
  public void fail(String param, String message) {
    fail(param, message, null);
  }

  /** Throws a {@link RequestFailedException} with the
   *  provided parameter and message. */
  public void fail(String message, Throwable cause) {
    fail(null, message, cause);
  }

  /** Throws {@link RequestFailedException} when the wrong
   *  class was encountered. */
  public void failWrongClass(String param, String reason, Object thingy) {
    fail(param, reason + "; got: " + thingy.getClass());
  }

  /** Throws a {@link RequestFailedException} with the
   *  provided parameter and message and original cause. */
  public void fail(String param, String reason, Throwable cause) {
    StringBuilder sb = new StringBuilder();
    buildPath(sb);
    if (param != null) {
      Param p = type.params.get(param);

      // Exempt param[N]:
      if (p == null && param.indexOf('[') == -1) {
        // BUG:
        assert false: "name \"" + param + "\" is not a known parameter";
      }

      if (sb.length() > 0) {
        sb.append(" > ");
      }
      sb.append(param);
    }

    RequestFailedException e = new RequestFailedException(this, param, sb.toString(), reason);
    if (cause != null) {
      e.initCause(cause);
    }

    throw e;
  }
}
