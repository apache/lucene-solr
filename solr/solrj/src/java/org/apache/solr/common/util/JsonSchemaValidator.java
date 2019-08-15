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

package org.apache.solr.common.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A very basic and lightweight json schema parsing and data validation tool. This custom tool is created
 * because a) we need to support non json inputs b) to avoiding double parsing (this accepts an already parsed json as a map)
 * It validates most aspects of json schema but it is NOT A FULLY COMPLIANT JSON schema parser or validator.
 * This validator borrow some design's idea from https://github.com/networknt/json-schema-validator
 */
@SuppressWarnings("unchecked")
public class JsonSchemaValidator {

  private List<Validator> validators;
  private static Set<String> KNOWN_FNAMES = new HashSet<>(Arrays.asList(
      "description","documentation","default","additionalProperties"));


  public JsonSchemaValidator(String jsonString) {
    this((Map) Utils.fromJSONString(jsonString));
  }

  public JsonSchemaValidator(Map jsonSchema) {
    this.validators = new LinkedList<>();
    for (Object fname : jsonSchema.keySet()) {
      if (KNOWN_FNAMES.contains(fname.toString())) continue;

      Function<Pair<Map, Object>, Validator> initializeFunction = VALIDATORS.get(fname.toString());
      if (initializeFunction == null) throw new RuntimeException("Unknown key : " + fname);

      this.validators.add(initializeFunction.apply(new Pair<>(jsonSchema, jsonSchema.get(fname))));
    }
  }

  static final Map<String, Function<Pair<Map,Object>, Validator>> VALIDATORS = new HashMap<>();

  static {
    VALIDATORS.put("items", pair -> new ItemsValidator(pair.first(), (Map) pair.second()));
    VALIDATORS.put("enum", pair -> new EnumValidator(pair.first(), (List) pair.second()));
    VALIDATORS.put("properties", pair -> new PropertiesValidator(pair.first(), (Map) pair.second()));
    VALIDATORS.put("type", pair -> new TypeValidator(pair.first(), pair.second()));
    VALIDATORS.put("required", pair -> new RequiredValidator(pair.first(), (List)pair.second()));
    VALIDATORS.put("oneOf", pair -> new OneOfValidator(pair.first(), (List)pair.second()));
  }

  public List<String> validateJson(Object data) {
    List<String> errs = new LinkedList<>();
    validate(data, errs);
    return errs.isEmpty() ? null : errs;
  }

  boolean validate(Object data, List<String> errs) {
    if (data == null) return true;
    for (Validator validator : validators) {
      if (!validator.validate(data, errs)) {
        return false;
      }
    }
    return true;
  }

}

abstract class Validator<T> {
  @SuppressWarnings("unused")
  Validator(Map schema, T properties) {};
  abstract boolean validate(Object o, List<String> errs);
}

enum Type {
  STRING(String.class),
  ARRAY(List.class),
  NUMBER(Number.class),
  INTEGER(Long.class){
    @Override
    boolean isValid(Object o) {
      if(super.isValid(o)) return true;
      try {
        Long.parseLong(String.valueOf(o));
        return true;
      } catch (NumberFormatException e) {
        return false;

      }
    }
  },
  BOOLEAN(Boolean.class){
    @Override
    boolean isValid(Object o) {
      if(super.isValid(o)) return true;
      try {
        Boolean.parseBoolean (String.valueOf(o));
        return true;
      } catch (NumberFormatException e) {
        return false;
      }

    }
  },
  ENUM(List.class),
  OBJECT(Map.class),
  NULL(null),
  UNKNOWN(Object.class);

  Class type;

  Type(Class type) {
    this.type = type;
  }

  boolean isValid(Object o) {
    if (type == null) return o == null;
    return type.isInstance(o);
  }
}

class TypeValidator extends Validator<Object> {
  private Set<Type> types;

  TypeValidator(Map schema, Object type) {
    super(schema, type);
    types = new HashSet<>(1);
    if (type instanceof List) {
      for (Object t : (List)type) {
        types.add(getType(t.toString()));
      }
    } else {
      types.add(getType(type.toString()));
    }
  }

  private Type getType(String typeStr) {
    try {
      return Type.valueOf(typeStr.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown type " + typeStr);
    }
  }

  @Override
  boolean validate(Object o, List<String> errs) {
    for (Type type: types) {
      if (type.isValid(o)) return true;
    }
    errs.add("Value is not valid, expected one of: " + types + ", found: " + o.getClass().getSimpleName());
    return false;
  }
}

class ItemsValidator extends Validator<Map> {
  private JsonSchemaValidator validator;
  ItemsValidator(Map schema, Map properties) {
    super(schema, properties);
    validator = new JsonSchemaValidator(properties);
  }

  @Override
  boolean validate(Object o, List<String> errs) {
    if (o instanceof List) {
      for (Object o2 : (List) o) {
        if (!validator.validate(o2, errs)) {
          errs.add("Items not valid");
          return false;
        }
      }
      return true;
    }
    return false;
  }
}

class EnumValidator extends Validator<List<String>> {

  private Set<String> enumVals;

  EnumValidator(Map schema, List<String> properties) {
    super(schema, properties);
    enumVals = new HashSet<>(properties);

  }

  @Override
  boolean validate(Object o, List<String> errs) {
    if (o instanceof String) {
      if(!enumVals.contains(o)) {
        errs.add("Value of enum must be one of " + enumVals);
        return false;
      }
      return true;
    }
    return false;
  }
}

class RequiredValidator extends Validator<List<String>> {

  private Set<String> requiredProps;

  RequiredValidator(Map schema, List<String> requiredProps) {
    super(schema, requiredProps);
    this.requiredProps = new HashSet<>(requiredProps);
  }

  @Override
  boolean validate(Object o, List<String> errs) {
    return validate(o,errs,requiredProps);
  }

  boolean validate( Object o, List<String> errs, Set<String> requiredProps) {
    if (o instanceof Map) {
      Set fnames = ((Map) o).keySet();
      for (String requiredProp : requiredProps) {
        if (requiredProp.contains(".")) {
          if (requiredProp.endsWith(".")) {
            errs.add("Illegal required attribute name (ends with '.': " + requiredProp + ").  This is a bug.");
            return false;
          }
          String subprop = requiredProp.substring(requiredProp.indexOf(".") + 1);
          if (!validate(((Map)o).get(requiredProp), errs, Collections.singleton(subprop))) {
            return false;
          }
        } else {
          if (!fnames.contains(requiredProp)) {
            errs.add("Missing required attribute '" + requiredProp + "' in object " + Utils.toJSONString(o));
            return false;
          }
        }
      }
      return true;
    }
    return false;
  }
}

class PropertiesValidator extends Validator<Map<String, Map>> {
  private Map<String, JsonSchemaValidator> jsonSchemas;
  private boolean additionalProperties;

  PropertiesValidator(Map schema, Map<String, Map> properties) {
    super(schema, properties);
    jsonSchemas = new HashMap<>();
    this.additionalProperties = (boolean) schema.getOrDefault("additionalProperties", false);
    for (Map.Entry<String, Map> entry : properties.entrySet()) {
      jsonSchemas.put(entry.getKey(), new JsonSchemaValidator(entry.getValue()));
    }
  }

  @Override
  boolean validate(Object o, List<String> errs) {
    if (o instanceof Map) {
      Map map = (Map) o;
      for (Object key : map.keySet()) {
        JsonSchemaValidator jsonSchema = jsonSchemas.get(key.toString());
        if (jsonSchema == null && !additionalProperties) {
          errs.add("Unknown field '" + key + "' in object : " + Utils.toJSONString(o));
          return false;
        }
        if (jsonSchema != null && !jsonSchema.validate(map.get(key), errs)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}

class OneOfValidator extends Validator<List<String>> {

  private Set<String> oneOfProps;

  OneOfValidator(Map schema, List<String> oneOfProps) {
    super(schema, oneOfProps);
    this.oneOfProps = new HashSet<>(oneOfProps);
  }

  @Override
  boolean validate(Object o, List<String> errs) {
    if (o instanceof Map) {
      Map map = (Map) o;
      for (Object key : map.keySet()) {
        if (oneOfProps.contains(key.toString())) return true;
      }
      errs.add("One of fields :"  + oneOfProps + " is not presented in object : " + Utils.toJSONString(o));
      return false;
    }

    return false;
  }
}
