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

package org.apache.solr.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**A very basic and lightweight json schema parsing and data validation tool. This custom tool is created
 * because a) we need to support non json inputs b) to avoiding double parsing (this accepts an already parsed json as a map)
 * It validates most aspects of json schema but it is NOT A FULLY COMPLIANT JSON schema parser or validator.
 * What is supported ?
 * a) all types and their validation (string, boolean, array, enum,object, integer, number)
 * b) 'required' properties, 'additionalProperties'
 *
 *
 */

public class JsonSchemaValidator {
  private final SchemaNode root;

  public JsonSchemaValidator(String jsonString) {
    this((Map) Utils.fromJSONString(jsonString));
  }
  public JsonSchemaValidator(Map jsonSchema) {
    root = new SchemaNode(null);
    root.isRequired = true;
    List<String> errs = new LinkedList<>();
    root.validateSchema(jsonSchema, errs);
    if(!errs.isEmpty()){
      throw new RuntimeException("Invalid schema. "+ StrUtils.join(errs,'|'));
    }
  }

  private static class SchemaNode {
    final SchemaNode parent;
    Type type;
    Type elementType;
    boolean isRequired = false;
    Object validationInfo;
    Boolean additionalProperties;
    Map<String, SchemaNode> children;

    private SchemaNode(SchemaNode parent) {
      this.parent = parent;
    }

    private void validateSchema(Map jsonSchema, List<String> errs) {
      Object typeStr = jsonSchema.get("type");
      if (typeStr == null) {
        errs.add("'type' is missing ");
      }
      Type type = Type.get(typeStr);
      if (type == null) {
        errs.add ("Unknown type " + typeStr + " in object "+ Utils.toJSONString(jsonSchema));
        return;
      }
      this.type = type;

      for (SchemaAttribute schemaAttribute : SchemaAttribute.values()) {
        schemaAttribute.validateSchema(jsonSchema, this, errs);
      }
      jsonSchema.keySet().forEach(o -> {
        if (!knownAttributes.containsKey(o)) errs.add("Unknown key : " + o);
      });
      if (!errs.isEmpty()) return;

      if (type == Type.OBJECT) {
        Map m = (Map) jsonSchema.get("properties");
        if (m != null) {
          for (Object o : m.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            if (e.getValue() instanceof Map) {
              Map od = (Map) e.getValue();
              if (children == null) children = new LinkedHashMap<>();
              SchemaNode child = new SchemaNode(this);
              children.put((String) e.getKey(), child);
              child.validateSchema(od, errs);
            } else {
              errs.add("Invalid Object definition for field " + e.getKey());
            }
          }
        } else {
          additionalProperties = Boolean.TRUE;
        }
      }
      for (SchemaAttribute attr : SchemaAttribute.values()) {
        attr.postValidateSchema(jsonSchema, this, errs);
      }

    }

    private void validate(String key, Object data, List<String> errs) {
      if (data == null) {
        if (isRequired) {
          errs.add("Missing field '" + key+"'");
          return;
        }
      } else {
        type.validateData(key, data, this, errs);
        if(!errs.isEmpty()) return;
        if (children != null && type == Type.OBJECT) {
          for (Map.Entry<String, SchemaNode> e : children.entrySet()) {
            e.getValue().validate(e.getKey(), ((Map) data).get(e.getKey()), errs);
          }
          if (Boolean.TRUE != additionalProperties) {
            for (Object o : ((Map) data).keySet()) {
              if (!children.containsKey(o)) {
                errs.add("Unknown field '" + o + "' in object : " + Utils.toJSONString(data));
              }
            }
          }
        }
      }
    }

  }

  public List<String> validateJson(Object data) {
    List<String> errs = new LinkedList<>();
    root.validate(null, data, errs);
    return errs.isEmpty() ? null : errs;
  }

  /**represents an attribute in the schema definition
   *
   */
  enum SchemaAttribute {
    type(true, Type.STRING),
    properties(false, Type.OBJECT) {
      @Override
      public void validateSchema(Map attrSchema, SchemaNode schemaNode, List<String> errors) {
        super.validateSchema(attrSchema, schemaNode, errors);
        if (schemaNode.type != Type.OBJECT) return;
        Object val = attrSchema.get(key);
        if (val == null) {
          Object additional = attrSchema.get(additionalProperties.key);
          if (Boolean.TRUE.equals(additional)) schemaNode.additionalProperties =  Boolean.TRUE;
        }
      }
    },
    additionalProperties(false, Type.BOOLEAN),
    items(false, Type.OBJECT) {
      @Override
      public void validateSchema(Map attrSchema, SchemaNode schemaNode, List<String> errors) {
        super.validateSchema(attrSchema, schemaNode, errors);
        Object itemsVal = attrSchema.get(key);
        if (itemsVal != null) {
          if (schemaNode.type != Type.ARRAY) {
            errors.add("Only 'array' can have 'items'");
            return;
          } else {
            if (itemsVal instanceof Map) {
              Map val = (Map) itemsVal;
              Object value = val.get(type.key);
              Type t = Type.get(String.valueOf(value));
              if (t == null) {
                errors.add("Unknown array type " + Utils.toJSONString(attrSchema));
              } else {
                schemaNode.elementType = t;
              }
            }
          }
        }
      }
    },
    __default(false,Type.UNKNOWN),
    description(false, Type.STRING),
    documentation(false, Type.STRING),
    oneOf(false, Type.ARRAY),
    __enum(false, Type.ARRAY) {
      @Override
      void validateSchema(Map attrSchema, SchemaNode schemaNode, List<String> errors) {
        if (attrSchema.get(Type.ENUM._name) != null) {
          schemaNode.elementType = schemaNode.type;
          schemaNode.type = Type.ENUM;
        }
      }

      @Override
      void postValidateSchema(Map attrSchema, SchemaNode schemaNode, List<String> errs) {
        Object val = attrSchema.get(key);
        if (val == null) return;
        if (val instanceof List) {
          List list = (List) val;
          for (Object o : list) {
            if (!schemaNode.elementType.validate(o)) {
              errs.add("Invalid value : " + o + " Expected type : " + schemaNode.elementType._name);
            }
          }
          if (!errs.isEmpty()) return;
          schemaNode.validationInfo = new HashSet(list);
        } else {
          errs.add("'enum' should have a an array as value in Object " + Utils.toJSONString(attrSchema));
        }
      }
    },
    id(false, Type.STRING),
    _ref(false, Type.STRING),
    _schema(false, Type.STRING),
    required(false, Type.ARRAY) {
      @Override
      public void postValidateSchema(Map attrSchema, SchemaNode attr, List<String> errors) {
        Object val = attrSchema.get(key);
        if (val instanceof List) {
          List list = (List) val;
          if (attr.children != null) {
            for (Map.Entry<String, SchemaNode> e : attr.children.entrySet()) {
              if (list.contains(e.getKey())) e.getValue().isRequired = true;
            }
          }
        }
      }
    };

    final String key;
    final boolean _required;
    final Type typ;

    public String getKey() {
      return key;
    }

    void validateSchema(Map attrSchema, SchemaNode schemaNode, List<String> errors) {
      Object val = attrSchema.get(key);
      if (val == null) {
        if (_required)
          errors.add("Missing required attribute '" + key + "' in object " + Utils.toJSONString(attrSchema));
      } else {
        if (!typ.validate(val)) errors.add(key + " should be of type " + typ._name);
      }
    }

    void postValidateSchema(Map attrSchema, SchemaNode schemaNode, List<String> errs) {
    }

    SchemaAttribute(boolean required, Type type) {
      this.key = name().replaceAll("__","").replace('_', '$');
      this._required = required;
      this.typ = type;
    }
  }

  interface TypeValidator {
    void validateData(String key, Object o, SchemaNode schemaNode, List<String> errs);
  }

  /**represents a type in json
   *
   */
  enum Type {
    STRING(o -> o instanceof String),
    ARRAY(o -> o instanceof List, (key, o, schemaNode, errs) -> {
      List l = o instanceof List ? (List) o : Collections.singletonList(o);
      if (schemaNode.elementType != null) {
        for (Object elem : l) {
          if (!schemaNode.elementType.validate(elem)) {
            errs.add("Expected elements of type : " + key + " but found : " + Utils.toJSONString(o));
            break;
          }
        }
      }
    }),
    NUMBER(o -> o instanceof Number, (key, o, schemaNode, errs) -> {
      if (o instanceof String) {
        try {
          Double.parseDouble((String) o);
        } catch (NumberFormatException e) {
          errs.add(e.getClass().getName() + " " + e.getMessage());
        }

      }

    }),
    INTEGER(o -> o instanceof Integer, (key, o, schemaNode, errs) -> {
      if (o instanceof String) {
        try {
          Integer.parseInt((String) o);
        } catch (NumberFormatException e) {
          errs.add(e.getClass().getName() + " " + e.getMessage());
        }
      }
    }),
    BOOLEAN(o -> o instanceof Boolean, (key, o, schemaNode, errs) -> {
      if (o instanceof String) {
        try {
          Boolean.parseBoolean((String) o);
        } catch (Exception e) {
          errs.add(e.getClass().getName() + " " + e.getMessage());
        }
      }
    }),
    ENUM(o -> o instanceof List, (key, o, schemaNode, errs) -> {
      if (schemaNode.validationInfo instanceof HashSet) {
        HashSet enumVals = (HashSet) schemaNode.validationInfo;
        if (!enumVals.contains(o)) {
          errs.add("value of enum " + key + " must be one of" + enumVals);
        }
      }
    }),
    OBJECT(o -> o instanceof Map),
    UNKNOWN((o -> true));
    final String _name;

    final java.util.function.Predicate typeValidator;
    private final TypeValidator validator;

    Type(java.util.function.Predicate validator) {
      this(validator, null);

    }

    Type(java.util.function.Predicate validator, TypeValidator v) {
      _name = this.name().toLowerCase(Locale.ROOT);
      this.typeValidator = validator;
      this.validator = v;
    }

    boolean validate(Object o) {
      return typeValidator.test(o);
    }

    void validateData(String key, Object o, SchemaNode attr, List<String> errs) {
      if (validator != null) {
        validator.validateData(key, o, attr, errs);
        return;
      }
      if (!typeValidator.test(o))
        errs.add("Expected type : " + _name + " but found : " + o + "in object : " + Utils.toJSONString(o));
    }

    static Type get(Object type) {
      for (Type t : Type.values()) {
        if (t._name.equals(type)) return t;
      }
      return null;
    }
  }


  static final Map<String, SchemaAttribute> knownAttributes = unmodifiableMap(asList(SchemaAttribute.values()).stream().collect(toMap(SchemaAttribute::getKey, identity())));

}
