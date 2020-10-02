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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.annotation.JsonProperty;

/**Creates a json schema from an annotated Java Object. This, by  no means, is an exhaustive impl
 * of schema generation. We will expand the scope as we use more types
 *
 */

public class JsonSchemaCreator {
  @SuppressWarnings({"rawtypes"})
  public static final Map<Class, String> natives = new HashMap<>();

  static {
    natives.put(String.class, "string");
    natives.put(Integer.class, "integer");
    natives.put(int.class, "integer");
    natives.put(Float.class, "number");
    natives.put(float.class, "number");
    natives.put(Double.class, "number");
    natives.put(double.class, "number");
    natives.put(Boolean.class, "boolean");
    natives.put(List.class, "array");
  }

  public static Map<String, Object> getSchema(java.lang.reflect.Type t) {
    return createSchemaFromType(t, new LinkedHashMap<>());
  }

  private static Map<String, Object> createSchemaFromType(java.lang.reflect.Type t, Map<String, Object> map) {
    if (natives.containsKey(t)) {
      map.put("type", natives.get(t));
    } else if (t instanceof ParameterizedType) {
      if (((ParameterizedType) t).getRawType() == List.class) {
        Type typ = ((ParameterizedType) t).getActualTypeArguments()[0];
        map.put("type", "array");
        map.put("items", getSchema(typ));
      } else if (((ParameterizedType) t).getRawType() == Map.class) {
        Type typ = ((ParameterizedType) t).getActualTypeArguments()[0];
        map.put("type", "object");
        map.put("additionalProperties", true);
      }
    } else {
      createObjectSchema((Class) t, map);
    }
    return map;
  }

  private static void createObjectSchema(@SuppressWarnings({"rawtypes"})Class klas, Map<String, Object> map) {
    map.put("type", "object");
    Map<String, Object> props = new HashMap<>();
    map.put("properties", props);
    Set<String>  required = new HashSet<>();
    for (Field fld : klas.getDeclaredFields()) {
      JsonProperty p = fld.getAnnotation(JsonProperty.class);
      if (p == null) continue;
      String name = p.value().isEmpty() ? fld.getName() : p.value();
      props.put(name, getSchema(fld.getGenericType()));
      if(p.required()) required.add(name);
    }
    if(!required.isEmpty()) map.put("required", new ArrayList<>(required));

  }
}
