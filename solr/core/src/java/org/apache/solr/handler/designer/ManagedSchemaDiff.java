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

package org.apache.solr.handler.designer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaField;

/**
 * Utility methods for comparing managed index schemas
 */
public class ManagedSchemaDiff {

  private static final String UPDATED_KEY_STRING = "updated";
  private static final String ADDED_KEY_STRING = "added";
  private static final String REMOVED_KEY_STRING = "removed";

  private static final String FIELDS_KEY_STRING = "fields";
  private static final String FIELD_TYPES_KEY_STRING = "fieldTypes";
  private static final String DYNAMIC_FIELDS_KEY_STRING = "dynamicFields";
  private static final String COPY_FIELDS_KEY_STRING = "copyFields";

  /**
   * Compute difference between two managed schemas. The returned map consists of changed, new, removed
   * elements in fields, field types, dynamic fields and copy fields between input schemas.
   *
   * <pre> Output format when rendered to json will look like below:
   *   {@code
   *   {
   *     "fields": {
   *       "updated": {...},
   *       "added": {...},
   *       "removed": {...}
   *     },
   *     "fieldTypes": {
   *       "updated": {...},
   *       "added": {...},
   *       "removed": {...}
   *     },
   *     "dynamicFields": {
   *       "updated": {...},
   *       "added": {...},
   *       "removed": {...}
   *     },
   *     "copyFields: {
   *       "new": [...],
   *       "old": [...]
   *     }
   *   }
   *   }
   * </pre>
   *
   * @param oldSchema instance of {@link ManagedIndexSchema}
   * @param newSchema instance of {@link ManagedIndexSchema}
   * @return the difference between two schemas
   */
  public static Map<String, Object> diff(ManagedIndexSchema oldSchema, ManagedIndexSchema newSchema) {
    Map<String, Object> diff = new HashMap<>();

    Map<String, Object> fieldsDiff = diff(mapFieldsToPropertyValues(oldSchema.getFields()), mapFieldsToPropertyValues(newSchema.getFields()));
    Map<String, Object> fieldTypesDiff = diff(mapFieldTypesToPropValues(oldSchema.getFieldTypes()), mapFieldTypesToPropValues(newSchema.getFieldTypes()));
    Map<String, Object> dynamicFieldDiff = diff(mapDynamicFieldToPropValues(oldSchema.getDynamicFields()), mapDynamicFieldToPropValues(newSchema.getDynamicFields()));
    Map<String, Object> copyFieldDiff = diff(getCopyFieldList(oldSchema), getCopyFieldList(newSchema));

    if (!fieldsDiff.isEmpty()) {
      diff.put(FIELDS_KEY_STRING, fieldsDiff);
    }
    if (!fieldTypesDiff.isEmpty()) {
      diff.put(FIELD_TYPES_KEY_STRING, fieldTypesDiff);
    }
    if (!dynamicFieldDiff.isEmpty()) {
      diff.put(DYNAMIC_FIELDS_KEY_STRING, dynamicFieldDiff);
    }
    if (!copyFieldDiff.isEmpty()) {
      diff.put(COPY_FIELDS_KEY_STRING, copyFieldDiff);
    }

    return diff;
  }

  /**
   * Compute difference between two map objects with {@link SimpleOrderedMap} as values.
   *
   * <pre> Example of the output format when rendered to json
   *   {@code
   *    {
   *      "updated": {
   *        "stringField": [
   *        {
   *          "docValues": "false"
   *        },
   *        {
   *          "docValues": "true"
   *        }
   *      },
   *      "added": {
   *        "newstringfield: {
   *          "name": "newstringfield",
   *          "type": "string",
   *          .....
   *        }
   *      },
   *      "removed": {
   *        "oldstringfield": {
   *          "name": "oldstringfield",
   *          "type": "string",
   *          .....
   *        }
   *      }
   *    }
   *   }
   * </pre>
   *
   * @param map1 instance of Map with {@link SimpleOrderedMap} elements
   * @param map2 instance of Map with {@link SimpleOrderedMap} elements
   * @return the difference between two Map
   */
  protected static Map<String, Object> diff(
      Map<String, SimpleOrderedMap<Object>> map1,
      Map<String, SimpleOrderedMap<Object>> map2) {
    Map<String, List<Map<String, Object>>> changedValues = new HashMap<>();
    Map<String, SimpleOrderedMap<Object>> newValues = new HashMap<>();
    Map<String, SimpleOrderedMap<Object>> removedValues = new HashMap<>();
    for (String fieldName : map1.keySet()) {
      if (map2.containsKey(fieldName)) {
        SimpleOrderedMap<Object> oldPropValues = map1.get(fieldName);
        SimpleOrderedMap<Object> newPropValues = map2.get(fieldName);
        if (!oldPropValues.equals(newPropValues)) {
          List<Map<String, Object>> mapDiff = getMapDifference(oldPropValues, newPropValues);
          if (!mapDiff.isEmpty()) {
            changedValues.put(fieldName, mapDiff);
          }
        }
      } else {
        removedValues.put(fieldName, map1.get(fieldName));
      }
    }

    for (String fieldName : map2.keySet()) {
      if (!map1.containsKey(fieldName)) {
        newValues.put(fieldName, map2.get(fieldName));
      }
    }

    Map<String, Object> mapDiff = new HashMap<>();
    if (!changedValues.isEmpty()) {
      mapDiff.put(UPDATED_KEY_STRING, changedValues);
    }
    if (!newValues.isEmpty()) {
      mapDiff.put(ADDED_KEY_STRING, newValues);
    }
    if (!removedValues.isEmpty()) {
      mapDiff.put(REMOVED_KEY_STRING, removedValues);
    }

    return mapDiff;
  }

  /**
   * Compute difference between two {@link SimpleOrderedMap} instances
   *
   * <pre> Output format example when rendered to json
   *   {@code
   *    [
   *         {
   *           "stored": false,
   *           "type": "string",
   *           "multiValued": false
   *         },
   *         {
   *           "stored": true,
   *           "type": "strings",
   *           "multiValued": true
   *         }
   *       ]
   *   }
   * </pre>
   *
   * @param simpleOrderedMap1 Map to treat as "left" map
   * @param simpleOrderedMap2 Map to treat as "right" map
   * @return List containing the left diff and right diff
   */
  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> getMapDifference(
      SimpleOrderedMap<Object> simpleOrderedMap1,
      SimpleOrderedMap<Object> simpleOrderedMap2) {
    Map<String, Object> map1 = simpleOrderedMap1.toMap(new HashMap<>());
    Map<String, Object> map2 = simpleOrderedMap2.toMap(new HashMap<>());
    Map<String, MapDifference.ValueDifference<Object>> mapDiff = Maps.difference(map1, map2).entriesDiffering();
    if (mapDiff.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, Object> leftMapDiff = leftOrRightMapDiff(mapDiff, true);
    Map<String, Object> rightMapDiff = leftOrRightMapDiff(mapDiff, false);
    return Arrays.asList(leftMapDiff, rightMapDiff);
  }

  private static Map<String, Object> leftOrRightMapDiff(Map<String, MapDifference.ValueDifference<Object>> mapDiff, boolean left) {
    Map<String, Object> leftMap = new HashMap<>(mapDiff.size() * 2);
    for (Map.Entry<String, MapDifference.ValueDifference<Object>> e : mapDiff.entrySet()) {
      Object value = left ? e.getValue().leftValue() : e.getValue().rightValue();
      leftMap.put(e.getKey(), value);
    }
    return leftMap;
  }

  protected static Map<String, Object> diff(List<SimpleOrderedMap<Object>> list1, List<SimpleOrderedMap<Object>> list2) {
    List<SimpleOrderedMap<Object>> oldList = new ArrayList<>(); // ordered map changed in list1 compared to list2
    List<SimpleOrderedMap<Object>> newList = new ArrayList<>(); // ordered map changed in list2 compared to list1

    list1.forEach(som -> {
      if (!list2.contains(som)) {
        oldList.add(som);
      }
    });

    list2.forEach(som -> {
      if (!list1.contains(som)) {
        newList.add(som);
      }
    });

    Map<String, Object> mapDiff = new HashMap<>();
    if (!oldList.isEmpty()) {
      mapDiff.put("old", oldList);
    }
    if (!newList.isEmpty()) {
      mapDiff.put("new", newList);
    }
    return mapDiff;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapFieldsToPropertyValues(Map<String, SchemaField> fields) {
    Map<String, SimpleOrderedMap<Object>> propValueMap = new HashMap<>();
    fields.forEach((k, v) -> propValueMap.put(k, v.getNamedPropertyValues(true)));
    return propValueMap;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapFieldTypesToPropValues(Map<String, FieldType> fieldTypes) {
    Map<String, SimpleOrderedMap<Object>> propValueMap = new HashMap<>();
    fieldTypes.forEach((k, v) -> propValueMap.put(k, v.getNamedPropertyValues(true)));
    return propValueMap;
  }

  protected static Map<String, SimpleOrderedMap<Object>> mapDynamicFieldToPropValues(IndexSchema.DynamicField[] dynamicFields) {
    Map<String, SimpleOrderedMap<Object>> map = new HashMap<>(dynamicFields.length * 2);
    for (IndexSchema.DynamicField df : dynamicFields) {
      map.put(df.getPrototype().getName(), df.getPrototype().getNamedPropertyValues(true));
    }
    return map;
  }

  protected static List<SimpleOrderedMap<Object>> getCopyFieldList(ManagedIndexSchema indexSchema) {
    return indexSchema.getCopyFieldProperties(false, null, null);
  }
}
