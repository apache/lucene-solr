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
package org.apache.solr.search.grouping.distributed.shardresultserializer;

import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;

/**
 * Utility functions used by implementations of the {@link ShardResultTransformer} interface.
 *
 * @lucene.experimental
 */
class ShardResultTransformerUtils {

  static Object marshalSortValue(Object originalSortValue, SchemaField schemaField) {
    return marshalOrUnmarshalSortValue(originalSortValue, schemaField, true);
  }

  static Object unmarshalSortValue(Object originalSortValue, SchemaField schemaField) {
    return marshalOrUnmarshalSortValue(originalSortValue, schemaField, false);
  }

  private static Object marshalOrUnmarshalSortValue(Object originalSortValue, SchemaField schemaField,
      boolean marshal) {
    if (originalSortValue != null && schemaField != null) {
      final FieldType fieldType = schemaField.getType();
      if (marshal) {
        return fieldType.marshalSortValue(originalSortValue);
      } else {
        return fieldType.unmarshalSortValue(originalSortValue);
      }
    } else {
      return originalSortValue;
    }
  }

}
