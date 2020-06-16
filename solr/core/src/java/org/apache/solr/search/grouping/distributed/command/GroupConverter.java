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
package org.apache.solr.search.grouping.distributed.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueDate;
import org.apache.lucene.util.mutable.MutableValueDouble;
import org.apache.lucene.util.mutable.MutableValueFloat;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.solr.common.util.Utils;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;

/** 
 * this is a transition class: for numeric types we use function-based distributed grouping,
 * otherwise term-based. so for now we internally use function-based but pretend like we did 
 * it all with bytes, to not change any wire serialization etc.
 */
class GroupConverter {
  
  static Collection<SearchGroup<BytesRef>> fromMutable(SchemaField field, Collection<SearchGroup<MutableValue>> values) {
    if (values == null) {
      return null;
    }
    FieldType fieldType = field.getType();
    List<SearchGroup<BytesRef>> result = new ArrayList<>(values.size());
    for (SearchGroup<MutableValue> original : values) {
      SearchGroup<BytesRef> converted = new SearchGroup<>();
      converted.sortValues = original.sortValues;
      if (original.groupValue.exists) {
        BytesRefBuilder binary = new BytesRefBuilder();
        fieldType.readableToIndexed(Utils.OBJECT_TO_STRING.apply(original.groupValue.toObject()), binary);
        converted.groupValue = binary.get();
      } else {
        converted.groupValue = null;
      }
      result.add(converted);
    }
    return result;
  }
  
  static Collection<SearchGroup<MutableValue>> toMutable(SchemaField field, Collection<SearchGroup<BytesRef>> values) {
    FieldType fieldType = field.getType();
    List<SearchGroup<MutableValue>> result = new ArrayList<>(values.size());
    for (SearchGroup<BytesRef> original : values) {
      SearchGroup<MutableValue> converted = new SearchGroup<>();
      converted.sortValues = original.sortValues; // ?
      NumberType type = fieldType.getNumberType();
      final MutableValue v;
      switch (type) {
        case INTEGER:
          MutableValueInt mutableInt = new MutableValueInt();
          if (original.groupValue == null) {
            mutableInt.value = 0;
            mutableInt.exists = false;
          } else {
            mutableInt.value = (Integer) fieldType.toObject(field, original.groupValue);
          }
          v = mutableInt;
          break;
        case FLOAT:
          MutableValueFloat mutableFloat = new MutableValueFloat();
          if (original.groupValue == null) {
            mutableFloat.value = 0;
            mutableFloat.exists = false;
          } else {
            mutableFloat.value = (Float) fieldType.toObject(field, original.groupValue);
          }
          v = mutableFloat;
          break;
        case DOUBLE:
          MutableValueDouble mutableDouble = new MutableValueDouble();
          if (original.groupValue == null) {
            mutableDouble.value = 0;
            mutableDouble.exists = false;
          } else {
            mutableDouble.value = (Double) fieldType.toObject(field, original.groupValue);
          }
          v = mutableDouble;
          break;
        case LONG:
          MutableValueLong mutableLong = new MutableValueLong();
          if (original.groupValue == null) {
            mutableLong.value = 0;
            mutableLong.exists = false;
          } else {
            mutableLong.value = (Long) fieldType.toObject(field, original.groupValue);
          }
          v = mutableLong;
          break;
        case DATE:
          MutableValueDate mutableDate = new MutableValueDate();
          if (original.groupValue == null) {
            mutableDate.value = 0;
            mutableDate.exists = false;
          } else {
            mutableDate.value = ((Date)fieldType.toObject(field, original.groupValue)).getTime();
          }
          v = mutableDate;
          break;
        default:
          throw new AssertionError();
      }
      converted.groupValue = v;
      result.add(converted);
    }
    return result;
  }
  
  @SuppressWarnings({"unchecked", "rawtypes"})
  static TopGroups<BytesRef> fromMutable(SchemaField field, TopGroups<MutableValue> values) {
    if (values == null) {
      return null;
    }
    
    FieldType fieldType = field.getType();

    GroupDocs<BytesRef> groupDocs[] = new GroupDocs[values.groups.length];

    for (int i = 0; i < values.groups.length; i++) {
      GroupDocs<MutableValue> original = values.groups[i];
      final BytesRef groupValue;
      if (original.groupValue.exists) {
        BytesRefBuilder binary = new BytesRefBuilder();
        fieldType.readableToIndexed(Utils.OBJECT_TO_STRING.apply(original.groupValue.toObject()), binary);
        groupValue = binary.get();
      } else {
        groupValue = null;
      }
      groupDocs[i] = new GroupDocs<>(original.score, original.maxScore, original.totalHits, original.scoreDocs, groupValue, original.groupSortValues);
    }
    
    return new TopGroups<>(values.groupSort, values.withinGroupSort, values.totalHitCount, values.totalGroupedHitCount, groupDocs, values.maxScore);
  }
}
