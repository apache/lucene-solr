package org.apache.lucene.collation;

/**
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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.FieldCacheRangeFilter;
import org.apache.lucene.util.BytesRef;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;

/**
 * Indexes collation keys as a single-valued {@link SortedDocValuesField}.
 * <p>
 * This is more efficient that {@link ICUCollationKeyAnalyzer} if the field 
 * only has one value: no uninversion is necessary to sort on the field, 
 * locale-sensitive range queries can still work via {@link FieldCacheRangeFilter}, 
 * and the underlying data structures built at index-time are likely more efficient 
 * and use less memory than FieldCache.
 */
public final class ICUCollationDocValuesField extends Field {
  private final String name;
  private final Collator collator;
  private final BytesRef bytes = new BytesRef();
  private final RawCollationKey key = new RawCollationKey();
  
  /**
   * Create a new ICUCollationDocValuesField.
   * <p>
   * NOTE: you should not create a new one for each document, instead
   * just make one and reuse it during your indexing process, setting
   * the value via {@link #setStringValue(String)}.
   * @param name field name
   * @param collator Collator for generating collation keys.
   */
  // TODO: can we make this trap-free? maybe just synchronize on the collator
  // instead? 
  public ICUCollationDocValuesField(String name, Collator collator) {
    super(name, SortedDocValuesField.TYPE);
    this.name = name;
    try {
      this.collator = (Collator) collator.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    fieldsData = bytes; // so wrong setters cannot be called
  }

  @Override
  public String name() {
    return name;
  }
  
  @Override
  public void setStringValue(String value) {
    collator.getRawCollationKey(value, key);
    bytes.bytes = key.bytes;
    bytes.offset = 0;
    bytes.length = key.size;
  }
}
