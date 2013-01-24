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
import org.apache.lucene.util.BytesRef;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;

/**
 * nocommit
 */
public final class ICUCollationDocValuesField extends Field {
  private final String name;
  private final Collator collator;
  private final BytesRef bytes = new BytesRef();
  private final RawCollationKey key = new RawCollationKey();
  
  public ICUCollationDocValuesField(String name, Collator collator) {
    super(name, SortedDocValuesField.TYPE);
    this.name = name;
    try {
      this.collator = (Collator) collator.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String name() {
    return name;
  }
  
  public void setStringValue(String value) {
    collator.getRawCollationKey(value, key);
    bytes.bytes = key.bytes;
    bytes.offset = 0;
    bytes.length = key.size;
  }

  @Override
  public BytesRef binaryValue() {
    return bytes;
  }
  
  // nocommit: make this thing trap-free
}
