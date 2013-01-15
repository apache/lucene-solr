package org.apache.lucene.document;

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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * Field that stores
 * a per-document {@link BytesRef} value, indexed for
 * sorting.  Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new SortedBytesDocValuesField(name, new BytesRef("hello")));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * 
 * */

public class SortedBytesDocValuesField extends StoredField {

  /**
   * Type for sorted bytes DocValues: all with the same length
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValueType(FieldInfo.DocValuesType.SORTED);
    TYPE.freeze();
  }

  /**
   * Create a new sorted DocValues field.
   * @param name field name
   * @param bytes binary content
   * @throws IllegalArgumentException if the field name is null
   */
  public SortedBytesDocValuesField(String name, BytesRef bytes) {
    super(name, TYPE);
    fieldsData = bytes;
  }

  /**
   * Create a new fixed or variable length sorted DocValues field.
   * @param name field name
   * @param bytes binary content
   * @param isFixedLength true if all values have the same length.
   * @throws IllegalArgumentException if the field name is null
   */
  @Deprecated
  public SortedBytesDocValuesField(String name, BytesRef bytes, boolean isFixedLength) {
    super(name, TYPE);
    fieldsData = bytes;
  }
}
