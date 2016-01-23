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

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;

/**
 * <p>
 * Field that stores
 * a set of per-document {@link BytesRef} values, indexed for
 * faceting,grouping,joining.  Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new SortedSetDocValuesField(name, new BytesRef("hello")));
 *   document.add(new SortedSetDocValuesField(name, new BytesRef("world")));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * 
 * */

public class SortedSetDocValuesField extends Field {

  /**
   * Type for sorted bytes DocValues
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_SET);
    TYPE.freeze();
  }

  /**
   * Create a new sorted DocValues field.
   * @param name field name
   * @param bytes binary content
   * @throws IllegalArgumentException if the field name is null
   */
  public SortedSetDocValuesField(String name, BytesRef bytes) {
    super(name, TYPE);
    fieldsData = bytes;
  }
}
