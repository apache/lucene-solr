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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.AtomicReader;      // javadocs

/**
 * <p>
 * Field that stores a per-document <code>long</code> value 
 * for scoring, sorting or value retrieval.  The values are 
 * encoded in the index an in RAM (when loaded via 
 * {@link AtomicReader#docValues})
 * using packed ints. Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new PackedLongDocValuesField(name, 22L));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * 
 * @see DocValues
 * */

public class PackedLongDocValuesField extends Field {

  /**
   * Type for packed long DocValues.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValueType(DocValues.Type.VAR_INTS);
    TYPE.freeze();
  }

  /** 
   * Creates a new DocValues field with the specified long value 
   * @param name field name
   * @param value 64-bit long value
   * @throws IllegalArgumentException if the field name is null
   */
  public PackedLongDocValuesField(String name, long value) {
    super(name, TYPE);
    fieldsData = Long.valueOf(value);
  }
}
