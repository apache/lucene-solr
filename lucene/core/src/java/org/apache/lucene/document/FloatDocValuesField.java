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

/**
 * <p>
 * Field that stores a per-document <code>float</code> value for scoring, 
 * sorting or value retrieval. Here's an example usage:
 * 
 * <pre class="prettyprint">
 *   document.add(new FloatDocValuesField(name, 22f));
 * </pre>
 * 
 * <p>
 * If you also need to store the value, you should add a
 * separate {@link StoredField} instance.
 * @see DocValues
 * */

public class FloatDocValuesField extends Field {

  /**
   * Type for 32-bit float DocValues.
   */
  public static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValueType(DocValues.Type.FLOAT_32);
    TYPE.freeze();
  }

  /** 
   * Creates a new DocValues field with the specified 32-bit float value 
   * @param name field name
   * @param value 32-bit float value
   * @throws IllegalArgumentException if the field name is null
   */
  public FloatDocValuesField(String name, float value) {
    super(name, TYPE);
    fieldsData = Float.valueOf(value);
  }
}
