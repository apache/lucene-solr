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
package org.apache.lucene.document;


import org.apache.lucene.index.DocValuesType;

/**
 * <p>
 * Field that stores per-document <code>int</code> references to documents.
 * Here's an example usage:
 *
 * <pre class="prettyprint">
 *   document.add(new ReferenceDocValuesField(name, 5L));
 *   document.add(new ReferenceDocValuesField(name, 14L));
 * </pre>
 *
 * @lucene.experimental
 */

public class ReferenceDocValuesField extends Field {

  public static final String REFTYPE_ATTR = "reftype";
  public static final String DOCID_ATTR_VALUE = "docid";
  public static final String KNN_GRAPH_ATTR_VALUE = "knn-graph";

  /**
   * Type for reference DocValues. Possible extension point for other reference use cases.
   * Not fully implemented.
   * @lucene.experimental
   */
  static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    // indicate this field holds references to docids
    TYPE.putAttribute(REFTYPE_ATTR, DOCID_ATTR_VALUE);
    TYPE.freeze();
  }

  /**
   * Type for reference DocValues used in a KNN graph. References are made to be symmetric. This
   * field will be recalculated on merge using a search for nearest neighbors in the merged segment.
   * @lucene.experimental
   */
  public static final FieldType KNN_GRAPH_TYPE = new FieldType();
  static {
    KNN_GRAPH_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
    // indicate this field holds references to docids managed by a KNN graph
    KNN_GRAPH_TYPE.putAttribute(REFTYPE_ATTR, KNN_GRAPH_ATTR_VALUE);
    KNN_GRAPH_TYPE.freeze();
  }

  /**
   * Creates a new DocValues field with the specified 64-bit long value
   * @param name field name
   * @param value 64-bit long value
   * @throws IllegalArgumentException if the field name is null
   * @lucene.experimental
   */
  public ReferenceDocValuesField(String name, int value) {
    super(name, TYPE);
    fieldsData = Integer.valueOf(value);
  }

  /**
   * Creates a new DocValues field with the specified 64-bit long value
   * @param name field name
   * @param value 64-bit long value
   * @param type the type of reference. knn-graph references do not remove references to deleted docs
   * @throws IllegalArgumentException if the field name is null
   * @lucene.experimental
   */
  public ReferenceDocValuesField(String name, int value, FieldType type) {
    super(name, type);
    fieldsData = Integer.valueOf(value);
  }
}
