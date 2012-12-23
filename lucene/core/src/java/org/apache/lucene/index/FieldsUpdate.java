package org.apache.lucene.index;

import org.apache.lucene.analysis.Analyzer;

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

public class FieldsUpdate implements Comparable<FieldsUpdate> {
  
  /**
   * Specifies the operation to perform when updating fields.
   */
  enum Operation {
    /**
     * Add the given fields to all existing documents matching the update
     * criterion.
     */
    ADD_FIELDS,
    
    /**
     * Use the given fields to replace fields with same names in all existing
     * documents matching the update criterion.
     */
    REPLACE_FIELDS
  }
  
  final Operation operation;
  final IndexDocument fields;
  final Analyzer analyzer;
  final int docIDUpto;
  
  /**
   * An update of fields which is not assigned to a specific live segment.
   * 
   * @param operation
   *          The type of update operation.
   * @param fields
   *          The fields to use in the update.
   * @param analyzer
   *          The analyzer to use in the update.
   */
  public FieldsUpdate(Operation operation, IndexDocument fields,
      Analyzer analyzer) {
    this.operation = operation;
    this.fields = fields;
    this.analyzer = analyzer;
    this.docIDUpto = -1;
  }
  
  /**
   * An update of fields for a specific live segment.
   * 
   * @param other
   *          A non-specific update with the update data.
   * @param docIDUpto
   *          The doc ID in the live segment up to which the update should be
   *          applied.
   */
  public FieldsUpdate(FieldsUpdate other, int docIDUpto) {
    this.operation = other.operation;
    this.fields = other.fields;
    this.analyzer = other.analyzer;
    this.docIDUpto = docIDUpto;
  }

  /* Order FrieldsUpdate by increasing docIDUpto */
  @Override
  public int compareTo(FieldsUpdate other) {
    return this.docIDUpto - other.docIDUpto;
  }
  
}
