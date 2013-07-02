package org.apache.lucene.index;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;

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

  final Term term;
  final Operation operation;
  final Set<String> replacedFields;
  final Analyzer analyzer;
  final int docIdUpto;
  final int updateNumber;

  IndexDocument fields;
  Directory directory;
  SegmentInfo segmentInfo;
  
  /**
   * An update of fields which is not assigned to a specific live segment.
   * 
   * @param term
   *          The term to apply this update on
   * @param operation
   *          The type of update operation.
   * @param fields
   *          The fields to use in the update operation.
   * @param analyzer
   *          The analyzer to use in the update.
   * @param docIdUpto
   *          The doc ID of the last document added before this update.
   * @param updateNumber
   *          The running number of this update for the current segment.
   */
  public FieldsUpdate(Term term, Operation operation, IndexDocument fields,
      Analyzer analyzer, int docIdUpto, int updateNumber) {
    this.term = term;
    this.fields = fields;
    this.operation = operation;
    if (operation == Operation.ADD_FIELDS) {
      replacedFields = null;
    } else {
      replacedFields = new HashSet<String>();
      for (IndexableField field : fields.indexableFields()) {
        replacedFields.add(field.name());
      }
      for (StorableField field : fields.storableFields()) {
        replacedFields.add(field.name());
      }
    }
    this.analyzer = analyzer;
    this.docIdUpto = docIdUpto;
    this.updateNumber = updateNumber;
  }
  
  /**
   * An update of fields for a specific live segment.
   * 
   * @param other
   *          A non-specific update with the update data.
   */
  public FieldsUpdate(FieldsUpdate other) {
    this.term = other.term;
    this.operation = other.operation;
    this.replacedFields = other.replacedFields;
    this.analyzer = other.analyzer;
    this.docIdUpto = other.docIdUpto;
    this.updateNumber = other.updateNumber;
    this.directory = other.directory;
    this.segmentInfo = other.segmentInfo;
  }
  
  @Override
  public int compareTo(FieldsUpdate other) {
    return this.updateNumber - other.updateNumber;
  }

  @Override
  public String toString() {
    return "FieldsUpdate [term=" + term + ", operation=" + operation
        + ", docIdUpto=" + docIdUpto + ", updateNumber=" + updateNumber + "]";
  }

}
