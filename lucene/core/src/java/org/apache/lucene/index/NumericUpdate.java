package org.apache.lucene.index;

import static org.apache.lucene.util.RamUsageEstimator.*;

import org.apache.lucene.document.NumericDocValuesField;

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

/** An in-place update to a numeric docvalues field */
final class NumericUpdate {
  
  /* Rough logic: OBJ_HEADER + 3*PTR + INT
   * Term: OBJ_HEADER + 2*PTR
   *   Term.field: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   *   Term.bytes: 2*OBJ_HEADER + 2*INT + PTR + bytes.length
   * String: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   * Long: OBJ_HEADER + LONG
   */
  private static final int RAW_SIZE_IN_BYTES = 9*NUM_BYTES_OBJECT_HEADER + 8*NUM_BYTES_OBJECT_REF + 8*NUM_BYTES_INT + NUM_BYTES_LONG;
  
  static final Long MISSING = new Long(0);
  
  Term term;
  String field;
  Long value;
  int docIDUpto = -1;  // unassigned until applied, and confusing that it's here, when it's just used in BufferedDeletes...

  /**
   * Constructor.
   * 
   * @param term the {@link Term} which determines the documents that will be updated
   * @param field the {@link NumericDocValuesField} to update
   * @param value the updated value
   */
  NumericUpdate(Term term, String field, Long value) {
    this.term = term;
    this.field = field;
    this.value = value == null ? MISSING : value;
  }

  int sizeInBytes() {
    int sizeInBytes = RAW_SIZE_IN_BYTES;
    sizeInBytes += term.field.length() * NUM_BYTES_CHAR;
    sizeInBytes += term.bytes.bytes.length;
    sizeInBytes += field.length() * NUM_BYTES_CHAR;
    return sizeInBytes;
  }
  
  @Override
  public String toString() {
    return "term=" + term + ",field=" + field + ",value=" + value;
  }
}
