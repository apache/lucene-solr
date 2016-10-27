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

package org.apache.lucene.concordance.classic.impl;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.concordance.classic.DocIdBuilder;

/**
 * Simple class that grabs the stringValue() of a specified
 * field to use as a document's unique key for the ConcordanceWindow
 * building process.
 * <p>
 * Note that this takes only the first value of the field.
 * If a multi-valued field is selected, surprises might happen.
 * <p>
 * Also, note that if the field is not found, this returns
 * a string representation of the ephemeral Lucene docId.
 * <p>
 * Some users might want to throw an exception instead of this behavior.
 */
public class FieldBasedDocIdBuilder implements DocIdBuilder {

  private final String fieldName;

  /**
   * @param fieldName, name of field to be used as a document's unique key
   */
  public FieldBasedDocIdBuilder(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public String build(Document d, long docId) {
    IndexableField field = d.getField(fieldName);
    //should probably throw exception, no?!
    if (field == null) {
      return Long.toString(docId);
    }
    return field.stringValue();
  }

  /**
   * Instead of getField(String fieldName), this allows for extension
   *
   * @return fields to use
   */
  public Set<String> getFields() {
    Set<String> fields = new HashSet<String>();
    fields.add(fieldName);
    return fields;
  }
}
