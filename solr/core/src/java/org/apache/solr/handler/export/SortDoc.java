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

package org.apache.solr.handler.export;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;

class SortDoc implements Comparable<SortDoc> {

  protected int docId = -1;
  protected int ord = -1;
  protected int docBase = -1;

  private SortValue[] sortValues;

  public SortDoc(SortValue[] sortValues) {
    this.sortValues = sortValues;
  }

  public SortDoc() {

  }

  @Override
  public boolean equals(Object obj) {
    // subclasses are not equal
    if (!obj.getClass().equals(getClass())) {
      return false;
    }
    return compareTo((SortDoc) obj) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(docId, ord, docBase);
  }

  public SortValue getSortValue(String field) {
    for (SortValue value : sortValues) {
      if (value.getField().equals(field)) {
        return value;
      }
    }
    return null;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.ord = context.ord;
    this.docBase = context.docBase;
    for (SortValue value : sortValues) {
      value.setNextReader(context);
    }
  }

  public void reset() {
    this.docId = -1;
    this.docBase = -1;
    this.ord = -1;
    for (SortValue value : sortValues) {
      value.reset();
    }
  }

  public void setValues(int docId) throws IOException {
    this.docId = docId;
    for (SortValue sortValue : sortValues) {
      sortValue.setCurrentValue(docId);
    }
  }

  public void setGlobalValues(SortDoc previous) {
    SortValue[] previousValues = previous.sortValues;
    for (int i = 0; i < sortValues.length; i++) {
      sortValues[i].toGlobalValue(previousValues[i]);
    }
  }

  public void setValues(SortDoc sortDoc) {
    this.docId = sortDoc.docId;
    this.ord = sortDoc.ord;
    this.docBase = sortDoc.docBase;
    SortValue[] vals = sortDoc.sortValues;
    for (int i = 0; i < vals.length; i++) {
      sortValues[i].setCurrentValue(vals[i]);
    }
  }

  public SortDoc copy() {
    SortValue[] svs = new SortValue[sortValues.length];
    for (int i = 0; i < sortValues.length; i++) {
      svs[i] = sortValues[i].copy();
    }
    return new SortDoc(svs);
  }

  public boolean lessThan(Object o) {
    if (docId == -1) {
      return true;
    }
    SortDoc sd = (SortDoc) o;
    SortValue[] sortValues1 = sd.sortValues;
    for (int i = 0; i < sortValues.length; i++) {
      int comp = sortValues[i].compareTo(sortValues1[i]);
      if (comp < 0) {
        return true;
      } else if (comp > 0) {
        return false;
      }
    }
    return docId + docBase > sd.docId + sd.docBase; //index order
  }

  @Override
  public int compareTo(SortDoc sd) {
    for (int i = 0; i < sortValues.length; i++) {
      int comp = sortValues[i].compareTo(sd.sortValues[i]);
      if (comp != 0) {
        return comp;
      }
    }
    return (sd.docId + sd.docBase) - (docId + docBase);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(ord).append(':').append(docBase).append(':').append(docId).append("; ");
    for (int i = 0; i < sortValues.length; i++) {
      builder.append("value").append(i).append(": ").append(sortValues[i]).append(", ");
    }
    return builder.toString();
  }
}