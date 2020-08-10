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

import org.apache.lucene.index.LeafReaderContext;

class TripleValueSortDoc extends DoubleValueSortDoc {

  protected SortValue value3;

  public SortValue getSortValue(String field) {
    if (value1.getField().equals(field)) {
      return value1;
    } else if (value2.getField().equals(field)) {
      return value2;
    } else if (value3.getField().equals(field)) {
      return value3;
    }
    return null;
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.ord = context.ord;
    this.docBase = context.docBase;
    value1.setNextReader(context);
    value2.setNextReader(context);
    value3.setNextReader(context);
  }

  public void reset() {
    this.docId = -1;
    this.docBase = -1;
    this.ord = -1;
    value1.reset();
    value2.reset();
    value3.reset();
  }

  public void setValues(int docId) throws IOException {
    this.docId = docId;
    value1.setCurrentValue(docId);
    value2.setCurrentValue(docId);
    value3.setCurrentValue(docId);
  }

  public void setValues(SortDoc sortDoc) {
    this.docId = sortDoc.docId;
    this.ord = sortDoc.ord;
    this.docBase = sortDoc.docBase;
    value1.setCurrentValue(((TripleValueSortDoc)sortDoc).value1);
    value2.setCurrentValue(((TripleValueSortDoc)sortDoc).value2);
    value3.setCurrentValue(((TripleValueSortDoc)sortDoc).value3);
  }

  public TripleValueSortDoc(SortValue value1, SortValue value2, SortValue value3) {
    super(value1, value2);
    this.value3 = value3;
  }

  public SortDoc copy() {
    return new TripleValueSortDoc(value1.copy(), value2.copy(), value3.copy());
  }

  public boolean lessThan(Object o) {

    TripleValueSortDoc sd = (TripleValueSortDoc)o;
    int comp = value1.compareTo(sd.value1);
    if(comp == -1) {
      return true;
    } else if (comp == 1) {
      return false;
    } else {
      comp = value2.compareTo(sd.value2);
      if(comp == -1) {
        return true;
      } else if (comp == 1) {
        return false;
      } else {
        comp = value3.compareTo(sd.value3);
        if(comp == -1) {
          return true;
        } else if (comp == 1) {
          return false;
        } else {
          return docId+docBase > sd.docId+sd.docBase;
        }
      }
    }
  }

  public int compareTo(Object o) {
    TripleValueSortDoc sd = (TripleValueSortDoc)o;
    int comp = value1.compareTo(sd.value1);
    if (comp == 0) {
      comp = value2.compareTo(sd.value2);
      if (comp == 0) {
        return value3.compareTo(sd.value3);
      } else {
        return comp;
      }
    } else {
      return comp;
    }
  }
}
