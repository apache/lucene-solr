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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

class DoubleValue implements SortValue {

  protected NumericDocValues vals;
  protected String field;
  protected double currentValue;
  protected DoubleComp comp;
  private int lastDocID;
  private LeafReader reader;
  private boolean present;

  public DoubleValue(String field, DoubleComp comp) {
    this.field = field;
    this.comp = comp;
    this.currentValue = comp.resetValue();
    this.present = false;
  }

  public Object getCurrentValue() {
    assert present == true;
    return currentValue;
  }

  public String getField() {
    return field;
  }

  public DoubleValue copy() {
    return new DoubleValue(field, comp);
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.reader = context.reader();
    this.vals = DocValues.getNumeric(this.reader, this.field);
    lastDocID = 0;
  }

  public void setCurrentValue(int docId) throws IOException {
    if (docId < lastDocID) {
      throw new AssertionError("docs were sent out-of-order: lastDocID=" + lastDocID + " vs doc=" + docId);
    }
    lastDocID = docId;
    int curDocID = vals.docID();
    if (docId > curDocID) {
      curDocID = vals.advance(docId);
    }
    if (docId == curDocID) {
      present = true;
      currentValue = Double.longBitsToDouble(vals.longValue());
    } else {
      present = false;
      currentValue = 0f;
    }
  }

  @Override
  public boolean isPresent() {
    return present;
  }

  public void setCurrentValue(SortValue sv) {
    DoubleValue dv = (DoubleValue)sv;
    this.currentValue = dv.currentValue;
    this.present = dv.present;
  }

  public void reset() {
    this.currentValue = comp.resetValue();
    this.present = false;
  }

  public int compareTo(SortValue o) {
    DoubleValue dv = (DoubleValue)o;
    return comp.compare(currentValue, dv.currentValue);
  }
}
