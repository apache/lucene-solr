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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;

public class LongValue implements SortValue {

  protected NumericDocValues vals;
  protected String field;
  protected long currentValue;
  protected LongComp comp;
  private int lastDocID;

  public LongValue(String field, LongComp comp) {
    this.field = field;
    this.comp = comp;
    this.currentValue = comp.resetValue();
  }

  public LongValue copy() {
    return new LongValue(field, comp);
  }

  public void setNextReader(LeafReaderContext context) throws IOException {
    this.vals = DocValues.getNumeric(context.reader(), field);
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
      currentValue = vals.longValue();
    } else {
      currentValue = 0;
    }
  }

  public void setCurrentValue(SortValue sv) {
    LongValue lv = (LongValue)sv;
    this.currentValue = lv.currentValue;
  }

  public int compareTo(SortValue o) {
    LongValue l = (LongValue)o;
    return comp.compare(currentValue, l.currentValue);
  }

  public void reset() {
    this.currentValue = comp.resetValue();
  }
}
