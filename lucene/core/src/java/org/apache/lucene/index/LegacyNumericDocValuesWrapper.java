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

package org.apache.lucene.index;

import org.apache.lucene.util.Bits;

/**
 * Wraps a {@link LegacyNumericDocValues} into a {@link NumericDocValues}.
 *
 * @deprecated Implement {@link NumericDocValues} directly.
 */
@Deprecated
public final class LegacyNumericDocValuesWrapper extends NumericDocValues {
  private final Bits docsWithField;
  private final LegacyNumericDocValues values;
  private final int maxDoc;
  private int docID = -1;
  
  public LegacyNumericDocValuesWrapper(Bits docsWithField, LegacyNumericDocValues values) {
    this.docsWithField = docsWithField;
    this.values = values;
    this.maxDoc = docsWithField.length();
  }

  /** Constructor used only for norms */
  public LegacyNumericDocValuesWrapper(int maxDoc, LegacyNumericDocValues values) {
    this(new Bits.MatchAllBits(maxDoc), values);
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public int nextDoc() {
    docID++;
    while (docID < maxDoc) {
      if (docsWithField.get(docID)) {
        return docID;
      }
      docID++;
    }
    docID = NO_MORE_DOCS;
    return NO_MORE_DOCS;
  }

  @Override
  public int advance(int target) {
    if (target < docID) {
      throw new IllegalArgumentException("cannot advance backwards: docID=" + docID + " target=" + target);
    }
    if (target == NO_MORE_DOCS) {
      this.docID = NO_MORE_DOCS;
    } else {
      this.docID = target-1;
      nextDoc();
    }
    return docID;
  }

  @Override
  public long cost() {
    // TODO
    return 0;
  }

  @Override
  public long longValue() {
    return values.get(docID);
  }

  @Override
  public String toString() {
    return "LegacyNumericDocValuesWrapper(" + values + ")";
  }
}
