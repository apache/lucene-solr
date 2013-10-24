package org.apache.lucene.index;

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

import org.apache.lucene.util.BytesRef;

/** 
 * Exposes multi-valued view over a single-valued instance.
 * <p>
 * This can be used if you want to have one multi-valued implementation
 * against e.g. FieldCache.getDocTermOrds that also works for single-valued 
 * fields.
 */
public class SingletonSortedSetDocValues extends SortedSetDocValues {
  private final SortedDocValues in;
  private int docID;
  private boolean set;
  
  /** Creates a multi-valued view over the provided SortedDocValues */
  public SingletonSortedSetDocValues(SortedDocValues in) {
    this.in = in;
    assert NO_MORE_ORDS == -1; // this allows our nextOrd() to work for missing values without a check
  }

  @Override
  public long nextOrd() {
    if (set) {
      return NO_MORE_ORDS;
    } else {
      set = true;
      return in.getOrd(docID);
    }
  }

  @Override
  public void setDocument(int docID) {
    this.docID = docID;
    set = false;
  }

  @Override
  public void lookupOrd(long ord, BytesRef result) {
    // cast is ok: single-valued cannot exceed Integer.MAX_VALUE
    in.lookupOrd((int)ord, result);
  }

  @Override
  public long getValueCount() {
    return in.getValueCount();
  }

  @Override
  public long lookupTerm(BytesRef key) {
    return in.lookupTerm(key);
  }
}
