package org.apache.lucene.index;

/**
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

import org.apache.lucene.util.ReaderUtil;
import java.io.IOException;
import java.util.Arrays;

/**
 * Exposes flex API, merged from flex API of sub-segments.
 *
 * @lucene.experimental
 */

public final class MultiDocsEnum extends DocsEnum {
  private final MultiTermsEnum parent;
  final DocsEnum[] subDocsEnum;
  private EnumWithSlice[] subs;
  int numSubs;
  int upto;
  DocsEnum current;
  int currentBase;
  int doc = -1;

  public MultiDocsEnum(MultiTermsEnum parent, int subReaderCount) {
    this.parent = parent;
    subDocsEnum = new DocsEnum[subReaderCount];
  }

  MultiDocsEnum reset(final EnumWithSlice[] subs, final int numSubs) throws IOException {
    this.numSubs = numSubs;

    this.subs = new EnumWithSlice[subs.length];
    for(int i=0;i<subs.length;i++) {
      this.subs[i] = new EnumWithSlice();
      this.subs[i].docsEnum = subs[i].docsEnum;
      this.subs[i].slice = subs[i].slice;
    }
    upto = -1;
    current = null;
    return this;
  }

  public boolean canReuse(MultiTermsEnum parent) {
    return this.parent == parent;
  }

  public int getNumSubs() {
    return numSubs;
  }

  public EnumWithSlice[] getSubs() {
    return subs;
  }

  @Override
  public int freq() {
    return current.freq();
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    while(true) {
      if (current != null) {
        final int doc = current.advance(target-currentBase);
        if (doc == NO_MORE_DOCS) {
          current = null;
        } else {
          return this.doc = doc + currentBase;
        }
      } else if (upto == numSubs-1) {
        return this.doc = NO_MORE_DOCS;
      } else {
        upto++;
        current = subs[upto].docsEnum;
        currentBase = subs[upto].slice.start;
      }
    }
  }

  @Override
  public int nextDoc() throws IOException {
    while(true) {
      if (current == null) {
        if (upto == numSubs-1) {
          return this.doc = NO_MORE_DOCS;
        } else {
          upto++;
          current = subs[upto].docsEnum;
          currentBase = subs[upto].slice.start;
        }
      }

      final int doc = current.nextDoc();
      if (doc != NO_MORE_DOCS) {
        return this.doc = currentBase + doc;
      } else {
        current = null;
      }
    }
  }

  // TODO: implement bulk read more efficiently than super
  public final static class EnumWithSlice {
    public DocsEnum docsEnum;
    public ReaderUtil.Slice slice;
    
    @Override
    public String toString() {
      return slice.toString()+":"+docsEnum;
    }
  }

  @Override
  public String toString() {
    return "MultiDocsEnum(" + Arrays.toString(getSubs()) + ")";
  }
}

