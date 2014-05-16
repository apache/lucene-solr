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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Utility class to do efficient primary-key (only 1 doc contains the
 *  given term) lookups by segment, re-using the enums.  This class is
 *  not thread safe, so it is the caller's job to create and use one
 *  instance of this per thread.  Do not use this if a term may appear
 *  in more than one document!  It will only return the first one it
 *  finds. */
public class PerThreadPKLookup {

  protected final TermsEnum[] termsEnums;
  protected final DocsEnum[] docsEnums;
  protected final Bits[] liveDocs;
  protected final int[] docBases;
  protected final int numSegs;
  protected final boolean hasDeletions;

  public PerThreadPKLookup(IndexReader r, String idFieldName) throws IOException {

    List<AtomicReaderContext> leaves = new ArrayList<>(r.leaves());

    // Larger segments are more likely to have the id, so we sort largest to smallest by numDocs:
    Collections.sort(leaves, new Comparator<AtomicReaderContext>() {
        @Override
        public int compare(AtomicReaderContext c1, AtomicReaderContext c2) {
          return c2.reader().numDocs() - c1.reader().numDocs();
        }
      });

    termsEnums = new TermsEnum[leaves.size()];
    docsEnums = new DocsEnum[leaves.size()];
    liveDocs = new Bits[leaves.size()];
    docBases = new int[leaves.size()];
    int numSegs = 0;
    boolean hasDeletions = false;
    for(int i=0;i<leaves.size();i++) {
      Fields fields = leaves.get(i).reader().fields();
      if (fields != null) {
        Terms terms = fields.terms(idFieldName);
        if (terms != null) {
          termsEnums[numSegs] = terms.iterator(null);
          assert termsEnums[numSegs] != null;
          docBases[numSegs] = leaves.get(i).docBase;
          liveDocs[numSegs] = leaves.get(i).reader().getLiveDocs();
          hasDeletions |= leaves.get(i).reader().hasDeletions();
          numSegs++;
        }
      }
    }
    this.numSegs = numSegs;
    this.hasDeletions = hasDeletions;
  }
    
  /** Returns docID if found, else -1. */
  public int lookup(BytesRef id) throws IOException {
    for(int seg=0;seg<numSegs;seg++) {
      if (termsEnums[seg].seekExact(id)) {
        docsEnums[seg] = termsEnums[seg].docs(liveDocs[seg], docsEnums[seg], 0);
        int docID = docsEnums[seg].nextDoc();
        if (docID != DocsEnum.NO_MORE_DOCS) {
          return docBases[seg] + docID;
        }
        assert hasDeletions;
      }
    }

    return -1;
  }

  // TODO: add reopen method to carry over re-used enums...?
}
