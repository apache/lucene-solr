package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;

import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;

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

/**
 * @lucene.experimental
 */
class ParentArray {

  // TODO: maybe use PackedInts?
  private final int[] parentOrdinals;

  /** Used by {@link #add(int, int)} when the array needs to grow. */
  ParentArray(int[] parentOrdinals) {
    this.parentOrdinals = parentOrdinals;
  }

  public ParentArray(IndexReader reader) throws IOException {
    parentOrdinals = new int[reader.maxDoc()];
    if (parentOrdinals.length > 0) {
      initFromReader(reader, 0);
      // Starting Lucene 2.9, following the change LUCENE-1542, we can
      // no longer reliably read the parent "-1" (see comment in
      // LuceneTaxonomyWriter.SinglePositionTokenStream). We have no way
      // to fix this in indexing without breaking backward-compatibility
      // with existing indexes, so what we'll do instead is just
      // hard-code the parent of ordinal 0 to be -1, and assume (as is
      // indeed the case) that no other parent can be -1.
      parentOrdinals[0] = TaxonomyReader.INVALID_ORDINAL;
    }
  }
  
  public ParentArray(IndexReader reader, ParentArray copyFrom) throws IOException {
    assert copyFrom != null;
    int[] copyParents = copyFrom.getArray();
    assert copyParents.length < reader.maxDoc() : "do not init a new ParentArray if the index hasn't changed";
    
    this.parentOrdinals = new int[reader.maxDoc()];
    System.arraycopy(copyParents, 0, parentOrdinals, 0, copyParents.length);
    initFromReader(reader, copyParents.length);
  }

  // Read the parents of the new categories
  private void initFromReader(IndexReader reader, int first) throws IOException {
    if (reader.maxDoc() == first) {
      return;
    }
    
    TermsEnum termsEnum = null;
    DocsAndPositionsEnum positions = null;
    int idx = 0;
    for (AtomicReaderContext context : reader.leaves()) {
      if (context.docBase < first) {
        continue;
      }

      // in general we could call readerCtx.reader().termPositionsEnum(), but that
      // passes the liveDocs. Since we know there are no deletions, the code
      // below may save some CPU cycles.
      termsEnum = context.reader().fields().terms(Consts.FIELD_PAYLOADS).iterator(termsEnum);
      if (!termsEnum.seekExact(Consts.PAYLOAD_PARENT_BYTES_REF, true)) {
        throw new CorruptIndexException("Missing parent stream data for segment " + context.reader());
      }
      positions = termsEnum.docsAndPositions(null /* no deletes in taxonomy */, positions);
      if (positions == null) {
        throw new CorruptIndexException("Missing parent stream data for segment " + context.reader());
      }

      idx = context.docBase;
      int doc;
      while ((doc = positions.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        doc += context.docBase;
        if (doc == idx) {
          if (positions.freq() == 0) { // shouldn't happen
            throw new CorruptIndexException("Missing parent data for category " + idx);
          }
          
          parentOrdinals[idx++] = positions.nextPosition();
        } else { // this shouldn't happen
          throw new CorruptIndexException("Missing parent data for category " + idx);
        }
      }
      if (idx + 1 < context.reader().maxDoc()) {
        throw new CorruptIndexException("Missing parent data for category " + (idx + 1));
      }
    }
    
    if (idx != reader.maxDoc()) {
      throw new CorruptIndexException("Missing parent data for category " + idx);
    }
  }
  
  public int[] getArray() {
    return parentOrdinals;
  }
  
  /**
   * Adds the given ordinal/parent info and returns either a new instance if the
   * underlying array had to grow, or this instance otherwise.
   * <p>
   * <b>NOTE:</b> you should call this method from a thread-safe code.
   */
  ParentArray add(int ordinal, int parentOrdinal) {
    if (ordinal >= parentOrdinals.length) {
      int[] newarray = ArrayUtil.grow(parentOrdinals);
      newarray[ordinal] = parentOrdinal;
      return new ParentArray(newarray);
    }
    parentOrdinals[ordinal] = parentOrdinal;
    return this;
  }

}
