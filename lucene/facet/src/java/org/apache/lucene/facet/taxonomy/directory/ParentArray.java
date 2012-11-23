package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;

import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
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

    // note that copyParents.length may be equal to reader.maxDoc(). this is not a bug
    // it may be caused if e.g. the taxonomy segments were merged, and so an updated
    // NRT reader was obtained, even though nothing was changed. this is not very likely
    // to happen.
    int[] copyParents = copyFrom.getArray();
    this.parentOrdinals = new int[reader.maxDoc()];
    System.arraycopy(copyParents, 0, parentOrdinals, 0, copyParents.length);
    initFromReader(reader, copyParents.length);
  }

  // Read the parents of the new categories
  private void initFromReader(IndexReader reader, int first) throws IOException {
    if (reader.maxDoc() == first) {
      return;
    }
    
    // it's ok to use MultiFields because we only iterate on one posting list.
    // breaking it to loop over the leaves() only complicates code for no
    // apparent gain.
    DocsAndPositionsEnum positions = MultiFields.getTermPositionsEnum(reader, null,
        Consts.FIELD_PAYLOADS, Consts.PAYLOAD_PARENT_BYTES_REF,
        DocsAndPositionsEnum.FLAG_PAYLOADS);

    // shouldn't really happen, if it does, something's wrong
    if (positions == null || positions.advance(first) == DocIdSetIterator.NO_MORE_DOCS) {
      throw new CorruptIndexException("Missing parent data for category " + first);
    }
    
    int num = reader.maxDoc();
    for (int i = first; i < num; i++) {
      if (positions.docID() == i) {
        if (positions.freq() == 0) { // shouldn't happen
          throw new CorruptIndexException("Missing parent data for category " + i);
        }
        
        parentOrdinals[i] = positions.nextPosition();
        
        if (positions.nextDoc() == DocIdSetIterator.NO_MORE_DOCS) {
          if (i + 1 < num) {
            throw new CorruptIndexException("Missing parent data for category "+ (i + 1));
          }
          break;
        }
      } else { // this shouldn't happen
        throw new CorruptIndexException("Missing parent data for category " + i);
      }
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
