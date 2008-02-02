package org.apache.lucene.util;

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

import java.util.BitSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;


/** Simple DocIdSet and DocIdSetIterator backed by a BitSet */
public class DocIdBitSet extends DocIdSet {
  private BitSet bitSet;
    
  public DocIdBitSet(BitSet bitSet) {
    this.bitSet = bitSet;
  }

  public DocIdSetIterator iterator() {
    return new DocIdBitSetIterator(bitSet);
  }
  
  /**
   * Returns the underlying BitSet. 
   */
  public BitSet getBitSet() {
	return this.bitSet;
  }
  
  private static class DocIdBitSetIterator extends DocIdSetIterator {
    private int docId;
    private BitSet bitSet;
    
    DocIdBitSetIterator(BitSet bitSet) {
      this.bitSet = bitSet;
      this.docId = -1;
    }
    
    public int doc() {
      assert docId != -1;
      return docId;
    }
    
    public boolean next() {
      // (docId + 1) on next line requires -1 initial value for docNr:
      return checkNextDocId(bitSet.nextSetBit(docId + 1));
    }
  
    public boolean skipTo(int skipDocNr) {
      return checkNextDocId( bitSet.nextSetBit(skipDocNr));
    }
  
    private boolean checkNextDocId(int d) {
      if (d == -1) { // -1 returned by BitSet.nextSetBit() when exhausted
        docId = Integer.MAX_VALUE;
        return false;
      } else {
        docId = d;
        return true;
      }
    }
  }
}
