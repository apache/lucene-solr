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
package org.apache.lucene.codecs.idversion;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs.Pair;

/** BlockTree's implementation of {@link Terms}. */
// public for CheckIndex:
final class VersionFieldReader extends Terms implements Accountable {
  final long numTerms;
  final FieldInfo fieldInfo;
  final long sumTotalTermFreq;
  final long sumDocFreq;
  final int docCount;
  final long indexStartFP;
  final long rootBlockFP;
  final Pair<BytesRef,Long> rootCode;
  final BytesRef minTerm;
  final BytesRef maxTerm;
  final VersionBlockTreeTermsReader parent;

  final FST<Pair<BytesRef,Long>> index;
  //private boolean DEBUG;

  VersionFieldReader(VersionBlockTreeTermsReader parent, FieldInfo fieldInfo, long numTerms, Pair<BytesRef,Long> rootCode, long sumTotalTermFreq, long sumDocFreq, int docCount,
              long indexStartFP, IndexInput indexIn, BytesRef minTerm, BytesRef maxTerm) throws IOException {
    assert numTerms > 0;
    this.fieldInfo = fieldInfo;
    //DEBUG = BlockTreeTermsReader.DEBUG && fieldInfo.name.equals("id");
    this.parent = parent;
    this.numTerms = numTerms;
    this.sumTotalTermFreq = sumTotalTermFreq; 
    this.sumDocFreq = sumDocFreq; 
    this.docCount = docCount;
    this.indexStartFP = indexStartFP;
    this.rootCode = rootCode;
    this.minTerm = minTerm;
    this.maxTerm = maxTerm;
    // if (DEBUG) {
    //   System.out.println("BTTR: seg=" + segment + " field=" + fieldInfo.name + " rootBlockCode=" + rootCode + " divisor=" + indexDivisor);
    // }

    rootBlockFP = (new ByteArrayDataInput(rootCode.output1.bytes, rootCode.output1.offset, rootCode.output1.length)).readVLong() >>> VersionBlockTreeTermsWriter.OUTPUT_FLAGS_NUM_BITS;

    if (indexIn != null) {
      final IndexInput clone = indexIn.clone();
      //System.out.println("start=" + indexStartFP + " field=" + fieldInfo.name);
      clone.seek(indexStartFP);
      index = new FST<>(clone, clone, VersionBlockTreeTermsWriter.FST_OUTPUTS);
        
      /*
        if (false) {
        final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
        Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
        Util.toDot(index, w, false, false);
        System.out.println("FST INDEX: SAVED to " + dotFileName);
        w.close();
        }
      */
    } else {
      index = null;
    }
  }

  @Override
  public BytesRef getMin() throws IOException {
    if (minTerm == null) {
      // Older index that didn't store min/maxTerm
      return super.getMin();
    } else {
      return minTerm;
    }
  }

  @Override
  public BytesRef getMax() throws IOException {
    if (maxTerm == null) {
      // Older index that didn't store min/maxTerm
      return super.getMax();
    } else {
      return maxTerm;
    }
  }

  @Override
  public boolean hasFreqs() {
    return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
  }

  @Override
  public boolean hasOffsets() {
    return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  public boolean hasPositions() {
    return fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
  }
    
  @Override
  public boolean hasPayloads() {
    return fieldInfo.hasPayloads();
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new IDVersionSegmentTermsEnum(this);
  }

  @Override
  public long size() {
    return numTerms;
  }

  @Override
  public long getSumTotalTermFreq() {
    return sumTotalTermFreq;
  }

  @Override
  public long getSumDocFreq() {
    return sumDocFreq;
  }

  @Override
  public int getDocCount() {
    return docCount;
  }

  @Override
  public long ramBytesUsed() {
    return ((index!=null)? index.ramBytesUsed() : 0);
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    if (index == null) {
      return Collections.emptyList();
    } else {
      return Collections.singletonList(Accountables.namedAccountable("term index", index));
    }
  }

  @Override
  public String toString() {
    return "IDVersionTerms(terms=" + numTerms + ",postings=" + sumDocFreq + ",positions=" + sumTotalTermFreq + ",docs=" + docCount + ")";
  }
}
