package org.apache.lucene.search.cache;

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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.OrdTermState;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache.DocTermsIndex;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;

public class DocTermsIndexCreator extends EntryCreatorWithOptions<DocTermsIndex>
{
  public static final int FASTER_BUT_MORE_RAM = 2;

  public String field;

  public DocTermsIndexCreator( String field )
  {
    super( FASTER_BUT_MORE_RAM ); // By default turn on FASTER_BUT_MORE_RAM
    if( field == null ) {
      throw new IllegalArgumentException( "field can not be null" );
    }
    this.field = field;
  }

  public DocTermsIndexCreator( String field, int flags )
  {
    super( flags );
    if( field == null ) {
      throw new IllegalArgumentException( "field can not be null" );
    }
    this.field = field;
  }

  @Override
  public EntryKey getCacheKey() {
    return new SimpleEntryKey( DocTermsIndexCreator.class, field );
  }

  @Override
  public DocTermsIndex create(IndexReader reader) throws IOException
  {
    String field = StringHelper.intern(this.field); // TODO?? necessary?
    Terms terms = MultiFields.getTerms(reader, field);

    final boolean fasterButMoreRAM = hasOption(FASTER_BUT_MORE_RAM);

    final PagedBytes bytes = new PagedBytes(15);

    int startBytesBPV;
    int startTermsBPV;
    int startNumUniqueTerms;

    int maxDoc = reader.maxDoc();
    final int termCountHardLimit;
    if (maxDoc == Integer.MAX_VALUE) {
      termCountHardLimit = Integer.MAX_VALUE;
    } else {
      termCountHardLimit = maxDoc+1;
    }

    if (terms != null) {
      // Try for coarse estimate for number of bits; this
      // should be an underestimate most of the time, which
      // is fine -- GrowableWriter will reallocate as needed
      long numUniqueTerms = 0;
      try {
        numUniqueTerms = terms.getUniqueTermCount();
      } catch (UnsupportedOperationException uoe) {
        numUniqueTerms = -1;
      }
      if (numUniqueTerms != -1) {

        if (numUniqueTerms > termCountHardLimit) {
          // app is misusing the API (there is more than
          // one term per doc); in this case we make best
          // effort to load what we can (see LUCENE-2142)
          numUniqueTerms = termCountHardLimit;
        }

        startBytesBPV = PackedInts.bitsRequired(numUniqueTerms*4);
        startTermsBPV = PackedInts.bitsRequired(numUniqueTerms);

        startNumUniqueTerms = (int) numUniqueTerms;
      } else {
        startBytesBPV = 1;
        startTermsBPV = 1;
        startNumUniqueTerms = 1;
      }
    } else {
      startBytesBPV = 1;
      startTermsBPV = 1;
      startNumUniqueTerms = 1;
    }

    GrowableWriter termOrdToBytesOffset = new GrowableWriter(startBytesBPV, 1+startNumUniqueTerms, fasterButMoreRAM);
    final GrowableWriter docToTermOrd = new GrowableWriter(startTermsBPV, reader.maxDoc(), fasterButMoreRAM);

    // 0 is reserved for "unset"
    bytes.copyUsingLengthPrefix(new BytesRef());
    int termOrd = 1;

    if (terms != null) {
      final TermsEnum termsEnum = terms.iterator();
      DocsEnum docs = null;

      while(true) {
        final BytesRef term = termsEnum.next();
        if (term == null) {
          break;
        }
        if (termOrd >= termCountHardLimit) {
          break;
        }

        if (termOrd == termOrdToBytesOffset.size()) {
          // NOTE: this code only runs if the incoming
          // reader impl doesn't implement
          // getUniqueTermCount (which should be uncommon)
          termOrdToBytesOffset = termOrdToBytesOffset.resize(ArrayUtil.oversize(1+termOrd, 1));
        }
        termOrdToBytesOffset.set(termOrd, bytes.copyUsingLengthPrefix(term));
        docs = termsEnum.docs(null, docs);
        while (true) {
          final int docID = docs.nextDoc();
          if (docID == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          docToTermOrd.set(docID, termOrd);
        }
        termOrd++;
      }

      if (termOrdToBytesOffset.size() > termOrd) {
        termOrdToBytesOffset = termOrdToBytesOffset.resize(termOrd);
      }
    }

    // maybe an int-only impl?
    return new DocTermsIndexImpl(bytes.freeze(true), termOrdToBytesOffset.getMutable(), docToTermOrd.getMutable(), termOrd);
  }

  @Override
  public DocTermsIndex validate(DocTermsIndex entry, IndexReader reader) throws IOException {
    // TODO? nothing? perhaps subsequent call with FASTER_BUT_MORE_RAM?
    return entry;
  }

  //-----------------------------------------------------------------------------
  //-----------------------------------------------------------------------------

  public static class DocTermsIndexImpl extends DocTermsIndex {
    private final PagedBytes.Reader bytes;
    private final PackedInts.Reader termOrdToBytesOffset;
    private final PackedInts.Reader docToTermOrd;
    private final int numOrd;

    public DocTermsIndexImpl(PagedBytes.Reader bytes, PackedInts.Reader termOrdToBytesOffset, PackedInts.Reader docToTermOrd, int numOrd) {
      this.bytes = bytes;
      this.docToTermOrd = docToTermOrd;
      this.termOrdToBytesOffset = termOrdToBytesOffset;
      this.numOrd = numOrd;
    }

    @Override
    public PackedInts.Reader getDocToOrd() {
      return docToTermOrd;
    }

    @Override
    public int numOrd() {
      return numOrd;
    }

    @Override
    public int getOrd(int docID) {
      return (int) docToTermOrd.get(docID);
    }

    @Override
    public int size() {
      return docToTermOrd.size();
    }

    @Override
    public BytesRef lookup(int ord, BytesRef ret) {
      return bytes.fill(ret, termOrdToBytesOffset.get(ord));
    }

    @Override
    public TermsEnum getTermsEnum() {
      return this.new DocTermsIndexEnum();
    }

    class DocTermsIndexEnum extends TermsEnum {
      int currentOrd;
      int currentBlockNumber;
      int end;  // end position in the current block
      final byte[][] blocks;
      final int[] blockEnds;

      final BytesRef term = new BytesRef();

      public DocTermsIndexEnum() {
        currentOrd = 0;
        currentBlockNumber = 0;
        blocks = bytes.getBlocks();
        blockEnds = bytes.getBlockEnds();
        currentBlockNumber = bytes.fillAndGetIndex(term, termOrdToBytesOffset.get(0));
        end = blockEnds[currentBlockNumber];
      }

      @Override
      public SeekStatus seek(BytesRef text, boolean useCache) throws IOException {
        int low = 1;
        int high = numOrd-1;
        
        while (low <= high) {
          int mid = (low + high) >>> 1;
          seek(mid);
          int cmp = term.compareTo(text);

          if (cmp < 0)
            low = mid + 1;
          else if (cmp > 0)
            high = mid - 1;
          else
            return SeekStatus.FOUND; // key found
        }
        
        if (low == numOrd) {
          return SeekStatus.END;
        } else {
          seek(low);
          return SeekStatus.NOT_FOUND;
        }
      }

      @Override
      public SeekStatus seek(long ord) throws IOException {
        assert(ord >= 0 && ord <= numOrd);
        // TODO: if gap is small, could iterate from current position?  Or let user decide that?
        currentBlockNumber = bytes.fillAndGetIndex(term, termOrdToBytesOffset.get((int)ord));
        end = blockEnds[currentBlockNumber];
        currentOrd = (int)ord;
        return SeekStatus.FOUND;
      }

      @Override
      public BytesRef next() throws IOException {
        int start = term.offset + term.length;
        if (start >= end) {
          // switch byte blocks
          if (currentBlockNumber +1 >= blocks.length) {
            return null;
          }
          currentBlockNumber++;
          term.bytes = blocks[currentBlockNumber];
          end = blockEnds[currentBlockNumber];
          start = 0;
          if (end<=0) return null;  // special case of empty last array
        }

        currentOrd++;

        byte[] block = term.bytes;
        if ((block[start] & 128) == 0) {
          term.length = block[start];
          term.offset = start+1;
        } else {
          term.length = (((block[start] & 0x7f)) << 8) | (block[1+start] & 0xff);
          term.offset = start+2;
        }

        return term;
      }

      @Override
      public BytesRef term() throws IOException {
        return term;
      }

      @Override
      public long ord() throws IOException {
        return currentOrd;
      }

      @Override
      public int docFreq() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long totalTermFreq() {
        return -1;
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Comparator<BytesRef> getComparator() throws IOException {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      }

      @Override
      public void seek(BytesRef term, TermState state) throws IOException {
        assert state != null && state instanceof OrdTermState;
        this.seek(((OrdTermState)state).ord);
      }

      @Override
      public TermState termState() throws IOException {
        OrdTermState state = new OrdTermState();
        state.ord = currentOrd;
        return state;
      }
    }
  }
}
