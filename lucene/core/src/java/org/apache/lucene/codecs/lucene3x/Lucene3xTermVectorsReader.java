package org.apache.lucene.codecs.lucene3x;

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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/** @deprecated Only for reading existing 3.x indexes */
@Deprecated
class Lucene3xTermVectorsReader extends TermVectorsReader {

  // NOTE: if you make a new format, it must be larger than
  // the current format

  // Changed strings to UTF8 with length-in-bytes not length-in-chars
  static final int FORMAT_UTF8_LENGTH_IN_BYTES = 4;

  // NOTE: always change this if you switch to a new format!
  // whenever you add a new format, make it 1 larger (positive version logic)!
  public static final int FORMAT_CURRENT = FORMAT_UTF8_LENGTH_IN_BYTES;
  
  // when removing support for old versions, leave the last supported version here
  public static final int FORMAT_MINIMUM = FORMAT_UTF8_LENGTH_IN_BYTES;

  //The size in bytes that the FORMAT_VERSION will take up at the beginning of each file 
  static final int FORMAT_SIZE = 4;

  public static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x1;

  public static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x2;
  
  /** Extension of vectors fields file */
  public static final String VECTORS_FIELDS_EXTENSION = "tvf";

  /** Extension of vectors documents file */
  public static final String VECTORS_DOCUMENTS_EXTENSION = "tvd";

  /** Extension of vectors index file */
  public static final String VECTORS_INDEX_EXTENSION = "tvx";

  private FieldInfos fieldInfos;

  private IndexInput tvx;
  private IndexInput tvd;
  private IndexInput tvf;
  private int size;
  private int numTotalDocs;

  // The docID offset where our docs begin in the index
  // file.  This will be 0 if we have our own private file.
  private int docStoreOffset;
  
  // when we are inside a compound share doc store (CFX),
  // (lucene 3.0 indexes only), we privately open our own fd.
  // TODO: if we are worried, maybe we could eliminate the
  // extra fd somehow when you also have vectors...
  private final CompoundFileDirectory storeCFSReader;
  
  private final int format;

  // used by clone
  Lucene3xTermVectorsReader(FieldInfos fieldInfos, IndexInput tvx, IndexInput tvd, IndexInput tvf, int size, int numTotalDocs, int docStoreOffset, int format) {
    this.fieldInfos = fieldInfos;
    this.tvx = tvx;
    this.tvd = tvd;
    this.tvf = tvf;
    this.size = size;
    this.numTotalDocs = numTotalDocs;
    this.docStoreOffset = docStoreOffset;
    this.format = format;
    this.storeCFSReader = null;
  }
    
  public Lucene3xTermVectorsReader(Directory d, SegmentInfo si, FieldInfos fieldInfos, IOContext context)
    throws CorruptIndexException, IOException {
    final String segment = Lucene3xSegmentInfoFormat.getDocStoreSegment(si);
    final int docStoreOffset = Lucene3xSegmentInfoFormat.getDocStoreOffset(si);
    final int size = si.getDocCount();
    
    boolean success = false;

    try {
      if (docStoreOffset != -1 && Lucene3xSegmentInfoFormat.getDocStoreIsCompoundFile(si)) {
        d = storeCFSReader = new CompoundFileDirectory(si.dir, 
            IndexFileNames.segmentFileName(segment, "", Lucene3xCodec.COMPOUND_FILE_STORE_EXTENSION), context, false);
      } else {
        storeCFSReader = null;
      }
      String idxName = IndexFileNames.segmentFileName(segment, "", VECTORS_INDEX_EXTENSION);
      tvx = d.openInput(idxName, context);
      format = checkValidFormat(tvx);
      String fn = IndexFileNames.segmentFileName(segment, "", VECTORS_DOCUMENTS_EXTENSION);
      tvd = d.openInput(fn, context);
      final int tvdFormat = checkValidFormat(tvd);
      fn = IndexFileNames.segmentFileName(segment, "", VECTORS_FIELDS_EXTENSION);
      tvf = d.openInput(fn, context);
      final int tvfFormat = checkValidFormat(tvf);

      assert format == tvdFormat;
      assert format == tvfFormat;

      numTotalDocs = (int) (tvx.length() >> 4);

      if (-1 == docStoreOffset) {
        this.docStoreOffset = 0;
        this.size = numTotalDocs;
        assert size == 0 || numTotalDocs == size;
      } else {
        this.docStoreOffset = docStoreOffset;
        this.size = size;
        // Verify the file is long enough to hold all of our
        // docs
        assert numTotalDocs >= size + docStoreOffset: "numTotalDocs=" + numTotalDocs + " size=" + size + " docStoreOffset=" + docStoreOffset;
      }

      this.fieldInfos = fieldInfos;
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        try {
          close();
        } catch (Throwable t) {} // keep our original exception
      }
    }
  }

  // Not private to avoid synthetic access$NNN methods
  void seekTvx(final int docNum) throws IOException {
    tvx.seek((docNum + docStoreOffset) * 16L + FORMAT_SIZE);
  }

  private int checkValidFormat(IndexInput in) throws CorruptIndexException, IOException
  {
    int format = in.readInt();
    if (format < FORMAT_MINIMUM)
      throw new IndexFormatTooOldException(in, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    if (format > FORMAT_CURRENT)
      throw new IndexFormatTooNewException(in, format, FORMAT_MINIMUM, FORMAT_CURRENT);
    return format;
  }

  public void close() throws IOException {
    IOUtils.close(tvx, tvd, tvf, storeCFSReader);
  }

  /**
   * 
   * @return The number of documents in the reader
   */
  int size() {
    return size;
  }

  private class TVFields extends Fields {
    private final int[] fieldNumbers;
    private final long[] fieldFPs;
    private final Map<Integer,Integer> fieldNumberToIndex = new HashMap<Integer,Integer>();

    public TVFields(int docID) throws IOException {
      seekTvx(docID);
      tvd.seek(tvx.readLong());
      
      final int fieldCount = tvd.readVInt();
      assert fieldCount >= 0;
      if (fieldCount != 0) {
        fieldNumbers = new int[fieldCount];
        fieldFPs = new long[fieldCount];
        for(int fieldUpto=0;fieldUpto<fieldCount;fieldUpto++) {
          final int fieldNumber = tvd.readVInt();
          fieldNumbers[fieldUpto] = fieldNumber;
          fieldNumberToIndex.put(fieldNumber, fieldUpto);
        }

        long position = tvx.readLong();
        fieldFPs[0] = position;
        for(int fieldUpto=1;fieldUpto<fieldCount;fieldUpto++) {
          position += tvd.readVLong();
          fieldFPs[fieldUpto] = position;
        }
      } else {
        // TODO: we can improve writer here, eg write 0 into
        // tvx file, so we know on first read from tvx that
        // this doc has no TVs
        fieldNumbers = null;
        fieldFPs = null;
      }
    }
    
    @Override
    public Iterator<String> iterator() {
      return new Iterator<String>() {
        private int fieldUpto;

        @Override
        public String next() {
          if (fieldNumbers != null && fieldUpto < fieldNumbers.length) {
            return fieldInfos.fieldInfo(fieldNumbers[fieldUpto++]).name;
          } else {
            throw new NoSuchElementException();
          }
        }

        @Override
        public boolean hasNext() {
          return fieldNumbers != null && fieldUpto < fieldNumbers.length;
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public Terms terms(String field) throws IOException {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
      if (fieldInfo == null) {
        // No such field
        return null;
      }

      final Integer fieldIndex = fieldNumberToIndex.get(fieldInfo.number);
      if (fieldIndex == null) {
        // Term vectors were not indexed for this field
        return null;
      }

      return new TVTerms(fieldFPs[fieldIndex]);
    }

    @Override
    public int size() {
      if (fieldNumbers == null) {
        return 0;
      } else {
        return fieldNumbers.length;
      }
    }
  }

  private class TVTerms extends Terms {
    private final int numTerms;
    private final long tvfFPStart;
    private final boolean storePositions;
    private final boolean storeOffsets;
    private final boolean unicodeSortOrder;

    public TVTerms(long tvfFP) throws IOException {
      tvf.seek(tvfFP);
      numTerms = tvf.readVInt();
      final byte bits = tvf.readByte();
      storePositions = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      storeOffsets = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
      tvfFPStart = tvf.getFilePointer();
      unicodeSortOrder = sortTermsByUnicode();
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      TVTermsEnum termsEnum;
      if (reuse instanceof TVTermsEnum) {
        termsEnum = (TVTermsEnum) reuse;
        if (!termsEnum.canReuse(tvf)) {
          termsEnum = new TVTermsEnum();
        }
      } else {
        termsEnum = new TVTermsEnum();
      }
      termsEnum.reset(numTerms, tvfFPStart, storePositions, storeOffsets, unicodeSortOrder);
      return termsEnum;
    }

    @Override
    public long size() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return -1;
    }

    @Override
    public long getSumDocFreq() {
      // Every term occurs in just one doc:
      return numTerms;
    }

    @Override
    public int getDocCount() {
      return 1;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      if (unicodeSortOrder) {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      } else {
        return BytesRef.getUTF8SortedAsUTF16Comparator();
      }
    }

    @Override
    public boolean hasOffsets() {
      return storeOffsets;
    }

    @Override
    public boolean hasPositions() {
      return storePositions;
    }

    @Override
    public boolean hasPayloads() {
      return false;
    }
  }

  static class TermAndPostings {
    BytesRef term;
    int freq;
    int[] positions;
    int[] startOffsets;
    int[] endOffsets;
  }
  
  private class TVTermsEnum extends TermsEnum {
    private boolean unicodeSortOrder;
    private final IndexInput origTVF;
    private final IndexInput tvf;
    private int numTerms;
    private int currentTerm;
    private boolean storePositions;
    private boolean storeOffsets;
    
    private TermAndPostings[] termAndPostings;

    // NOTE: tvf is pre-positioned by caller
    public TVTermsEnum() throws IOException {
      this.origTVF = Lucene3xTermVectorsReader.this.tvf;
      tvf = origTVF.clone();
    }

    public boolean canReuse(IndexInput tvf) {
      return tvf == origTVF;
    }

    public void reset(int numTerms, long tvfFPStart, boolean storePositions, boolean storeOffsets, boolean unicodeSortOrder) throws IOException {
      this.numTerms = numTerms;
      this.storePositions = storePositions;
      this.storeOffsets = storeOffsets;
      currentTerm = -1;
      tvf.seek(tvfFPStart);
      this.unicodeSortOrder = unicodeSortOrder;
      readVectors();
      if (unicodeSortOrder) {
        Arrays.sort(termAndPostings, new Comparator<TermAndPostings>() {
          public int compare(TermAndPostings left, TermAndPostings right) {
            return left.term.compareTo(right.term);
          }
        });
      }
    }
    
    private void readVectors() throws IOException {
      termAndPostings = new TermAndPostings[numTerms];
      BytesRef lastTerm = new BytesRef();
      for (int i = 0; i < numTerms; i++) {
        TermAndPostings t = new TermAndPostings();
        BytesRef term = new BytesRef();
        term.copyBytes(lastTerm);
        final int start = tvf.readVInt();
        final int deltaLen = tvf.readVInt();
        term.length = start + deltaLen;
        term.grow(term.length);
        tvf.readBytes(term.bytes, start, deltaLen);
        t.term = term;
        int freq = tvf.readVInt();
        t.freq = freq;
        
        if (storePositions) {
          int positions[] = new int[freq];
          int pos = 0;
          for(int posUpto=0;posUpto<freq;posUpto++) {
            int delta = tvf.readVInt();
            if (delta == -1) {
              delta = 0; // LUCENE-1542 correction
            }
            pos += delta;
            positions[posUpto] = pos;
          }
          t.positions = positions;
        }

        if (storeOffsets) {
          int startOffsets[] = new int[freq];
          int endOffsets[] = new int[freq];
          int offset = 0;
          for(int posUpto=0;posUpto<freq;posUpto++) {
            startOffsets[posUpto] = offset + tvf.readVInt();
            offset = endOffsets[posUpto] = startOffsets[posUpto] + tvf.readVInt();
          }
          t.startOffsets = startOffsets;
          t.endOffsets = endOffsets;
        }
        lastTerm.copyBytes(term);
        termAndPostings[i] = t;
      }
    }

    // NOTE: slow!  (linear scan)
    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
      Comparator<BytesRef> comparator = getComparator();
      for (int i = 0; i < numTerms; i++) {
        int cmp = comparator.compare(text, termAndPostings[i].term);
        if (cmp < 0) {
          currentTerm = i;
          return SeekStatus.NOT_FOUND;
        } else if (cmp == 0) {
          currentTerm = i;
          return SeekStatus.FOUND;
        }
      }
      currentTerm = termAndPostings.length;
      return SeekStatus.END;
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      if (++currentTerm >= numTerms) {
        return null;
      }
      return term();
    }

    @Override
    public BytesRef term() {
      return termAndPostings[currentTerm].term;
    }

    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() {
      return 1;
    }

    @Override
    public long totalTermFreq() {
      return termAndPostings[currentTerm].freq;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags /* ignored */) throws IOException {
      TVDocsEnum docsEnum;
      if (reuse != null && reuse instanceof TVDocsEnum) {
        docsEnum = (TVDocsEnum) reuse;
      } else {
        docsEnum = new TVDocsEnum();
      }
      docsEnum.reset(liveDocs, termAndPostings[currentTerm]);
      return docsEnum;
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      if (!storePositions && !storeOffsets) {
        return null;
      }
      
      TVDocsAndPositionsEnum docsAndPositionsEnum;
      if (reuse != null && reuse instanceof TVDocsAndPositionsEnum) {
        docsAndPositionsEnum = (TVDocsAndPositionsEnum) reuse;
      } else {
        docsAndPositionsEnum = new TVDocsAndPositionsEnum();
      }
      docsAndPositionsEnum.reset(liveDocs, termAndPostings[currentTerm]);
      return docsAndPositionsEnum;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      if (unicodeSortOrder) {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
      } else {
        return BytesRef.getUTF8SortedAsUTF16Comparator();
      }
    }
  }

  // NOTE: sort of a silly class, since you can get the
  // freq() already by TermsEnum.totalTermFreq
  private static class TVDocsEnum extends DocsEnum {
    private boolean didNext;
    private int doc = -1;
    private int freq;
    private Bits liveDocs;

    @Override
    public int freq() throws IOException {
      return freq;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext && (liveDocs == null || liveDocs.get(0))) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) {
      if (!didNext && target == 0) {
        return nextDoc();
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    public void reset(Bits liveDocs, TermAndPostings termAndPostings) {
      this.liveDocs = liveDocs;
      this.freq = termAndPostings.freq;
      this.doc = -1;
      didNext = false;
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  private static class TVDocsAndPositionsEnum extends DocsAndPositionsEnum {
    private boolean didNext;
    private int doc = -1;
    private int nextPos;
    private Bits liveDocs;
    private int[] positions;
    private int[] startOffsets;
    private int[] endOffsets;

    @Override
    public int freq() throws IOException {
      if (positions != null) {
        return positions.length;
      } else {
        assert startOffsets != null;
        return startOffsets.length;
      }
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext && (liveDocs == null || liveDocs.get(0))) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) {
      if (!didNext && target == 0) {
        return nextDoc();
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    public void reset(Bits liveDocs, TermAndPostings termAndPostings) {
      this.liveDocs = liveDocs;
      this.positions = termAndPostings.positions;
      this.startOffsets = termAndPostings.startOffsets;
      this.endOffsets = termAndPostings.endOffsets;
      this.doc = -1;
      didNext = false;
      nextPos = 0;
    }

    @Override
    public BytesRef getPayload() {
      return null;
    }

    @Override
    public int nextPosition() {
      assert (positions != null && nextPos < positions.length) ||
        startOffsets != null && nextPos < startOffsets.length;

      if (positions != null) {
        return positions[nextPos++];
      } else {
        nextPos++;
        return -1;
      }
    }

    @Override
    public int startOffset() {
      if (startOffsets != null) {
        return startOffsets[nextPos-1];
      } else {
        return -1;
      }
    }

    @Override
    public int endOffset() {
      if (endOffsets != null) {
        return endOffsets[nextPos-1];
      } else {
        return -1;
      }
    }
    
    @Override
    public long cost() {
      return 1;
    }
  }

  @Override
  public Fields get(int docID) throws IOException {
    if (tvx != null) {
      Fields fields = new TVFields(docID);
      if (fields.size() == 0) {
        // TODO: we can improve writer here, eg write 0 into
        // tvx file, so we know on first read from tvx that
        // this doc has no TVs
        return null;
      } else {
        return fields;
      }
    } else {
      return null;
    }
  }

  @Override
  public TermVectorsReader clone() {
    IndexInput cloneTvx = null;
    IndexInput cloneTvd = null;
    IndexInput cloneTvf = null;

    // These are null when a TermVectorsReader was created
    // on a segment that did not have term vectors saved
    if (tvx != null && tvd != null && tvf != null) {
      cloneTvx = tvx.clone();
      cloneTvd = tvd.clone();
      cloneTvf = tvf.clone();
    }
    
    return new Lucene3xTermVectorsReader(fieldInfos, cloneTvx, cloneTvd, cloneTvf, size, numTotalDocs, docStoreOffset, format);
  }
  
  // If this returns, we do the surrogates shuffle so that the
  // terms are sorted by unicode sort order.  This should be
  // true when segments are used for "normal" searching;
  // it's only false during testing, to create a pre-flex
  // index, using the test-only PreFlexRW.
  protected boolean sortTermsByUnicode() {
    return true;
  }
}

