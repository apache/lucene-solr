package org.apache.lucene.codecs.lucene40;

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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class Lucene40TermVectorsReader extends TermVectorsReader {

  // NOTE: if you make a new format, it must be larger than
  // the current format

  // Changed strings to UTF8 with length-in-bytes not length-in-chars
  static final int FORMAT_UTF8_LENGTH_IN_BYTES = 4;

  // NOTE: always change this if you switch to a new format!
  // whenever you add a new format, make it 1 larger (positive version logic)!
  static final int FORMAT_CURRENT = FORMAT_UTF8_LENGTH_IN_BYTES;
  
  // when removing support for old versions, leave the last supported version here
  static final int FORMAT_MINIMUM = FORMAT_UTF8_LENGTH_IN_BYTES;

  //The size in bytes that the FORMAT_VERSION will take up at the beginning of each file 
  static final int FORMAT_SIZE = 4;

  static final byte STORE_POSITIONS_WITH_TERMVECTOR = 0x1;

  static final byte STORE_OFFSET_WITH_TERMVECTOR = 0x2;
  
  /** Extension of vectors fields file */
  static final String VECTORS_FIELDS_EXTENSION = "tvf";

  /** Extension of vectors documents file */
  static final String VECTORS_DOCUMENTS_EXTENSION = "tvd";

  /** Extension of vectors index file */
  static final String VECTORS_INDEX_EXTENSION = "tvx";

  private FieldInfos fieldInfos;

  private IndexInput tvx;
  private IndexInput tvd;
  private IndexInput tvf;
  private int size;
  private int numTotalDocs;
  
  private final int format;

  // used by clone
  Lucene40TermVectorsReader(FieldInfos fieldInfos, IndexInput tvx, IndexInput tvd, IndexInput tvf, int size, int numTotalDocs, int format) {
    this.fieldInfos = fieldInfos;
    this.tvx = tvx;
    this.tvd = tvd;
    this.tvf = tvf;
    this.size = size;
    this.numTotalDocs = numTotalDocs;
    this.format = format;
  }
    
  public Lucene40TermVectorsReader(Directory d, SegmentInfo si, FieldInfos fieldInfos, IOContext context)
    throws CorruptIndexException, IOException {
    final String segment = si.name;
    final int size = si.docCount;
    
    boolean success = false;

    try {
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

      this.size = numTotalDocs;
      assert size == 0 || numTotalDocs == size;

      this.fieldInfos = fieldInfos;
      success = true;
    } finally {
      // With lock-less commits, it's entirely possible (and
      // fine) to hit a FileNotFound exception above. In
      // this case, we want to explicitly close any subset
      // of things that were opened so that we don't have to
      // wait for a GC to do so.
      if (!success) {
        close();
      }
    }
  }

  // Used for bulk copy when merging
  IndexInput getTvdStream() {
    return tvd;
  }

  // Used for bulk copy when merging
  IndexInput getTvfStream() {
    return tvf;
  }

  // Not private to avoid synthetic access$NNN methods
  void seekTvx(final int docNum) throws IOException {
    tvx.seek(docNum * 16L + FORMAT_SIZE);
  }

  boolean canReadRawDocs() {
    // we can always read raw docs, unless the term vectors
    // didn't exist
    return format != 0;
  }

  /** Retrieve the length (in bytes) of the tvd and tvf
   *  entries for the next numDocs starting with
   *  startDocID.  This is used for bulk copying when
   *  merging segments, if the field numbers are
   *  congruent.  Once this returns, the tvf & tvd streams
   *  are seeked to the startDocID. */
  final void rawDocs(int[] tvdLengths, int[] tvfLengths, int startDocID, int numDocs) throws IOException {

    if (tvx == null) {
      Arrays.fill(tvdLengths, 0);
      Arrays.fill(tvfLengths, 0);
      return;
    }

    seekTvx(startDocID);

    long tvdPosition = tvx.readLong();
    tvd.seek(tvdPosition);

    long tvfPosition = tvx.readLong();
    tvf.seek(tvfPosition);

    long lastTvdPosition = tvdPosition;
    long lastTvfPosition = tvfPosition;

    int count = 0;
    while (count < numDocs) {
      final int docID = startDocID + count + 1;
      assert docID <= numTotalDocs;
      if (docID < numTotalDocs)  {
        tvdPosition = tvx.readLong();
        tvfPosition = tvx.readLong();
      } else {
        tvdPosition = tvd.length();
        tvfPosition = tvf.length();
        assert count == numDocs-1;
      }
      tvdLengths[count] = (int) (tvdPosition-lastTvdPosition);
      tvfLengths[count] = (int) (tvfPosition-lastTvfPosition);
      count++;
      lastTvdPosition = tvdPosition;
      lastTvfPosition = tvfPosition;
    }
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
    IOUtils.close(tvx, tvd, tvf);
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
    public FieldsEnum iterator() throws IOException {

      return new FieldsEnum() {
        private int fieldUpto;

        @Override
        public String next() throws IOException {
          if (fieldNumbers != null && fieldUpto < fieldNumbers.length) {
            return fieldInfos.fieldName(fieldNumbers[fieldUpto++]);
          } else {
            return null;
          }
        }

        @Override
        public Terms terms() throws IOException {
          return TVFields.this.terms(fieldInfos.fieldName(fieldNumbers[fieldUpto-1]));
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

    public TVTerms(long tvfFP) throws IOException {
      tvf.seek(tvfFP);
      numTerms = tvf.readVInt();
      tvfFPStart = tvf.getFilePointer();
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
      termsEnum.reset(numTerms, tvfFPStart);
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
      // TODO: really indexer hardwires
      // this...?  I guess codec could buffer and re-sort...
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }

  private class TVTermsEnum extends TermsEnum {
    private final IndexInput origTVF;
    private final IndexInput tvf;
    private int numTerms;
    private int nextTerm;
    private int freq;
    private BytesRef lastTerm = new BytesRef();
    private BytesRef term = new BytesRef();
    private boolean storePositions;
    private boolean storeOffsets;
    private long tvfFP;

    private int[] positions;
    private int[] startOffsets;
    private int[] endOffsets;

    // NOTE: tvf is pre-positioned by caller
    public TVTermsEnum() throws IOException {
      this.origTVF = Lucene40TermVectorsReader.this.tvf;
      tvf = (IndexInput) origTVF.clone();
    }

    public boolean canReuse(IndexInput tvf) {
      return tvf == origTVF;
    }

    public void reset(int numTerms, long tvfFPStart) throws IOException {
      this.numTerms = numTerms;
      nextTerm = 0;
      tvf.seek(tvfFPStart);
      final byte bits = tvf.readByte();
      storePositions = (bits & STORE_POSITIONS_WITH_TERMVECTOR) != 0;
      storeOffsets = (bits & STORE_OFFSET_WITH_TERMVECTOR) != 0;
      tvfFP = 1+tvfFPStart;
      positions = null;
      startOffsets = null;
      endOffsets = null;
    }

    // NOTE: slow!  (linear scan)
    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache)
      throws IOException {
      if (nextTerm != 0) {
        final int cmp = text.compareTo(term);
        if (cmp < 0) {
          nextTerm = 0;
          tvf.seek(tvfFP);
        } else if (cmp == 0) {
          return SeekStatus.FOUND;
        }
      }

      while (next() != null) {
        final int cmp = text.compareTo(term);
        if (cmp < 0) {
          return SeekStatus.NOT_FOUND;
        } else if (cmp == 0) {
          return SeekStatus.FOUND;
        }
      }

      return SeekStatus.END;
    }

    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      if (nextTerm >= numTerms) {
        return null;
      }
      term.copyBytes(lastTerm);
      final int start = tvf.readVInt();
      final int deltaLen = tvf.readVInt();
      term.length = start + deltaLen;
      term.grow(term.length);
      tvf.readBytes(term.bytes, start, deltaLen);
      freq = tvf.readVInt();

      if (storePositions) {
        // TODO: we could maybe reuse last array, if we can
        // somehow be careful about consumer never using two
        // D&PEnums at once...
        positions = new int[freq];
        int pos = 0;
        for(int posUpto=0;posUpto<freq;posUpto++) {
          pos += tvf.readVInt();
          positions[posUpto] = pos;
        }
      }

      if (storeOffsets) {
        startOffsets = new int[freq];
        endOffsets = new int[freq];
        int offset = 0;
        for(int posUpto=0;posUpto<freq;posUpto++) {
          startOffsets[posUpto] = offset + tvf.readVInt();
          offset = endOffsets[posUpto] = startOffsets[posUpto] + tvf.readVInt();
        }
      }

      lastTerm.copyBytes(term);
      nextTerm++;
      return term;
    }

    @Override
    public BytesRef term() {
      return term;
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
      return freq;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs /* ignored */) throws IOException {
      TVDocsEnum docsEnum;
      if (reuse != null && reuse instanceof TVDocsEnum) {
        docsEnum = (TVDocsEnum) reuse;
      } else {
        docsEnum = new TVDocsEnum();
      }
      docsEnum.reset(liveDocs, freq);
      return docsEnum;
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
      if (needsOffsets && !storeOffsets) {
        return null;
      }

      if (!storePositions && !storeOffsets) {
        return null;
      }
      
      TVDocsAndPositionsEnum docsAndPositionsEnum;
      if (reuse != null && reuse instanceof TVDocsAndPositionsEnum) {
        docsAndPositionsEnum = (TVDocsAndPositionsEnum) reuse;
      } else {
        docsAndPositionsEnum = new TVDocsAndPositionsEnum();
      }
      docsAndPositionsEnum.reset(liveDocs, positions, startOffsets, endOffsets);
      return docsAndPositionsEnum;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // TODO: really indexer hardwires
      // this...?  I guess codec could buffer and re-sort...
      return BytesRef.getUTF8SortedAsUnicodeComparator();
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
    public int freq() {
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

    public void reset(Bits liveDocs, int freq) {
      this.liveDocs = liveDocs;
      this.freq = freq;
      this.doc = -1;
      didNext = false;
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
    public int freq() {
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

    public void reset(Bits liveDocs, int[] positions, int[] startOffsets, int[] endOffsets) {
      this.liveDocs = liveDocs;
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.endOffsets = endOffsets;
      this.doc = -1;
      didNext = false;
      nextPos = 0;
    }

    @Override
    public BytesRef getPayload() {
      return null;
    }

    @Override
    public boolean hasPayload() {
      return false;
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
      assert startOffsets != null;
      return startOffsets[nextPos-1];
    }

    @Override
    public int endOffset() {
      assert endOffsets != null;
      return endOffsets[nextPos-1];
    }
  }

  @Override
  public Fields get(int docID) throws IOException {
    if (docID < 0 || docID >= numTotalDocs) {
      throw new IllegalArgumentException("doID=" + docID + " is out of bounds [0.." + (numTotalDocs-1) + "]");
    }
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
      cloneTvx = (IndexInput) tvx.clone();
      cloneTvd = (IndexInput) tvd.clone();
      cloneTvf = (IndexInput) tvf.clone();
    }
    
    return new Lucene40TermVectorsReader(fieldInfos, cloneTvx, cloneTvd, cloneTvf, size, numTotalDocs, format);
  }
  
  public static void files(SegmentInfo info, Set<String> files) throws IOException {
    if (info.getHasVectors()) {
      files.add(IndexFileNames.segmentFileName(info.name, "", VECTORS_INDEX_EXTENSION));
      files.add(IndexFileNames.segmentFileName(info.name, "", VECTORS_FIELDS_EXTENSION));
      files.add(IndexFileNames.segmentFileName(info.name, "", VECTORS_DOCUMENTS_EXTENSION));
    }
  }
}

