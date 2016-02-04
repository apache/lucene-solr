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
package org.apache.lucene.codecs.simpletext;


import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

import static org.apache.lucene.codecs.simpletext.SimpleTextTermVectorsWriter.*;

/**
 * Reads plain-text term vectors.
 * <p>
 * <b>FOR RECREATIONAL USE ONLY</b>
 * @lucene.experimental
 */
public class SimpleTextTermVectorsReader extends TermVectorsReader {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(SimpleTextTermVectorsReader.class)
          + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class)
          + RamUsageEstimator.shallowSizeOfInstance(CharsRef.class);

  private long offsets[]; /* docid -> offset in .vec file */
  private IndexInput in;
  private BytesRefBuilder scratch = new BytesRefBuilder();
  private CharsRefBuilder scratchUTF16 = new CharsRefBuilder();

  public SimpleTextTermVectorsReader(Directory directory, SegmentInfo si, IOContext context) throws IOException {
    boolean success = false;
    try {
      in = directory.openInput(IndexFileNames.segmentFileName(si.name, "", VECTORS_EXTENSION), context);
      success = true;
    } finally {
      if (!success) {
        try {
          close();
        } catch (Throwable t) {} // ensure we throw our original exception
      }
    }
    readIndex(si.maxDoc());
  }

  // used by clone
  SimpleTextTermVectorsReader(long offsets[], IndexInput in) {
    this.offsets = offsets;
    this.in = in;
  }

  // we don't actually write a .tvx-like index, instead we read the
  // vectors file in entirety up-front and save the offsets
  // so we can seek to the data later.
  private void readIndex(int maxDoc) throws IOException {
    ChecksumIndexInput input = new BufferedChecksumIndexInput(in);
    offsets = new long[maxDoc];
    int upto = 0;
    while (!scratch.get().equals(END)) {
      SimpleTextUtil.readLine(input, scratch);
      if (StringHelper.startsWith(scratch.get(), DOC)) {
        offsets[upto] = input.getFilePointer();
        upto++;
      }
    }
    SimpleTextUtil.checkFooter(input);
    assert upto == offsets.length;
  }

  @Override
  public Fields get(int doc) throws IOException {
    SortedMap<String,SimpleTVTerms> fields = new TreeMap<>();
    in.seek(offsets[doc]);
    readLine();
    assert StringHelper.startsWith(scratch.get(), NUMFIELDS);
    int numFields = parseIntAt(NUMFIELDS.length);
    if (numFields == 0) {
      return null; // no vectors for this doc
    }
    for (int i = 0; i < numFields; i++) {
      readLine();
      assert StringHelper.startsWith(scratch.get(), FIELD);
      // skip fieldNumber:
      parseIntAt(FIELD.length);

      readLine();
      assert StringHelper.startsWith(scratch.get(), FIELDNAME);
      String fieldName = readString(FIELDNAME.length, scratch);

      readLine();
      assert StringHelper.startsWith(scratch.get(), FIELDPOSITIONS);
      boolean positions = Boolean.parseBoolean(readString(FIELDPOSITIONS.length, scratch));

      readLine();
      assert StringHelper.startsWith(scratch.get(), FIELDOFFSETS);
      boolean offsets = Boolean.parseBoolean(readString(FIELDOFFSETS.length, scratch));

      readLine();
      assert StringHelper.startsWith(scratch.get(), FIELDPAYLOADS);
      boolean payloads = Boolean.parseBoolean(readString(FIELDPAYLOADS.length, scratch));

      readLine();
      assert StringHelper.startsWith(scratch.get(), FIELDTERMCOUNT);
      int termCount = parseIntAt(FIELDTERMCOUNT.length);

      SimpleTVTerms terms = new SimpleTVTerms(offsets, positions, payloads);
      fields.put(fieldName, terms);

      BytesRefBuilder term = new BytesRefBuilder();
      for (int j = 0; j < termCount; j++) {
        readLine();
        assert StringHelper.startsWith(scratch.get(), TERMTEXT);
        int termLength = scratch.length() - TERMTEXT.length;
        term.grow(termLength);
        term.setLength(termLength);
        System.arraycopy(scratch.bytes(), TERMTEXT.length, term.bytes(), 0, termLength);

        SimpleTVPostings postings = new SimpleTVPostings();
        terms.terms.put(term.toBytesRef(), postings);

        readLine();
        assert StringHelper.startsWith(scratch.get(), TERMFREQ);
        postings.freq = parseIntAt(TERMFREQ.length);

        if (positions || offsets) {
          if (positions) {
            postings.positions = new int[postings.freq];
            if (payloads) {
              postings.payloads = new BytesRef[postings.freq];
            }
          }

          if (offsets) {
            postings.startOffsets = new int[postings.freq];
            postings.endOffsets = new int[postings.freq];
          }

          for (int k = 0; k < postings.freq; k++) {
            if (positions) {
              readLine();
              assert StringHelper.startsWith(scratch.get(), POSITION);
              postings.positions[k] = parseIntAt(POSITION.length);
              if (payloads) {
                readLine();
                assert StringHelper.startsWith(scratch.get(), PAYLOAD);
                if (scratch.length() - PAYLOAD.length == 0) {
                  postings.payloads[k] = null;
                } else {
                  byte payloadBytes[] = new byte[scratch.length() - PAYLOAD.length];
                  System.arraycopy(scratch.bytes(), PAYLOAD.length, payloadBytes, 0, payloadBytes.length);
                  postings.payloads[k] = new BytesRef(payloadBytes);
                }
              }
            }

            if (offsets) {
              readLine();
              assert StringHelper.startsWith(scratch.get(), STARTOFFSET);
              postings.startOffsets[k] = parseIntAt(STARTOFFSET.length);

              readLine();
              assert StringHelper.startsWith(scratch.get(), ENDOFFSET);
              postings.endOffsets[k] = parseIntAt(ENDOFFSET.length);
            }
          }
        }
      }
    }
    return new SimpleTVFields(fields);
  }

  @Override
  public TermVectorsReader clone() {
    if (in == null) {
      throw new AlreadyClosedException("this TermVectorsReader is closed");
    }
    return new SimpleTextTermVectorsReader(offsets, in.clone());
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(in);
    } finally {
      in = null;
      offsets = null;
    }
  }

  private void readLine() throws IOException {
    SimpleTextUtil.readLine(in, scratch);
  }

  private int parseIntAt(int offset) {
    scratchUTF16.copyUTF8Bytes(scratch.bytes(), offset, scratch.length()-offset);
    return ArrayUtil.parseInt(scratchUTF16.chars(), 0, scratchUTF16.length());
  }

  private String readString(int offset, BytesRefBuilder scratch) {
    scratchUTF16.copyUTF8Bytes(scratch.bytes(), offset, scratch.length()-offset);
    return scratchUTF16.toString();
  }

  private class SimpleTVFields extends Fields {
    private final SortedMap<String,SimpleTVTerms> fields;

    SimpleTVFields(SortedMap<String,SimpleTVTerms> fields) {
      this.fields = fields;
    }

    @Override
    public Iterator<String> iterator() {
      return Collections.unmodifiableSet(fields.keySet()).iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      return fields.get(field);
    }

    @Override
    public int size() {
      return fields.size();
    }
  }

  private static class SimpleTVTerms extends Terms {
    final SortedMap<BytesRef,SimpleTVPostings> terms;
    final boolean hasOffsets;
    final boolean hasPositions;
    final boolean hasPayloads;

    SimpleTVTerms(boolean hasOffsets, boolean hasPositions, boolean hasPayloads) {
      this.hasOffsets = hasOffsets;
      this.hasPositions = hasPositions;
      this.hasPayloads = hasPayloads;
      terms = new TreeMap<>();
    }

    @Override
    public TermsEnum iterator() throws IOException {
      // TODO: reuse
      return new SimpleTVTermsEnum(terms);
    }

    @Override
    public long size() throws IOException {
      return terms.size();
    }

    @Override
    public long getSumTotalTermFreq() throws IOException {
      return -1;
    }

    @Override
    public long getSumDocFreq() throws IOException {
      return terms.size();
    }

    @Override
    public int getDocCount() throws IOException {
      return 1;
    }

    @Override
    public boolean hasFreqs() {
      return true;
    }

    @Override
    public boolean hasOffsets() {
      return hasOffsets;
    }

    @Override
    public boolean hasPositions() {
      return hasPositions;
    }

    @Override
    public boolean hasPayloads() {
      return hasPayloads;
    }
  }

  private static class SimpleTVPostings {
    private int freq;
    private int positions[];
    private int startOffsets[];
    private int endOffsets[];
    private BytesRef payloads[];
  }

  private static class SimpleTVTermsEnum extends TermsEnum {
    SortedMap<BytesRef,SimpleTVPostings> terms;
    Iterator<Map.Entry<BytesRef,SimpleTextTermVectorsReader.SimpleTVPostings>> iterator;
    Map.Entry<BytesRef,SimpleTextTermVectorsReader.SimpleTVPostings> current;

    SimpleTVTermsEnum(SortedMap<BytesRef,SimpleTVPostings> terms) {
      this.terms = terms;
      this.iterator = terms.entrySet().iterator();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      iterator = terms.tailMap(text).entrySet().iterator();
      if (!iterator.hasNext()) {
        return SeekStatus.END;
      } else {
        return next().equals(text) ? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public BytesRef next() throws IOException {
      if (!iterator.hasNext()) {
        return null;
      } else {
        current = iterator.next();
        return current.getKey();
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return current.getKey();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      return 1;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return current.getValue().freq;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      
      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        SimpleTVPostings postings = current.getValue();
        if (postings.positions != null || postings.startOffsets != null) {
          // TODO: reuse
          SimpleTVPostingsEnum e = new SimpleTVPostingsEnum();
          e.reset(postings.positions, postings.startOffsets, postings.endOffsets, postings.payloads);
          return e;
        } else if (PostingsEnum.featureRequested(flags, DocsAndPositionsEnum.OLD_NULL_SEMANTICS)) {
          return null;
        }
      }

      // TODO: reuse
      SimpleTVDocsEnum e = new SimpleTVDocsEnum();
      e.reset(PostingsEnum.featureRequested(flags, PostingsEnum.FREQS) == false ? 1 : current.getValue().freq);
      return e;
    }

  }

  // note: these two enum classes are exactly like the Default impl...
  private static class SimpleTVDocsEnum extends PostingsEnum {
    private boolean didNext;
    private int doc = -1;
    private int freq;

    @Override
    public int freq() throws IOException {
      assert freq != -1;
      return freq;
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      if (!didNext) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    public void reset(int freq) {
      this.freq = freq;
      this.doc = -1;
      didNext = false;
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  private static class SimpleTVPostingsEnum extends PostingsEnum {
    private boolean didNext;
    private int doc = -1;
    private int nextPos;
    private int[] positions;
    private BytesRef[] payloads;
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
      if (!didNext) {
        didNext = true;
        return (doc = 0);
      } else {
        return (doc = NO_MORE_DOCS);
      }
    }

    @Override
    public int advance(int target) throws IOException {
      return slowAdvance(target);
    }

    public void reset(int[] positions, int[] startOffsets, int[] endOffsets, BytesRef payloads[]) {
      this.positions = positions;
      this.startOffsets = startOffsets;
      this.endOffsets = endOffsets;
      this.payloads = payloads;
      this.doc = -1;
      didNext = false;
      nextPos = 0;
    }

    @Override
    public BytesRef getPayload() {
      return payloads == null ? null : payloads[nextPos-1];
    }

    @Override
    public int nextPosition() {
      if (positions != null) {
        assert nextPos < positions.length;
        return positions[nextPos++];
      } else {
        assert nextPos < startOffsets.length;
        nextPos++;
        return -1;
      }
    }

    @Override
    public int startOffset() {
      if (startOffsets == null) {
        return -1;
      } else {
        return startOffsets[nextPos-1];
      }
    }

    @Override
    public int endOffset() {
      if (endOffsets == null) {
        return -1;
      } else {
        return endOffsets[nextPos-1];
      }
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(offsets);
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public void checkIntegrity() throws IOException {}
}
