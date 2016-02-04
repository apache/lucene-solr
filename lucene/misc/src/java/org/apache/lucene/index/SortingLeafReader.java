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
package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.Sorter.DocMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TimSorter;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * An {@link org.apache.lucene.index.LeafReader} which supports sorting documents by a given
 * {@link Sort}. You can use this class to sort an index as follows:
 *
 * <pre class="prettyprint">
 * IndexWriter writer; // writer to which the sorted index will be added
 * DirectoryReader reader; // reader on the input index
 * Sort sort; // determines how the documents are sorted
 * LeafReader sortingReader = SortingLeafReader.wrap(SlowCompositeReaderWrapper.wrap(reader), sort);
 * writer.addIndexes(reader);
 * writer.close();
 * reader.close();
 * </pre>
 *
 * @lucene.experimental
 */
public class SortingLeafReader extends FilterLeafReader {

  private static class SortingFields extends FilterFields {

    private final Sorter.DocMap docMap;
    private final FieldInfos infos;

    public SortingFields(final Fields in, FieldInfos infos, Sorter.DocMap docMap) {
      super(in);
      this.docMap = docMap;
      this.infos = infos;
    }

    @Override
    public Terms terms(final String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      } else {
        return new SortingTerms(terms, infos.fieldInfo(field).getIndexOptions(), docMap);
      }
    }

  }

  private static class SortingTerms extends FilterTerms {

    private final Sorter.DocMap docMap;
    private final IndexOptions indexOptions;

    public SortingTerms(final Terms in, IndexOptions indexOptions, final Sorter.DocMap docMap) {
      super(in);
      this.docMap = docMap;
      this.indexOptions = indexOptions;
    }

    @Override
    public TermsEnum iterator() throws IOException {
      return new SortingTermsEnum(in.iterator(), docMap, indexOptions, hasPositions());
    }

    @Override
    public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm)
        throws IOException {
      return new SortingTermsEnum(in.intersect(compiled, startTerm), docMap, indexOptions, hasPositions());
    }

  }

  private static class SortingTermsEnum extends FilterTermsEnum {

    final Sorter.DocMap docMap; // pkg-protected to avoid synthetic accessor methods
    private final IndexOptions indexOptions;
    private final boolean hasPositions;

    public SortingTermsEnum(final TermsEnum in, Sorter.DocMap docMap, IndexOptions indexOptions, boolean hasPositions) {
      super(in);
      this.docMap = docMap;
      this.indexOptions = indexOptions;
      this.hasPositions = hasPositions;
    }

    Bits newToOld(final Bits liveDocs) {
      if (liveDocs == null) {
        return null;
      }
      return new Bits() {

        @Override
        public boolean get(int index) {
          return liveDocs.get(docMap.oldToNew(index));
        }

        @Override
        public int length() {
          return liveDocs.length();
        }

      };
    }

    @Override
    public PostingsEnum postings( PostingsEnum reuse, final int flags) throws IOException {

      if (hasPositions && PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        final PostingsEnum inReuse;
        final SortingPostingsEnum wrapReuse;
        if (reuse != null && reuse instanceof SortingPostingsEnum) {
          // if we're asked to reuse the given DocsEnum and it is Sorting, return
          // the wrapped one, since some Codecs expect it.
          wrapReuse = (SortingPostingsEnum) reuse;
          inReuse = wrapReuse.getWrapped();
        } else {
          wrapReuse = null;
          inReuse = reuse;
        }

        final PostingsEnum inDocsAndPositions = in.postings(inReuse, flags);
        // we ignore the fact that offsets may be stored but not asked for,
        // since this code is expected to be used during addIndexes which will
        // ask for everything. if that assumption changes in the future, we can
        // factor in whether 'flags' says offsets are not required.
        final boolean storeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        return new SortingPostingsEnum(docMap.size(), wrapReuse, inDocsAndPositions, docMap, storeOffsets);
      }

      final PostingsEnum inReuse;
      final SortingDocsEnum wrapReuse;
      if (reuse != null && reuse instanceof SortingDocsEnum) {
        // if we're asked to reuse the given DocsEnum and it is Sorting, return
        // the wrapped one, since some Codecs expect it.
        wrapReuse = (SortingDocsEnum) reuse;
        inReuse = wrapReuse.getWrapped();
      } else {
        wrapReuse = null;
        inReuse = reuse;
      }

      final PostingsEnum inDocs = in.postings(inReuse, flags);
      final boolean withFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >=0 && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS);
      return new SortingDocsEnum(docMap.size(), wrapReuse, inDocs, withFreqs, docMap);
    }

  }

  private static class SortingBinaryDocValues extends BinaryDocValues {

    private final BinaryDocValues in;
    private final Sorter.DocMap docMap;

    SortingBinaryDocValues(BinaryDocValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public BytesRef get(int docID) {
      return in.get(docMap.newToOld(docID));
    }
  }

  private static class SortingNumericDocValues extends NumericDocValues {

    private final NumericDocValues in;
    private final Sorter.DocMap docMap;

    public SortingNumericDocValues(final NumericDocValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public long get(int docID) {
      return in.get(docMap.newToOld(docID));
    }
  }

  private static class SortingSortedNumericDocValues extends SortedNumericDocValues {

    private final SortedNumericDocValues in;
    private final Sorter.DocMap docMap;

    SortingSortedNumericDocValues(SortedNumericDocValues in, DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public int count() {
      return in.count();
    }

    @Override
    public void setDocument(int doc) {
      in.setDocument(docMap.newToOld(doc));
    }

    @Override
    public long valueAt(int index) {
      return in.valueAt(index);
    }
  }

  private static class SortingBits implements Bits {

    private final Bits in;
    private final Sorter.DocMap docMap;

    public SortingBits(final Bits in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public boolean get(int index) {
      return in.get(docMap.newToOld(index));
    }

    @Override
    public int length() {
      return in.length();
    }
  }

  private static class SortingSortedDocValues extends SortedDocValues {

    private final SortedDocValues in;
    private final Sorter.DocMap docMap;

    SortingSortedDocValues(SortedDocValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public int getOrd(int docID) {
      return in.getOrd(docMap.newToOld(docID));
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return in.getValueCount();
    }

    @Override
    public BytesRef get(int docID) {
      return in.get(docMap.newToOld(docID));
    }

    @Override
    public int lookupTerm(BytesRef key) {
      return in.lookupTerm(key);
    }
  }

  private static class SortingSortedSetDocValues extends SortedSetDocValues {

    private final SortedSetDocValues in;
    private final Sorter.DocMap docMap;

    SortingSortedSetDocValues(SortedSetDocValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public long nextOrd() {
      return in.nextOrd();
    }

    @Override
    public void setDocument(int docID) {
      in.setDocument(docMap.newToOld(docID));
    }

    @Override
    public BytesRef lookupOrd(long ord) {
      return in.lookupOrd(ord);
    }

    @Override
    public long getValueCount() {
      return in.getValueCount();
    }

    @Override
    public long lookupTerm(BytesRef key) {
      return in.lookupTerm(key);
    }
  }

  static class SortingDocsEnum extends FilterPostingsEnum {

    private static final class DocFreqSorter extends TimSorter {

      private int[] docs;
      private int[] freqs;
      private final int[] tmpDocs;
      private int[] tmpFreqs;

      public DocFreqSorter(int maxDoc) {
        super(maxDoc / 64);
        this.tmpDocs = new int[maxDoc / 64];
      }

      public void reset(int[] docs, int[] freqs) {
        this.docs = docs;
        this.freqs = freqs;
        if (freqs != null && tmpFreqs == null) {
          tmpFreqs = new int[tmpDocs.length];
        }
      }

      @Override
      protected int compare(int i, int j) {
        return docs[i] - docs[j];
      }

      @Override
      protected void swap(int i, int j) {
        int tmpDoc = docs[i];
        docs[i] = docs[j];
        docs[j] = tmpDoc;

        if (freqs != null) {
          int tmpFreq = freqs[i];
          freqs[i] = freqs[j];
          freqs[j] = tmpFreq;
        }
      }

      @Override
      protected void copy(int src, int dest) {
        docs[dest] = docs[src];
        if (freqs != null) {
          freqs[dest] = freqs[src];
        }
      }

      @Override
      protected void save(int i, int len) {
        System.arraycopy(docs, i, tmpDocs, 0, len);
        if (freqs != null) {
          System.arraycopy(freqs, i, tmpFreqs, 0, len);
        }
      }

      @Override
      protected void restore(int i, int j) {
        docs[j] = tmpDocs[i];
        if (freqs != null) {
          freqs[j] = tmpFreqs[i];
        }
      }

      @Override
      protected int compareSaved(int i, int j) {
        return tmpDocs[i] - docs[j];
      }
    }

    private final int maxDoc;
    private final DocFreqSorter sorter;
    private int[] docs;
    private int[] freqs;
    private int docIt = -1;
    private final int upto;
    private final boolean withFreqs;

    SortingDocsEnum(int maxDoc, SortingDocsEnum reuse, final PostingsEnum in, boolean withFreqs, final Sorter.DocMap docMap) throws IOException {
      super(in);
      this.maxDoc = maxDoc;
      this.withFreqs = withFreqs;
      if (reuse != null) {
        if (reuse.maxDoc == maxDoc) {
          sorter = reuse.sorter;
        } else {
          sorter = new DocFreqSorter(maxDoc);
        }
        docs = reuse.docs;
        freqs = reuse.freqs; // maybe null
      } else {
        docs = new int[64];
        sorter = new DocFreqSorter(maxDoc);
      }
      docIt = -1;
      int i = 0;
      int doc;
      if (withFreqs) {
        if (freqs == null || freqs.length < docs.length) {
          freqs = new int[docs.length];
        }
        while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS){
          if (i >= docs.length) {
            docs = ArrayUtil.grow(docs, docs.length + 1);
            freqs = ArrayUtil.grow(freqs, freqs.length + 1);
          }
          docs[i] = docMap.oldToNew(doc);
          freqs[i] = in.freq();
          ++i;
        }
      } else {
        freqs = null;
        while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS){
          if (i >= docs.length) {
            docs = ArrayUtil.grow(docs, docs.length + 1);
          }
          docs[i++] = docMap.oldToNew(doc);
        }
      }
      // TimSort can save much time compared to other sorts in case of
      // reverse sorting, or when sorting a concatenation of sorted readers
      sorter.reset(docs, freqs);
      sorter.sort(0, i);
      upto = i;
    }

    // for testing
    boolean reused(PostingsEnum other) {
      if (other == null || !(other instanceof SortingDocsEnum)) {
        return false;
      }
      return docs == ((SortingDocsEnum) other).docs;
    }

    @Override
    public int advance(final int target) throws IOException {
      // need to support it for checkIndex, but in practice it won't be called, so
      // don't bother to implement efficiently for now.
      return slowAdvance(target);
    }

    @Override
    public int docID() {
      return docIt < 0 ? -1 : docIt >= upto ? NO_MORE_DOCS : docs[docIt];
    }

    @Override
    public int freq() throws IOException {
      return withFreqs && docIt < upto ? freqs[docIt] : 1;
    }

    @Override
    public int nextDoc() throws IOException {
      if (++docIt >= upto) return NO_MORE_DOCS;
      return docs[docIt];
    }

    /** Returns the wrapped {@link PostingsEnum}. */
    PostingsEnum getWrapped() {
      return in;
    }
    
    // we buffer up docs/freqs only, don't forward any positions requests to underlying enum

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
  }

  static class SortingPostingsEnum extends FilterPostingsEnum {

    /**
     * A {@link TimSorter} which sorts two parallel arrays of doc IDs and
     * offsets in one go. Everytime a doc ID is 'swapped', its corresponding offset
     * is swapped too.
     */
    private static final class DocOffsetSorter extends TimSorter {

      private int[] docs;
      private long[] offsets;
      private final int[] tmpDocs;
      private final long[] tmpOffsets;

      public DocOffsetSorter(int maxDoc) {
        super(maxDoc / 64);
        this.tmpDocs = new int[maxDoc / 64];
        this.tmpOffsets = new long[maxDoc / 64];
      }

      public void reset(int[] docs, long[] offsets) {
        this.docs = docs;
        this.offsets = offsets;
      }

      @Override
      protected int compare(int i, int j) {
        return docs[i] - docs[j];
      }

      @Override
      protected void swap(int i, int j) {
        int tmpDoc = docs[i];
        docs[i] = docs[j];
        docs[j] = tmpDoc;

        long tmpOffset = offsets[i];
        offsets[i] = offsets[j];
        offsets[j] = tmpOffset;
      }

      @Override
      protected void copy(int src, int dest) {
        docs[dest] = docs[src];
        offsets[dest] = offsets[src];
      }

      @Override
      protected void save(int i, int len) {
        System.arraycopy(docs, i, tmpDocs, 0, len);
        System.arraycopy(offsets, i, tmpOffsets, 0, len);
      }

      @Override
      protected void restore(int i, int j) {
        docs[j] = tmpDocs[i];
        offsets[j] = tmpOffsets[i];
      }

      @Override
      protected int compareSaved(int i, int j) {
        return tmpDocs[i] - docs[j];
      }
    }

    private final int maxDoc;
    private final DocOffsetSorter sorter;
    private int[] docs;
    private long[] offsets;
    private final int upto;

    private final IndexInput postingInput;
    private final boolean storeOffsets;

    private int docIt = -1;
    private int pos;
    private int startOffset = -1;
    private int endOffset = -1;
    private final BytesRef payload;
    private int currFreq;

    private final RAMFile file;

    SortingPostingsEnum(int maxDoc, SortingPostingsEnum reuse, final PostingsEnum in, Sorter.DocMap docMap, boolean storeOffsets) throws IOException {
      super(in);
      this.maxDoc = maxDoc;
      this.storeOffsets = storeOffsets;
      if (reuse != null) {
        docs = reuse.docs;
        offsets = reuse.offsets;
        payload = reuse.payload;
        file = reuse.file;
        if (reuse.maxDoc == maxDoc) {
          sorter = reuse.sorter;
        } else {
          sorter = new DocOffsetSorter(maxDoc);
        }
      } else {
        docs = new int[32];
        offsets = new long[32];
        payload = new BytesRef(32);
        file = new RAMFile();
        sorter = new DocOffsetSorter(maxDoc);
      }
      final IndexOutput out = new RAMOutputStream(file, false);
      int doc;
      int i = 0;
      while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (i == docs.length) {
          final int newLength = ArrayUtil.oversize(i + 1, 4);
          docs = Arrays.copyOf(docs, newLength);
          offsets = Arrays.copyOf(offsets, newLength);
        }
        docs[i] = docMap.oldToNew(doc);
        offsets[i] = out.getFilePointer();
        addPositions(in, out);
        i++;
      }
      upto = i;
      sorter.reset(docs, offsets);
      sorter.sort(0, upto);
      out.close();
      this.postingInput = new RAMInputStream("", file);
    }

    // for testing
    boolean reused(PostingsEnum other) {
      if (other == null || !(other instanceof SortingPostingsEnum)) {
        return false;
      }
      return docs == ((SortingPostingsEnum) other).docs;
    }

    private void addPositions(final PostingsEnum in, final IndexOutput out) throws IOException {
      int freq = in.freq();
      out.writeVInt(freq);
      int previousPosition = 0;
      int previousEndOffset = 0;
      for (int i = 0; i < freq; i++) {
        final int pos = in.nextPosition();
        final BytesRef payload = in.getPayload();
        // The low-order bit of token is set only if there is a payload, the
        // previous bits are the delta-encoded position.
        final int token = (pos - previousPosition) << 1 | (payload == null ? 0 : 1);
        out.writeVInt(token);
        previousPosition = pos;
        if (storeOffsets) { // don't encode offsets if they are not stored
          final int startOffset = in.startOffset();
          final int endOffset = in.endOffset();
          out.writeVInt(startOffset - previousEndOffset);
          out.writeVInt(endOffset - startOffset);
          previousEndOffset = endOffset;
        }
        if (payload != null) {
          out.writeVInt(payload.length);
          out.writeBytes(payload.bytes, payload.offset, payload.length);
        }
      }
    }

    @Override
    public int advance(final int target) throws IOException {
      // need to support it for checkIndex, but in practice it won't be called, so
      // don't bother to implement efficiently for now.
      return slowAdvance(target);
    }

    @Override
    public int docID() {
      return docIt < 0 ? -1 : docIt >= upto ? NO_MORE_DOCS : docs[docIt];
    }

    @Override
    public int endOffset() throws IOException {
      return endOffset;
    }

    @Override
    public int freq() throws IOException {
      return currFreq;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return payload.length == 0 ? null : payload;
    }

    @Override
    public int nextDoc() throws IOException {
      if (++docIt >= upto) return DocIdSetIterator.NO_MORE_DOCS;
      postingInput.seek(offsets[docIt]);
      currFreq = postingInput.readVInt();
      // reset variables used in nextPosition
      pos = 0;
      endOffset = 0;
      return docs[docIt];
    }

    @Override
    public int nextPosition() throws IOException {
      final int token = postingInput.readVInt();
      pos += token >>> 1;
      if (storeOffsets) {
        startOffset = endOffset + postingInput.readVInt();
        endOffset = startOffset + postingInput.readVInt();
      }
      if ((token & 1) != 0) {
        payload.offset = 0;
        payload.length = postingInput.readVInt();
        if (payload.length > payload.bytes.length) {
          payload.bytes = new byte[ArrayUtil.oversize(payload.length, 1)];
        }
        postingInput.readBytes(payload.bytes, 0, payload.length);
      } else {
        payload.length = 0;
      }
      return pos;
    }

    @Override
    public int startOffset() throws IOException {
      return startOffset;
    }

    /** Returns the wrapped {@link PostingsEnum}. */
    PostingsEnum getWrapped() {
      return in;
    }
  }

  /** Return a sorted view of <code>reader</code> according to the order
   *  defined by <code>sort</code>. If the reader is already sorted, this
   *  method might return the reader as-is. */
  public static LeafReader wrap(LeafReader reader, Sort sort) throws IOException {
    return wrap(reader, new Sorter(sort).sort(reader));
  }

  /** Expert: same as {@link #wrap(org.apache.lucene.index.LeafReader, Sort)} but operates directly on a {@link Sorter.DocMap}. */
  static LeafReader wrap(LeafReader reader, Sorter.DocMap docMap) {
    if (docMap == null) {
      // the reader is already sorted
      return reader;
    }
    if (reader.maxDoc() != docMap.size()) {
      throw new IllegalArgumentException("reader.maxDoc() should be equal to docMap.size(), got" + reader.maxDoc() + " != " + docMap.size());
    }
    assert Sorter.isConsistent(docMap);
    return new SortingLeafReader(reader, docMap);
  }

  final Sorter.DocMap docMap; // pkg-protected to avoid synthetic accessor methods

  private SortingLeafReader(final LeafReader in, final Sorter.DocMap docMap) {
    super(in);
    this.docMap = docMap;
  }

  @Override
  public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
    in.document(docMap.newToOld(docID), visitor);
  }

  @Override
  public Fields fields() throws IOException {
    return new SortingFields(in.fields(), in.getFieldInfos(), docMap);
  }

  @Override
  public BinaryDocValues getBinaryDocValues(String field) throws IOException {
    BinaryDocValues oldDocValues = in.getBinaryDocValues(field);
    if (oldDocValues == null) {
      return null;
    } else {
      return new SortingBinaryDocValues(oldDocValues, docMap);
    }
  }

  @Override
  public Bits getLiveDocs() {
    final Bits inLiveDocs = in.getLiveDocs();
    if (inLiveDocs == null) {
      return null;
    } else {
      return new SortingBits(inLiveDocs, docMap);
    }
  }

  @Override
  public NumericDocValues getNormValues(String field) throws IOException {
    final NumericDocValues norm = in.getNormValues(field);
    if (norm == null) {
      return null;
    } else {
      return new SortingNumericDocValues(norm, docMap);
    }
  }

  @Override
  public NumericDocValues getNumericDocValues(String field) throws IOException {
    final NumericDocValues oldDocValues = in.getNumericDocValues(field);
    if (oldDocValues == null) return null;
    return new SortingNumericDocValues(oldDocValues, docMap);
  }

  @Override
  public SortedNumericDocValues getSortedNumericDocValues(String field)
      throws IOException {
    final SortedNumericDocValues oldDocValues = in.getSortedNumericDocValues(field);
    if (oldDocValues == null) {
      return null;
    } else {
      return new SortingSortedNumericDocValues(oldDocValues, docMap);
    }
  }

  @Override
  public SortedDocValues getSortedDocValues(String field) throws IOException {
    SortedDocValues sortedDV = in.getSortedDocValues(field);
    if (sortedDV == null) {
      return null;
    } else {
      return new SortingSortedDocValues(sortedDV, docMap);
    }
  }

  @Override
  public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
    SortedSetDocValues sortedSetDV = in.getSortedSetDocValues(field);
    if (sortedSetDV == null) {
      return null;
    } else {
      return new SortingSortedSetDocValues(sortedSetDV, docMap);
    }
  }

  @Override
  public Bits getDocsWithField(String field) throws IOException {
    Bits bits = in.getDocsWithField(field);
    if (bits == null || bits instanceof Bits.MatchAllBits || bits instanceof Bits.MatchNoBits) {
      return bits;
    } else {
      return new SortingBits(bits, docMap);
    }
  }

  @Override
  public Fields getTermVectors(final int docID) throws IOException {
    return in.getTermVectors(docMap.newToOld(docID));
  }

  @Override
  public String toString() {
    return "SortingLeafReader(" + in + ")";
  }
}
