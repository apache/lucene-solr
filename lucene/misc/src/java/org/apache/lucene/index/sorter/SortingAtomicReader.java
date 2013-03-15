package org.apache.lucene.index.sorter;

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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMFile;
import org.apache.lucene.store.RAMInputStream;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SorterTemplate;

/**
 * An {@link AtomicReader} which supports sorting documents by a given
 * {@link Sorter}. You can use this class to sort an index as follows:
 * 
 * <pre class="prettyprint">
 * IndexWriter writer; // writer to which the sorted index will be added
 * DirectoryReader reader; // reader on the input index
 * Sorter sorter; // determines how the documents are sorted
 * AtomicReader sortingReader = new SortingAtomicReader(reader, sorter);
 * writer.addIndexes(reader);
 * writer.close();
 * reader.close();
 * </pre>
 * 
 * @lucene.experimental
 */
public class SortingAtomicReader extends FilterAtomicReader {

  private static class SortingFields extends FilterFields {

    private final Sorter.DocMap docMap;
    private final Bits inLiveDocs;
    private final FieldInfos infos;

    public SortingFields(final Fields in, final Bits inLiveDocs, FieldInfos infos, Sorter.DocMap docMap) {
      super(in);
      this.docMap = docMap;
      this.inLiveDocs = inLiveDocs;
      this.infos = infos;
    }

    @Override
    public Terms terms(final String field) throws IOException {
      Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      } else {
        return new SortingTerms(terms, inLiveDocs, infos.fieldInfo(field).getIndexOptions(), docMap);
      }
    }

  }

  private static class SortingTerms extends FilterTerms {

    private final Sorter.DocMap docMap;
    private final Bits inLiveDocs;
    private final IndexOptions indexOptions;
    
    public SortingTerms(final Terms in, final Bits inLiveDocs, IndexOptions indexOptions, final Sorter.DocMap docMap) {
      super(in);
      this.docMap = docMap;
      this.inLiveDocs = inLiveDocs;
      this.indexOptions = indexOptions;
    }

    @Override
    public TermsEnum iterator(final TermsEnum reuse) throws IOException {
      return new SortingTermsEnum(in.iterator(reuse), inLiveDocs, docMap, indexOptions);
    }

  }

  private static class SortingTermsEnum extends FilterTermsEnum {

    private final Sorter.DocMap docMap;
    private final Bits inLiveDocs;
    private final IndexOptions indexOptions;
    
    public SortingTermsEnum(final TermsEnum in, final Bits inLiveDocs, Sorter.DocMap docMap, IndexOptions indexOptions) {
      super(in);
      this.docMap = docMap;
      this.inLiveDocs = inLiveDocs;
      this.indexOptions = indexOptions;
    }

    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, final int flags) throws IOException {
      if (liveDocs != null) {
        liveDocs = inLiveDocs;
      }
      
      // if we're asked to reuse the given DocsEnum and it is Sorting, return
      // the wrapped one, since some Codecs expect it.
      if (reuse != null && reuse instanceof SortingDocsEnum) {
        reuse = ((SortingDocsEnum) reuse).getWrapped();
      }
      boolean withFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >=0 && (flags & DocsEnum.FLAG_FREQS) != 0;
      return new SortingDocsEnum(in.docs(liveDocs, reuse, flags), withFreqs, docMap);
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, final int flags) throws IOException {
      if (liveDocs != null) {
        liveDocs = inLiveDocs;
      }
      
      // if we're asked to reuse the given DocsAndPositionsEnum and it is
      // Sorting, return the wrapped one, since some Codecs expect it.
      if (reuse != null && reuse instanceof SortingDocsAndPositionsEnum) {
        reuse = ((SortingDocsAndPositionsEnum) reuse).getWrapped();
      }
      
      final DocsAndPositionsEnum positions = in.docsAndPositions(liveDocs, reuse, flags);
      if (positions == null) {
        return null;
      } else {
        // we ignore the fact that offsets may be stored but not asked for,
        // since this code is expected to be used during addIndexes which will
        // ask for everything. if that assumption changes in the future, we can
        // factor in whether 'flags' says offsets are not required.
        boolean storeOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        return new SortingDocsAndPositionsEnum(positions, docMap, storeOffsets);
      }
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
    public void get(int docID, BytesRef result) {
      in.get(docMap.newToOld(docID), result);
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
    public void lookupOrd(int ord, BytesRef result) {
      in.lookupOrd(ord, result);
    }

    @Override
    public int getValueCount() {
      return in.getValueCount();
    }

    @Override
    public void get(int docID, BytesRef result) {
      in.get(docMap.newToOld(docID), result);
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
    public void lookupOrd(long ord, BytesRef result) {
      in.lookupOrd(ord, result);
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

  private static class SortingDocsEnum extends FilterDocsEnum {
    
    private static final class DocFreqSorterTemplate extends SorterTemplate {
      
      private final int[] docs;
      private final int[] freqs;
      
      private int pivot;
      
      public DocFreqSorterTemplate(int[] docs, int[] freqs) {
        this.docs = docs;
        this.freqs = freqs;
      }
      
      @Override
      protected int compare(int i, int j) {
        return docs[i] - docs[j];
      }
      
      @Override
      protected int comparePivot(int j) {
        return pivot - docs[j];
      }
      
      @Override
      protected void setPivot(int i) {
        pivot = docs[i];
      }
      
      @Override
      protected void swap(int i, int j) {
        int tmpDoc = docs[i];
        docs[i] = docs[j];
        docs[j] = tmpDoc;
        
        int tmpFreq = freqs[i];
        freqs[i] = freqs[j];
        freqs[j] = tmpFreq;
      }
    }
    
    private int[] docs = new int[64];
    private int[] freqs;
    private int docIt = -1;
    private final int upto;
    private final boolean withFreqs;
    
    public SortingDocsEnum(final DocsEnum in, boolean withFreqs, final Sorter.DocMap docMap) throws IOException {
      super(in);
      this.withFreqs = withFreqs;
      int i = 0;
      int doc;
      if (withFreqs) {
        freqs = new int[docs.length];
        while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS){
          if (i >= docs.length) {
            docs = ArrayUtil.grow(docs, docs.length + 1);
            freqs = ArrayUtil.grow(freqs, freqs.length + 1);
          }
          docs[i] = docMap.oldToNew(doc);
          freqs[i] = in.freq();
          ++i;
        }
        SorterTemplate sorter = new DocFreqSorterTemplate(docs, freqs);
        sorter.quickSort(0, i - 1);
      } else {
        freqs = null;
        while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS){
          if (i >= docs.length) {
            docs = ArrayUtil.grow(docs, docs.length + 1);
          }
          docs[i++] = docMap.oldToNew(doc);
        }
        Arrays.sort(docs, 0, i);
      }
      upto = i;
    }
    
    @Override
    public int advance(final int target) throws IOException {
      // need to support it for checkIndex, but in practice it won't be called, so
      // don't bother to implement efficiently for now.
      while (nextDoc() < target) {}
      return docID();
    }
    
    @Override
    public int docID() {
      return docIt >= upto ? NO_MORE_DOCS : docs[docIt];
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
    
    /** Returns the wrapped {@link DocsEnum}. */
    DocsEnum getWrapped() {
      return in;
    }
  }
  
  private static class SortingDocsAndPositionsEnum extends FilterDocsAndPositionsEnum {
    
    /**
     * A {@link SorterTemplate} which sorts two parallel arrays of doc IDs and
     * offsets in one go. Everytime a doc ID is 'swapped', its correponding offset
     * is swapped too.
     */
    private static final class DocOffsetSorterTemplate extends SorterTemplate {
      
      private final int[] docs;
      private final long[] offsets;
      
      private int pivot;
      
      public DocOffsetSorterTemplate(int[] docs, long[] offsets) {
        this.docs = docs;
        this.offsets = offsets;
      }
      
      @Override
      protected int compare(int i, int j) {
        return docs[i] - docs[j];
      }
      
      @Override
      protected int comparePivot(int j) {
        return pivot - docs[j];
      }
      
      @Override
      protected void setPivot(int i) {
        pivot = docs[i];
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
    }
    
    private int[] docs;
    private long[] offsets;
    private final int upto;
    
    private final IndexInput postingInput;
    private final boolean storeOffsets;
    
    private int docIt = -1;
    private int pos;
    private int startOffset = -1;
    private int endOffset = -1;
    private final BytesRef payload = new BytesRef(32);
    private int currFreq;
    
    public SortingDocsAndPositionsEnum(final DocsAndPositionsEnum in, Sorter.DocMap docMap, boolean storeOffsets) throws IOException {
      super(in);
      this.storeOffsets = storeOffsets;
      final RAMFile file = new RAMFile();
      final IndexOutput out = new RAMOutputStream(file);
      docs = new int[32];
      offsets = new long[32];
      int doc;
      int i = 0;
      while ((doc = in.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        if (i == docs.length) {
          docs = ArrayUtil.grow(docs, docs.length + 1);
          // don't grow() offsets since growing pattern for long and int is not the same.
          // since we want docs and offsets at the same length, just grow it manually.
          long[] tmp = new long[docs.length];
          System.arraycopy(offsets, 0, tmp, 0, offsets.length);
          offsets = tmp;
        }
        docs[i] = docMap.oldToNew(doc);
        offsets[i] = out.getFilePointer();
        addPositions(in, out);
        i++;
      }
      upto = i;
      SorterTemplate sorter = new DocOffsetSorterTemplate(docs, offsets);
      sorter.quickSort(0, upto - 1);
      out.close();
      this.postingInput = new RAMInputStream("", file);
    }
    
    private void addPositions(final DocsAndPositionsEnum in, final IndexOutput out) throws IOException {
      int freq = in.freq();
      out.writeVInt(freq);
      for (int i = 0; i < freq; i++) {
        final int pos = in.nextPosition();
        out.writeVInt(pos);
        if (storeOffsets) { // don't encode offsets if they are not stored
          out.writeVInt(in.startOffset());
          out.writeVInt(in.endOffset());
        }
        BytesRef payload = in.getPayload();
        if (payload != null) {
          out.writeVInt(payload.length);
          out.writeBytes(payload.bytes, payload.offset, payload.length);
        } else {
          out.writeVInt(0);
        }
      }
    }
    
    @Override
    public int advance(final int target) throws IOException {
      // need to support it for checkIndex, but in practice it won't be called, so
      // don't bother to implement efficiently for now.
      while (nextDoc() < target) {}
      return docID();
    }
    
    @Override
    public int docID() {
      return docIt >= upto ? NO_MORE_DOCS : docs[docIt];
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
      return docs[docIt];
    }
    
    @Override
    public int nextPosition() throws IOException {
      pos = postingInput.readVInt();
      if (storeOffsets) {
        startOffset = postingInput.readVInt();
        endOffset = postingInput.readVInt();
      }
      int length = postingInput.readVInt();
      if (length > 0) {
        if (length >= payload.bytes.length) {
          payload.grow(length + 1);
        }
        postingInput.readBytes(payload.bytes, 0, length);
      }
      payload.length = length;
      return pos;
    }
    
    @Override
    public int startOffset() throws IOException {
      return startOffset;
    }

    /** Returns the wrapped {@link DocsAndPositionsEnum}. */
    DocsAndPositionsEnum getWrapped() {
      return in;
    }
  }

  /** Return a sorted view of <code>reader</code> according to the order
   *  defined by <code>sorter</code>. If the reader is already sorted, this
   *  method might return the reader as-is. */
  public static AtomicReader sort(AtomicReader reader, Sorter sorter) throws IOException {
    final Sorter.DocMap docMap = sorter.sort(reader);
    if (docMap == null) {
      // the reader is already sorter
      return reader;
    }
    assert Sorter.isConsistent(docMap, reader.maxDoc());
    return new SortingAtomicReader(reader, docMap);
  }

  private final Sorter.DocMap docMap;

  private SortingAtomicReader(final AtomicReader in, final Sorter.DocMap docMap) {
    super(in);
    this.docMap = docMap;
  }

  @Override
  public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
    in.document(docMap.newToOld(docID), visitor);
  }
  
  @Override
  public Fields fields() throws IOException {
    Fields fields = in.fields();
    if (fields == null) {
      return null;
    } else {
      return new SortingFields(fields, in.getLiveDocs(), in.getFieldInfos(), docMap);
    }
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
    }
    return new Bits() {

      @Override
      public boolean get(int index) {
        return inLiveDocs.get(docMap.newToOld(index));
      }

      @Override
      public int length() {
        return inLiveDocs.length();
      }

    };
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
  public Fields getTermVectors(final int docID) throws IOException {
    return in.getTermVectors(docMap.newToOld(docID));
  }
  
}
