package org.apache.lucene.index.codecs.preflex;

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
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Comparator;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.CompoundFileReader;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/** Exposes flex API on a pre-flex index, as a codec. 
 * @lucene.experimental */
public class PreFlexFields extends FieldsProducer {

  public TermInfosReader tis;
  public final TermInfosReader tisNoIndex;

  public final IndexInput freqStream;
  public final IndexInput proxStream;
  final private FieldInfos fieldInfos;
  private final SegmentInfo si;
  final TreeMap<String,FieldInfo> fields = new TreeMap<String,FieldInfo>();
  private final Directory dir;
  private final int readBufferSize;
  private Directory cfsReader;

  PreFlexFields(Directory dir, FieldInfos fieldInfos, SegmentInfo info, int readBufferSize, int indexDivisor)
    throws IOException {

    si = info;
    TermInfosReader r = new TermInfosReader(dir, info.name, fieldInfos, readBufferSize, indexDivisor);    
    if (indexDivisor == -1) {
      tisNoIndex = r;
    } else {
      tisNoIndex = null;
      tis = r;
    }
    this.readBufferSize = readBufferSize;
    this.fieldInfos = fieldInfos;

    // make sure that all index files have been read or are kept open
    // so that if an index update removes them we'll still have them
    freqStream = dir.openInput(info.name + ".frq", readBufferSize);
    boolean anyProx = false;
    final int numFields = fieldInfos.size();
    for(int i=0;i<numFields;i++) {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(i);
      if (fieldInfo.isIndexed) {
        fields.put(fieldInfo.name, fieldInfo);
        if (!fieldInfo.omitTermFreqAndPositions) {
          anyProx = true;
        }
      }
    }

    if (anyProx) {
      proxStream = dir.openInput(info.name + ".prx", readBufferSize);
    } else {
      proxStream = null;
    }

    this.dir = dir;
  }

  static void files(Directory dir, SegmentInfo info, Collection<String> files) throws IOException {
    files.add(IndexFileNames.segmentFileName(info.name, PreFlexCodec.TERMS_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, PreFlexCodec.TERMS_INDEX_EXTENSION));
    files.add(IndexFileNames.segmentFileName(info.name, PreFlexCodec.FREQ_EXTENSION));
    if (info.getHasProx()) {
      // LUCENE-1739: for certain versions of 2.9-dev,
      // hasProx would be incorrectly computed during
      // indexing as true, and then stored into the segments
      // file, when it should have been false.  So we do the
      // extra check, here:
      final String prx = IndexFileNames.segmentFileName(info.name, PreFlexCodec.PROX_EXTENSION);
      if (dir.fileExists(prx)) {
        files.add(prx);
      }
    }
  }

  @Override
  public FieldsEnum iterator() throws IOException {
    return new PreFlexFieldsEnum();
  }

  @Override
  public Terms terms(String field) {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi != null) {
      return new PreTerms(fi);
    } else {
      return null;
    }
  }

  synchronized private TermInfosReader getTermsDict() {
    if (tis != null) {
      return tis;
    } else {
      return tisNoIndex;
    }
  }

  @Override
  synchronized public void loadTermsIndex(int indexDivisor) throws IOException {
    if (tis == null) {
      Directory dir0;
      if (si.getUseCompoundFile()) {
        // In some cases, we were originally opened when CFS
        // was not used, but then we are asked to open the
        // terms reader with index, the segment has switched
        // to CFS

        if (!(dir instanceof CompoundFileReader)) {
          dir0 = cfsReader = new CompoundFileReader(dir, IndexFileNames.segmentFileName(si.name, IndexFileNames.COMPOUND_FILE_EXTENSION), readBufferSize);
        } else {
          dir0 = dir;
        }
        dir0 = cfsReader;
      } else {
        dir0 = dir;
      }

      tis = new TermInfosReader(dir0, si.name, fieldInfos, readBufferSize, indexDivisor);
    }
  }

  @Override
  public void close() throws IOException {
    if (tis != null) {
      tis.close();
    }
    if (tisNoIndex != null) {
      tisNoIndex.close();
    }
    if (cfsReader != null) {
      cfsReader.close();
    }
  }

  private class PreFlexFieldsEnum extends FieldsEnum {
    final Iterator<FieldInfo> it;
    private final PreTermsEnum termsEnum;
    private int count;
    FieldInfo current;

    public PreFlexFieldsEnum() throws IOException {
      it = fields.values().iterator();
      termsEnum = new PreTermsEnum();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        count++;
        current = it.next();
        return current.name;
      } else {
        return null;
      }
    }

    @Override
    public TermsEnum terms() throws IOException {
      termsEnum.reset(current, count == 1);
      return termsEnum;
    }
  }
  
  private class PreTerms extends Terms {
    final FieldInfo fieldInfo;
    PreTerms(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    @Override
    public TermsEnum iterator() throws IOException {    
      PreTermsEnum termsEnum = new PreTermsEnum();
      termsEnum.reset(fieldInfo, false);
      return termsEnum;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order
      return BytesRef.getUTF8SortedAsUTF16Comparator();
    }
  }

  private class PreTermsEnum extends TermsEnum {
    private SegmentTermEnum termEnum;
    private FieldInfo fieldInfo;
    private boolean skipNext;
    private BytesRef current;
    private final BytesRef scratchBytesRef = new BytesRef();

    void reset(FieldInfo fieldInfo, boolean isFirstField) throws IOException {
      this.fieldInfo = fieldInfo;
      if (termEnum == null) {
        // First time reset is called
        if (isFirstField) {
          termEnum = getTermsDict().terms();
          skipNext = false;
        } else {
          termEnum = getTermsDict().terms(new Term(fieldInfo.name, ""));
          skipNext = true;
        }
      } else {
        final Term t = termEnum.term();
        if (t != null && t.field() == fieldInfo.name) {
          // No need to seek -- we have already advanced onto
          // this field.  We must be @ first term because
          // flex API will not advance this enum further, on
          // seeing a different field.
        } else {
          assert t == null || !t.field().equals(fieldInfo.name);  // make sure field name is interned
          final TermInfosReader tis = getTermsDict();
          tis.seekEnum(termEnum, new Term(fieldInfo.name, ""));
        }
        skipNext = true;
      }
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      // Pre-flex indexes always sorted in UTF16 order
      return BytesRef.getUTF8SortedAsUTF16Comparator();
    }

    @Override
    public SeekStatus seek(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long ord() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seek(BytesRef term, boolean useCache) throws IOException {
      skipNext = false;
      final TermInfosReader tis = getTermsDict();
      final Term t0 = new Term(fieldInfo.name, term.utf8ToString());
      if (termEnum == null) {
        termEnum = tis.terms(t0);
      } else {
        tis.seekEnum(termEnum, t0);
      }
      final Term t = termEnum.term();

      final BytesRef tr;
      if (t != null) {
        tr = scratchBytesRef;
        scratchBytesRef.copy(t.text());
      } else {
        tr = null;
      }

      if (t != null && t.field() == fieldInfo.name && term.bytesEquals(tr)) {
        current = tr;
        return SeekStatus.FOUND;
      } else if (t == null || t.field() != fieldInfo.name) {
        current = null;
        return SeekStatus.END;
      } else {
        current = tr;
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public BytesRef next() throws IOException {
      if (skipNext) {
        skipNext = false;
        if (termEnum.term() == null) {
          return null;
        } else {
          scratchBytesRef.copy(termEnum.term().text());
          return current = scratchBytesRef;
        }
      }
      if (termEnum.next()) {
        final Term t = termEnum.term();
        if (t.field() == fieldInfo.name) {
          scratchBytesRef.copy(t.text());
          current = scratchBytesRef;
          return current;
        } else {
          assert !t.field().equals(fieldInfo.name);  // make sure field name is interned
          // Crossed into new field
          return null;
        }
      } else {
        return null;
      }
    }

    @Override
    public BytesRef term() {
      return current;
    }

    @Override
    public int docFreq() {
      return termEnum.docFreq();
    }

    @Override
    public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
      if (reuse != null) {
        return ((PreDocsEnum) reuse).reset(termEnum, skipDocs);        
      } else {
        return (new PreDocsEnum()).reset(termEnum, skipDocs);
      }
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
      if (reuse != null) {
        return ((PreDocsAndPositionsEnum) reuse).reset(termEnum, skipDocs);        
      } else {
        return (new PreDocsAndPositionsEnum()).reset(termEnum, skipDocs);
      }
    }
  }

  private final class PreDocsEnum extends DocsEnum {
    final private SegmentTermDocs docs;

    PreDocsEnum() throws IOException {
      docs = new SegmentTermDocs(freqStream, getTermsDict(), fieldInfos);
    }

    public PreDocsEnum reset(SegmentTermEnum termEnum, Bits skipDocs) throws IOException {
      docs.setSkipDocs(skipDocs);
      docs.seek(termEnum);
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docs.next()) {
        return docs.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (docs.skipTo(target)) {
        return docs.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return docs.freq();
    }

    @Override
    public int docID() {
      return docs.doc();
    }

    @Override
    public int read() throws IOException {
      if (bulkResult == null) {
        initBulkResult();
        bulkResult.docs.ints = new int[32];
        bulkResult.freqs.ints = new int[32];
      }
      return this.docs.read(bulkResult.docs.ints, bulkResult.freqs.ints);
    }
  }

  private final class PreDocsAndPositionsEnum extends DocsAndPositionsEnum {
    final private SegmentTermPositions pos;

    PreDocsAndPositionsEnum() throws IOException {
      pos = new SegmentTermPositions(freqStream, proxStream, getTermsDict(), fieldInfos);
    }

    public DocsAndPositionsEnum reset(SegmentTermEnum termEnum, Bits skipDocs) throws IOException {
      pos.setSkipDocs(skipDocs);
      pos.seek(termEnum);
      return this;
    }

    @Override
    public int nextDoc() throws IOException {
      if (pos.next()) {
        return pos.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int advance(int target) throws IOException {
      if (pos.skipTo(target)) {
        return pos.doc();
      } else {
        return NO_MORE_DOCS;
      }
    }

    @Override
    public int freq() {
      return pos.freq();
    }

    @Override
    public int docID() {
      return pos.doc();
    }

    @Override
    public int nextPosition() throws IOException {
      return pos.nextPosition();
    }

    @Override
    public int getPayloadLength() {
      return pos.getPayloadLength();
    }

    @Override
    public boolean hasPayload() {
      return pos.isPayloadAvailable();
    }

    private BytesRef payload;

    @Override
    public BytesRef getPayload() throws IOException {
      final int len = pos.getPayloadLength();
      if (payload == null) {
        payload = new BytesRef();
        payload.bytes = new byte[len];
      } else {
        if (payload.bytes.length < len) {
          payload.grow(len);
        }
      }
      
      payload.bytes = pos.getPayload(payload.bytes, 0);
      payload.length = len;
      return payload;
    }
  }
}
