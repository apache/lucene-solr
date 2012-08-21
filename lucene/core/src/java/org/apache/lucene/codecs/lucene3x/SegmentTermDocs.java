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

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;

/** @deprecated (4.0)
 *  @lucene.experimental */
@Deprecated
class SegmentTermDocs {
  //protected SegmentReader parent;
  private final FieldInfos fieldInfos;
  private final TermInfosReader tis;
  protected Bits liveDocs;
  protected IndexInput freqStream;
  protected int count;
  protected int df;
  int doc = 0;
  int freq;

  private int skipInterval;
  private int maxSkipLevels;
  private Lucene3xSkipListReader skipListReader;
  
  private long freqBasePointer;
  private long proxBasePointer;

  private long skipPointer;
  private boolean haveSkipped;
  
  protected boolean currentFieldStoresPayloads;
  protected IndexOptions indexOptions;
  
  public SegmentTermDocs(IndexInput freqStream, TermInfosReader tis, FieldInfos fieldInfos) {
    this.freqStream = freqStream.clone();
    this.tis = tis;
    this.fieldInfos = fieldInfos;
    skipInterval = tis.getSkipInterval();
    maxSkipLevels = tis.getMaxSkipLevels();
  }

  public void seek(Term term) throws IOException {
    TermInfo ti = tis.get(term);
    seek(ti, term);
  }

  public void setLiveDocs(Bits liveDocs) {
    this.liveDocs = liveDocs;
  }

  public void seek(SegmentTermEnum segmentTermEnum) throws IOException {
    TermInfo ti;
    Term term;
    
    // use comparison of fieldinfos to verify that termEnum belongs to the same segment as this SegmentTermDocs
    if (segmentTermEnum.fieldInfos == fieldInfos) {        // optimized case
      term = segmentTermEnum.term();
      ti = segmentTermEnum.termInfo();
    } else  {                                         // punt case
      term = segmentTermEnum.term();
      ti = tis.get(term); 
    }
    
    seek(ti, term);
  }

  void seek(TermInfo ti, Term term) throws IOException {
    count = 0;
    FieldInfo fi = fieldInfos.fieldInfo(term.field());
    this.indexOptions = (fi != null) ? fi.getIndexOptions() : IndexOptions.DOCS_AND_FREQS_AND_POSITIONS;
    currentFieldStoresPayloads = (fi != null) ? fi.hasPayloads() : false;
    if (ti == null) {
      df = 0;
    } else {
      df = ti.docFreq;
      doc = 0;
      freqBasePointer = ti.freqPointer;
      proxBasePointer = ti.proxPointer;
      skipPointer = freqBasePointer + ti.skipOffset;
      freqStream.seek(freqBasePointer);
      haveSkipped = false;
    }
  }

  public void close() throws IOException {
    freqStream.close();
    if (skipListReader != null)
      skipListReader.close();
  }

  public final int doc() { return doc; }
  public final int freq() {
    return freq;
  }

  protected void skippingDoc() throws IOException {
  }

  public boolean next() throws IOException {
    while (true) {
      if (count == df)
        return false;
      final int docCode = freqStream.readVInt();
      
      if (indexOptions == IndexOptions.DOCS_ONLY) {
        doc += docCode;
      } else {
        doc += docCode >>> 1;       // shift off low bit
        if ((docCode & 1) != 0)       // if low bit is set
          freq = 1;         // freq is one
        else {
          freq = freqStream.readVInt();     // else read freq
          assert freq != 1;
        }
      }
      
      count++;

      if (liveDocs == null || liveDocs.get(doc)) {
        break;
      }
      skippingDoc();
    }
    return true;
  }

  /** Optimized implementation. */
  public int read(final int[] docs, final int[] freqs)
          throws IOException {
    final int length = docs.length;
    if (indexOptions == IndexOptions.DOCS_ONLY) {
      return readNoTf(docs, freqs, length);
    } else {
      int i = 0;
      while (i < length && count < df) {
        // manually inlined call to next() for speed
        final int docCode = freqStream.readVInt();
        doc += docCode >>> 1;       // shift off low bit
        if ((docCode & 1) != 0)       // if low bit is set
          freq = 1;         // freq is one
        else
          freq = freqStream.readVInt();     // else read freq
        count++;

        if (liveDocs == null || liveDocs.get(doc)) {
          docs[i] = doc;
          freqs[i] = freq;
          ++i;
        }
      }
      return i;
    }
  }

  private final int readNoTf(final int[] docs, final int[] freqs, final int length) throws IOException {
    int i = 0;
    while (i < length && count < df) {
      // manually inlined call to next() for speed
      doc += freqStream.readVInt();       
      count++;

      if (liveDocs == null || liveDocs.get(doc)) {
        docs[i] = doc;
        // Hardware freq to 1 when term freqs were not
        // stored in the index
        freqs[i] = 1;
        ++i;
      }
    }
    return i;
  }
 
  
  /** Overridden by SegmentTermPositions to skip in prox stream. */
  protected void skipProx(long proxPointer, int payloadLength) throws IOException {}

  /** Optimized implementation. */
  public boolean skipTo(int target) throws IOException {
    // don't skip if the target is close (within skipInterval docs away)
    if ((target - skipInterval) >= doc && df >= skipInterval) {                      // optimized case
      if (skipListReader == null)
        skipListReader = new Lucene3xSkipListReader(freqStream.clone(), maxSkipLevels, skipInterval); // lazily clone

      if (!haveSkipped) {                          // lazily initialize skip stream
        skipListReader.init(skipPointer, freqBasePointer, proxBasePointer, df, currentFieldStoresPayloads);
        haveSkipped = true;
      }

      int newCount = skipListReader.skipTo(target); 
      if (newCount > count) {
        freqStream.seek(skipListReader.getFreqPointer());
        skipProx(skipListReader.getProxPointer(), skipListReader.getPayloadLength());

        doc = skipListReader.getDoc();
        count = newCount;
      }      
    }

    // done skipping, now just scan
    do {
      if (!next())
        return false;
    } while (target > doc);
    return true;
  }
}
