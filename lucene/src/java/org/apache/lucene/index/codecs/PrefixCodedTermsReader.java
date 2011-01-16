package org.apache.lucene.index.codecs;

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
import java.io.Closeable;
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
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DoubleBarrelLRUCache;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;

import org.apache.lucene.index.codecs.standard.StandardPostingsReader; // javadocs

/** Handles a terms dict, but decouples all details of
 *  doc/freqs/positions reading to an instance of {@link
 *  StandardPostingsReader}.  This class is reusable for
 *  codecs that use a different format for
 *  docs/freqs/positions (though codecs are also free to
 *  make their own terms dict impl).
 *
 * <p>This class also interacts with an instance of {@link
 * TermsIndexReaderBase}, to abstract away the specific
 * implementation of the terms dict index. 
 * @lucene.experimental */

public class PrefixCodedTermsReader extends FieldsProducer {
  // Open input to the main terms dict file (_X.tis)
  private final IndexInput in;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  private final PostingsReaderBase postingsReader;

  private final TreeMap<String,FieldReader> fields = new TreeMap<String,FieldReader>();

  // Comparator that orders our terms
  private final Comparator<BytesRef> termComp;

  // Caches the most recently looked-up field + terms:
  private final DoubleBarrelLRUCache<FieldAndTerm,PrefixCodedTermState> termsCache;

  // Reads the terms index
  private TermsIndexReaderBase indexReader;
  
  // keeps the dirStart offset
  protected long dirOffset;

  // Used as key for the terms cache
  private static class FieldAndTerm extends DoubleBarrelLRUCache.CloneableKey {
    String field;
    BytesRef term;

    public FieldAndTerm() {
    }

    public FieldAndTerm(FieldAndTerm other) {
      field = other.field;
      term = new BytesRef(other.term);
    }

    @Override
    public boolean equals(Object _other) {
      FieldAndTerm other = (FieldAndTerm) _other;
      return other.field == field && term.bytesEquals(other.term);
    }

    @Override
    public Object clone() {
      return new FieldAndTerm(this);
    }

    @Override
    public int hashCode() {
      return field.hashCode() * 31 + term.hashCode();
    }
  }
  
  public PrefixCodedTermsReader(TermsIndexReaderBase indexReader, Directory dir, FieldInfos fieldInfos, String segment, PostingsReaderBase postingsReader, int readBufferSize,
                                 Comparator<BytesRef> termComp, int termsCacheSize, String codecId)
    throws IOException {
    
    this.postingsReader = postingsReader;
    termsCache = new DoubleBarrelLRUCache<FieldAndTerm,PrefixCodedTermState>(termsCacheSize);

    this.termComp = termComp;
    
    in = dir.openInput(IndexFileNames.segmentFileName(segment, codecId, PrefixCodedTermsWriter.TERMS_EXTENSION),
                       readBufferSize);

    boolean success = false;
    try {
      readHeader(in);

      // Have PostingsReader init itself
      postingsReader.init(in);

      // Read per-field details
      seekDir(in, dirOffset);

      final int numFields = in.readVInt();

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long numTerms = in.readVLong();
        assert numTerms >= 0;
        final long termsStartPointer = in.readVLong();
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        final long sumTotalTermFreq = fieldInfo.omitTermFreqAndPositions ? -1 : in.readVLong();
        assert !fields.containsKey(fieldInfo.name);
        fields.put(fieldInfo.name, new FieldReader(fieldInfo, numTerms, termsStartPointer, sumTotalTermFreq));
      }
      success = true;
    } finally {
      if (!success) {
        in.close();
      }
    }

    this.indexReader = indexReader;
  }

  protected void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(in, PrefixCodedTermsWriter.CODEC_NAME,
      PrefixCodedTermsWriter.VERSION_START, PrefixCodedTermsWriter.VERSION_CURRENT);
    dirOffset = in.readLong();    
  }
  
  protected void seekDir(IndexInput input, long dirOffset)
      throws IOException {
    input.seek(dirOffset);
  }
  
  @Override
  public void loadTermsIndex(int indexDivisor) throws IOException {
    indexReader.loadTermsIndex(indexDivisor);
  }

  @Override
  public void close() throws IOException {
    try {
      try {
        if (indexReader != null) {
          indexReader.close();
        }
      } finally {
        // null so if an app hangs on to us (ie, we are not
        // GCable, despite being closed) we still free most
        // ram
        indexReader = null;
        if (in != null) {
          in.close();
        }
      }
    } finally {
      try {
        if (postingsReader != null) {
          postingsReader.close();
        }
      } finally {
        for(FieldReader field : fields.values()) {
          field.close();
        }
      }
    }
  }

  public static void files(Directory dir, SegmentInfo segmentInfo, String id, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(segmentInfo.name, id, PrefixCodedTermsWriter.TERMS_EXTENSION));
  }

  public static void getExtensions(Collection<String> extensions) {
    extensions.add(PrefixCodedTermsWriter.TERMS_EXTENSION);
  }

  @Override
  public FieldsEnum iterator() {
    return new TermFieldsEnum();
  }

  @Override
  public Terms terms(String field) throws IOException {
    return fields.get(field);
  }

  // Iterates through all fields
  private class TermFieldsEnum extends FieldsEnum {
    final Iterator<FieldReader> it;
    FieldReader current;

    TermFieldsEnum() {
      it = fields.values().iterator();
    }

    @Override
    public String next() {
      if (it.hasNext()) {
        current = it.next();
        return current.fieldInfo.name;
      } else {
        current = null;
        return null;
      }
    }
    
    @Override
    public TermsEnum terms() throws IOException {
      return current.iterator();
    }
  }

  private class FieldReader extends Terms implements Closeable {
    final long numTerms;
    final FieldInfo fieldInfo;
    final long termsStartPointer;
    final long sumTotalTermFreq;

    FieldReader(FieldInfo fieldInfo, long numTerms, long termsStartPointer, long sumTotalTermFreq) {
      assert numTerms > 0;
      this.fieldInfo = fieldInfo;
      this.numTerms = numTerms;
      this.termsStartPointer = termsStartPointer;
      this.sumTotalTermFreq = sumTotalTermFreq;
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return termComp;
    }

    @Override
    public void close() {
      super.close();
    }
    
    @Override
    public TermsEnum iterator() throws IOException {
      return new SegmentTermsEnum();
    }

    @Override
    public long getUniqueTermCount() {
      return numTerms;
    }

    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }

    // Iterates through terms in this field, not supporting ord()
    private final class SegmentTermsEnum extends TermsEnum {
      private final IndexInput in;
      private final DeltaBytesReader bytesReader;
      private final PrefixCodedTermState state;
      private boolean seekPending;
      private final FieldAndTerm fieldTerm = new FieldAndTerm();
      private final TermsIndexReaderBase.FieldIndexEnum indexEnum;
      private boolean positioned;
      private boolean didIndexNext;
      private BytesRef nextIndexTerm;
      private boolean isIndexTerm;
      private final boolean doOrd;

      SegmentTermsEnum() throws IOException {
        in = (IndexInput) PrefixCodedTermsReader.this.in.clone();
        in.seek(termsStartPointer);
        indexEnum = indexReader.getFieldEnum(fieldInfo);
        doOrd = indexReader.supportsOrd();
        bytesReader = new DeltaBytesReader(in);
        fieldTerm.field = fieldInfo.name;
        state = postingsReader.newTermState();
        state.totalTermFreq = -1;
        state.ord = -1;
      }

      @Override
      public Comparator<BytesRef> getComparator() {
        return termComp;
      }

      // called only from assert
      private boolean first;
      private int indexTermCount;

      private boolean startSeek() {
        first = true;
        indexTermCount = 0;
        return true;
      }

      private boolean checkSeekScan() {
        if (!first && isIndexTerm) {
          indexTermCount++;
          if (indexTermCount >= indexReader.getDivisor()) {
            //System.out.println("now fail count=" + indexTermCount);
            return false;
          }
        }
        first = false;
        return true;
      }

      /** Seeks until the first term that's >= the provided
       *  text; returns SeekStatus.FOUND if the exact term
       *  is found, SeekStatus.NOT_FOUND if a different term
       *  was found, SeekStatus.END if we hit EOF */
      @Override
      public SeekStatus seek(final BytesRef term, final boolean useCache) throws IOException {

        if (indexEnum == null) {
          throw new IllegalStateException("terms index was not loaded");
        }
        
        //System.out.println("te.seek term=" + fieldInfo.name + ":" + term.utf8ToString() + " current=" + term().utf8ToString() + " useCache=" + useCache + " this="  + this);

        // Check cache
        fieldTerm.term = term;
        TermState cachedState;
        if (useCache) {
          cachedState = termsCache.get(fieldTerm);
          if (cachedState != null) {
            state.copyFrom(cachedState);
            setTermState(term, state);
            positioned = false;
            //System.out.println("  cached!");
            return SeekStatus.FOUND;
          }
        } else {
          cachedState = null;
        }

        boolean doSeek = true;

        if (positioned) {

          final int cmp = termComp.compare(bytesReader.term, term);

          if (cmp == 0) {
            // already at the requested term
            return SeekStatus.FOUND;
          } else if (cmp < 0) {

            if (seekPending) {
              seekPending = false;
              in.seek(state.filePointer);
              indexEnum.seek(bytesReader.term);
              didIndexNext = false;
            }

            // Target term is after current term
            if (!didIndexNext) {
              if (indexEnum.next() == -1) {
                nextIndexTerm = null;
              } else {
                nextIndexTerm = indexEnum.term();
              }
              //System.out.println("  now do index next() nextIndexTerm=" + (nextIndexTerm == null ? "null" : nextIndexTerm.utf8ToString()));
              didIndexNext = true;
            }

            if (nextIndexTerm == null || termComp.compare(term, nextIndexTerm) < 0) {
              // Optimization: requested term is within the
              // same index block we are now in; skip seeking
              // (but do scanning):
              doSeek = false;
              //System.out.println("  skip seek: nextIndexTerm=" + nextIndexTerm);
            }
          }
        }

        if (doSeek) {

          positioned = true;

          // Ask terms index to find biggest index term that's <=
          // our text:
          in.seek(indexEnum.seek(term));
          didIndexNext = false;
          if (doOrd) {
            state.ord = indexEnum.ord()-1;
          }
          seekPending = false;

          // NOTE: the first next() after an index seek is
          // wasteful, since it redundantly reads the same
          // bytes into the buffer.  We could avoid storing
          // those bytes in the primary file, but then when
          // scanning over an index term we'd have to
          // special case it:
          bytesReader.reset(indexEnum.term());
          //System.out.println("  doSeek term=" + indexEnum.term().utf8ToString() + " vs target=" + term.utf8ToString());
        } else {
          //System.out.println("  skip seek");
        }

        assert startSeek();

        // Now scan:
        while (next() != null) {
          final int cmp = termComp.compare(bytesReader.term, term);
          if (cmp == 0) {
            // Done!
            if (useCache) {
              cacheTerm(fieldTerm);
            }

            return SeekStatus.FOUND;
          } else if (cmp > 0) {
            return SeekStatus.NOT_FOUND;
          }

          // The purpose of the terms dict index is to seek
          // the enum to the closest index term before the
          // term we are looking for.  So, we should never
          // cross another index term (besides the first
          // one) while we are scanning:
          assert checkSeekScan();
        }

        positioned = false;
        return SeekStatus.END;
      }

      private final void setTermState(BytesRef term, final TermState termState) {
        assert termState != null && termState instanceof PrefixCodedTermState;
        state.copyFrom(termState);
        seekPending = true;
        bytesReader.term.copy(term);
      }

      private final void cacheTerm(FieldAndTerm other) {
        // Store in cache
        final FieldAndTerm entryKey = new FieldAndTerm(other);
        final PrefixCodedTermState cachedState = (PrefixCodedTermState) state.clone();
        // this is fp after current term
        cachedState.filePointer = in.getFilePointer();
        termsCache.put(entryKey, cachedState);
      }
      

      @Override
      public BytesRef term() {
        return bytesReader.term;
      }

      @Override
      public BytesRef next() throws IOException {

        if (seekPending) {
          seekPending = false;
          in.seek(state.filePointer);
          indexEnum.seek(bytesReader.term);
          didIndexNext = false;
        }
        
        if (!bytesReader.read()) {
          //System.out.println("te.next end!");
          positioned = false;
          return null;
        }

        final byte b = in.readByte();
        isIndexTerm = (b & 0x80) != 0;

        if ((b & 0x40) == 0) {
          // Fast case -- docFreq fits in 6 bits
          state.docFreq = b & 0x3F;
        } else {
          state.docFreq = (in.readVInt() << 6) | (b & 0x3F);
        }

        if (!fieldInfo.omitTermFreqAndPositions) {
          state.totalTermFreq = state.docFreq + in.readVLong();
        }

        postingsReader.readTerm(in,
                                fieldInfo, state,
                                isIndexTerm);
        if (doOrd) {
          state.ord++;
        }
        positioned = true;

        //System.out.println("te.next term=" + bytesReader.term.utf8ToString());
        return bytesReader.term;
      }

      @Override
      public int docFreq() {
        return state.docFreq;
      }

      @Override
      public long totalTermFreq() {
        return state.totalTermFreq;
      }

      @Override
      public DocsEnum docs(Bits skipDocs, DocsEnum reuse) throws IOException {
        final DocsEnum docsEnum = postingsReader.docs(fieldInfo, state, skipDocs, reuse);
        assert docsEnum != null;
        return docsEnum;
      }

      @Override
      public DocsAndPositionsEnum docsAndPositions(Bits skipDocs, DocsAndPositionsEnum reuse) throws IOException {
        if (fieldInfo.omitTermFreqAndPositions) {
          return null;
        } else {
          return postingsReader.docsAndPositions(fieldInfo, state, skipDocs, reuse);
        }
      }

      @Override
      public SeekStatus seek(BytesRef term, TermState otherState) throws IOException {
        assert otherState != null && otherState instanceof PrefixCodedTermState;
        assert otherState.getClass() == this.state.getClass() : "Illegal TermState type " + otherState.getClass();
        assert ((PrefixCodedTermState)otherState).ord < numTerms;
        setTermState(term, otherState);
        positioned = false;
        return SeekStatus.FOUND;
      }
      
      @Override
      public TermState termState() throws IOException {
        final PrefixCodedTermState newTermState = (PrefixCodedTermState) state.clone();
        newTermState.filePointer = in.getFilePointer();
        return newTermState;
      }

      @Override
      public SeekStatus seek(long ord) throws IOException {

        if (indexEnum == null) {
          throw new IllegalStateException("terms index was not loaded");
        }

        if (ord >= numTerms) {
          state.ord = numTerms-1;
          return SeekStatus.END;
        }

        in.seek(indexEnum.seek(ord));
        seekPending = false;
        positioned = true;

        // NOTE: the first next() after an index seek is
        // wasteful, since it redundantly reads the same
        // bytes into the buffer
        bytesReader.reset(indexEnum.term());

        state.ord = indexEnum.ord()-1;
        assert state.ord >= -1: "ord=" + state.ord;

        // Now, scan:
        int left = (int) (ord - state.ord);
        while(left > 0) {
          final BytesRef term = next();
          assert term != null;
          left--;
        }

        // always found
        return SeekStatus.FOUND;
      }

      public long ord() {
        if (!doOrd) {
          throw new UnsupportedOperationException();
        }
        return state.ord;
      }
    }
  }
}
