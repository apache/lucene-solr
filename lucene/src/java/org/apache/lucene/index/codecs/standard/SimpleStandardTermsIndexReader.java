package org.apache.lucene.index.codecs.standard;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Collection;
import java.util.Comparator;
import java.io.IOException;

/**
 * Uses a simplistic format to record terms dict index
 * information.  Limititations:
 *
 *   - Index for all fields is loaded entirely into RAM up
 *     front 
 *   - Index is stored in RAM using shared byte[] that
 *     wastefully expand every term.  Using FST to share
 *     common prefix & suffix would save RAM.
 *   - Index is taken at regular numTerms (every 128 by
 *     default); might be better to do it by "net docFreqs"
 *     encountered, so that for spans of low-freq terms we
 *     take index less often.
 *
 * A better approach might be something similar to how
 * postings are encoded, w/ multi-level skips.  Ie, load all
 * terms index data into memory, as a single large compactly
 * encoded stream (eg delta bytes + delta offset).  Index
 * that w/ multi-level skipper.  Then to look up a term is
 * the equivalent binary search, using the skipper instead,
 * while data remains compressed in memory.
 */

import org.apache.lucene.index.IndexFileNames;

/** @lucene.experimental */
public class SimpleStandardTermsIndexReader extends StandardTermsIndexReader {

  // NOTE: long is overkill here, since this number is 128
  // by default and only indexDivisor * 128 if you change
  // the indexDivisor at search time.  But, we use this in a
  // number of places to multiply out the actual ord, and we
  // will overflow int during those multiplies.  So to avoid
  // having to upgrade each multiple to long in multiple
  // places (error proned), we use long here:
  private long totalIndexInterval;

  private int indexDivisor;
  final private int indexInterval;

  // Closed if indexLoaded is true:
  final private IndexInput in;
  private volatile boolean indexLoaded;

  private final Comparator<BytesRef> termComp;

  private final static int PAGED_BYTES_BITS = 15;

  // all fields share this single logical byte[]
  private final PagedBytes termBytes = new PagedBytes(PAGED_BYTES_BITS);
  private PagedBytes.Reader termBytesReader;

  final HashMap<FieldInfo,FieldIndexReader> fields = new HashMap<FieldInfo,FieldIndexReader>();
  
  // start of the field info data
  protected long dirOffset;

  public SimpleStandardTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, int indexDivisor, Comparator<BytesRef> termComp)
    throws IOException {

    this.termComp = termComp;

    IndexInput in = dir.openInput(IndexFileNames.segmentFileName(segment, "", StandardCodec.TERMS_INDEX_EXTENSION));
    
    boolean success = false;

    try {
      
      readHeader(in);
      indexInterval = in.readInt();
      this.indexDivisor = indexDivisor;

      if (indexDivisor < 0) {
        totalIndexInterval = indexInterval;
      } else {
        // In case terms index gets loaded, later, on demand
        totalIndexInterval = indexInterval * indexDivisor;
      }
      
      seekDir(in, dirOffset);

      // Read directory
      final int numFields = in.readInt();

      for(int i=0;i<numFields;i++) {
        final int field = in.readInt();
        final int numIndexTerms = in.readInt();
        final long termsStart = in.readLong();
        final long indexStart = in.readLong();
        final long packedIndexStart = in.readLong();
        final long packedOffsetsStart = in.readLong();
        assert packedIndexStart >= indexStart: "packedStart=" + packedIndexStart + " indexStart=" + indexStart + " numIndexTerms=" + numIndexTerms + " seg=" + segment;
        if (numIndexTerms > 0) {
          final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
          fields.put(fieldInfo, new FieldIndexReader(in, fieldInfo, numIndexTerms, indexStart, termsStart, packedIndexStart, packedOffsetsStart));
        }
      }
      success = true;
    } finally {
      if (indexDivisor > 0) {
        in.close();
        this.in = null;
        if (success) {
          indexLoaded = true;
        }
        termBytesReader = termBytes.freeze(true);
      } else {
        this.in = in;
      }
    }
  }
  
  protected void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, SimpleStandardTermsIndexWriter.CODEC_NAME, SimpleStandardTermsIndexWriter.VERSION_START);
    dirOffset = input.readLong();
  }

  private final class FieldIndexReader extends FieldReader {

    final private FieldInfo fieldInfo;

    private volatile CoreFieldIndex coreIndex;

    private final IndexInput in;

    private final long indexStart;
    private final long termsStart;
    private final long packedIndexStart;
    private final long packedOffsetsStart;

    private final int numIndexTerms;

    public FieldIndexReader(IndexInput in, FieldInfo fieldInfo, int numIndexTerms, long indexStart, long termsStart, long packedIndexStart,
                            long packedOffsetsStart) throws IOException {

      this.fieldInfo = fieldInfo;
      this.in = in;
      this.termsStart = termsStart;
      this.indexStart = indexStart;
      this.packedIndexStart = packedIndexStart;
      this.packedOffsetsStart = packedOffsetsStart;
      this.numIndexTerms = numIndexTerms;

      // We still create the indexReader when indexDivisor
      // is -1, so that StandardTermsDictReader can call
      // isIndexTerm for each field:
      if (indexDivisor > 0) {
        coreIndex = new CoreFieldIndex(indexStart,
                                       termsStart,
                                       packedIndexStart,
                                       packedOffsetsStart,
                                       numIndexTerms);
      
      }
    }

    public void loadTermsIndex() throws IOException {
      if (coreIndex == null) {
        coreIndex = new CoreFieldIndex(indexStart, termsStart, packedIndexStart, packedOffsetsStart, numIndexTerms);
      }
    }

    @Override
    public boolean isIndexTerm(long ord, int docFreq, boolean onlyLoaded) {
      if (onlyLoaded) {
        return ord % totalIndexInterval == 0;
      } else {
        return ord % indexInterval == 0;
      }
    }

    @Override
    public boolean nextIndexTerm(long ord, TermsIndexResult result) throws IOException {
      if (coreIndex == null) {
        throw new IllegalStateException("terms index was not loaded");
      } else {
        return coreIndex.nextIndexTerm(ord, result);
      }
    }

    @Override
    public void getIndexOffset(BytesRef term, TermsIndexResult result) throws IOException {
      // You must call loadTermsIndex if you had specified -1 for indexDivisor
      if (coreIndex == null) {
        throw new IllegalStateException("terms index was not loaded");
      }
      coreIndex.getIndexOffset(term, result);
    }

    @Override
    public void getIndexOffset(long ord, TermsIndexResult result) throws IOException {
      // You must call loadTermsIndex if you had specified
      // indexDivisor < 0 to ctor
      if (coreIndex == null) {
        throw new IllegalStateException("terms index was not loaded");
      }
      coreIndex.getIndexOffset(ord, result);
    }

    private final class CoreFieldIndex {

      final private long termBytesStart;

      // offset into index termBytes
      final PackedInts.Reader termOffsets;

      // index pointers into main terms dict
      final PackedInts.Reader termsDictOffsets;

      final int numIndexTerms;

      final long termsStart;

      public CoreFieldIndex(long indexStart, long termsStart, long packedIndexStart, long packedOffsetsStart, int numIndexTerms) throws IOException {

        this.termsStart = termsStart;
        termBytesStart = termBytes.getPointer();

        IndexInput clone = (IndexInput) in.clone();
        clone.seek(indexStart);

        // -1 is passed to mean "don't load term index", but
        // if we are then later loaded it's overwritten with
        // a real value
        assert indexDivisor > 0;

        this.numIndexTerms = 1+(numIndexTerms-1) / indexDivisor;

        assert this.numIndexTerms  > 0: "numIndexTerms=" + numIndexTerms + " indexDivisor=" + indexDivisor;

        if (indexDivisor == 1) {
          // Default (load all index terms) is fast -- slurp in the images from disk:
          
          try {
            final long numTermBytes = packedIndexStart - indexStart;
            termBytes.copy(clone, numTermBytes);

            // records offsets into main terms dict file
            termsDictOffsets = PackedInts.getReader(clone);
            assert termsDictOffsets.size() == numIndexTerms;

            // records offsets into byte[] term data
            termOffsets = PackedInts.getReader(clone);
            assert termOffsets.size() == 1+numIndexTerms;
          } finally {
            clone.close();
          }
        } else {
          // Get packed iterators
          final IndexInput clone1 = (IndexInput) in.clone();
          final IndexInput clone2 = (IndexInput) in.clone();

          try {
            // Subsample the index terms
            clone1.seek(packedIndexStart);
            final PackedInts.ReaderIterator termsDictOffsetsIter = PackedInts.getReaderIterator(clone1);

            clone2.seek(packedOffsetsStart);
            final PackedInts.ReaderIterator termOffsetsIter = PackedInts.getReaderIterator(clone2);

            // TODO: often we can get by w/ fewer bits per
            // value, below.. .but this'd be more complex:
            // we'd have to try @ fewer bits and then grow
            // if we overflowed it.

            PackedInts.Mutable termsDictOffsetsM = PackedInts.getMutable(this.numIndexTerms, termsDictOffsetsIter.getBitsPerValue());
            PackedInts.Mutable termOffsetsM = PackedInts.getMutable(this.numIndexTerms+1, termOffsetsIter.getBitsPerValue());

            termsDictOffsets = termsDictOffsetsM;
            termOffsets = termOffsetsM;

            int upto = 0;

            long termOffsetUpto = 0;

            while(upto < this.numIndexTerms) {
              // main file offset copies straight over
              termsDictOffsetsM.set(upto, termsDictOffsetsIter.next());

              termOffsetsM.set(upto, termOffsetUpto);
              upto++;

              long termOffset = termOffsetsIter.next();
              long nextTermOffset = termOffsetsIter.next();
              final int numTermBytes = (int) (nextTermOffset - termOffset);

              clone.seek(indexStart + termOffset);
              assert indexStart + termOffset < clone.length() : "indexStart=" + indexStart + " termOffset=" + termOffset + " len=" + clone.length();
              assert indexStart + termOffset + numTermBytes < clone.length();

              termBytes.copy(clone, numTermBytes);
              termOffsetUpto += numTermBytes;

              // skip terms:
              termsDictOffsetsIter.next();
              for(int i=0;i<indexDivisor-2;i++) {
                termOffsetsIter.next();
                termsDictOffsetsIter.next();
              }
            }
            termOffsetsM.set(upto, termOffsetUpto);

          } finally {
            clone1.close();
            clone2.close();
            clone.close();
          }
        }
      }

      public boolean nextIndexTerm(long ord, TermsIndexResult result) throws IOException {
        int idx = 1 + (int) (ord / totalIndexInterval);
        if (idx < numIndexTerms) {
          fillResult(idx, result);
          return true;
        } else {
          return false;
        }
      }

      private void fillResult(int idx, TermsIndexResult result) {
        final long offset = termOffsets.get(idx);
        final int length = (int) (termOffsets.get(1+idx) - offset);
        termBytesReader.fill(result.term, termBytesStart + offset, length);
        result.position = idx * totalIndexInterval;
        result.offset = termsStart + termsDictOffsets.get(idx);
      }

      public void getIndexOffset(BytesRef term, TermsIndexResult result) throws IOException {
        int lo = 0;					  // binary search
        int hi = numIndexTerms - 1;

        while (hi >= lo) {
          int mid = (lo + hi) >>> 1;

          final long offset = termOffsets.get(mid);
          final int length = (int) (termOffsets.get(1+mid) - offset);
          termBytesReader.fill(result.term, termBytesStart + offset, length);

          int delta = termComp.compare(term, result.term);
          if (delta < 0) {
            hi = mid - 1;
          } else if (delta > 0) {
            lo = mid + 1;
          } else {
            assert mid >= 0;
            result.position = mid*totalIndexInterval;
            result.offset = termsStart + termsDictOffsets.get(mid);
            return;
          }
        }
        if (hi < 0) {
          assert hi == -1;
          hi = 0;
        }

        final long offset = termOffsets.get(hi);
        final int length = (int) (termOffsets.get(1+hi) - offset);
        termBytesReader.fill(result.term, termBytesStart + offset, length);

        result.position = hi*totalIndexInterval;
        result.offset = termsStart + termsDictOffsets.get(hi);
      }

      public void getIndexOffset(long ord, TermsIndexResult result) throws IOException {
        int idx = (int) (ord / totalIndexInterval);
        // caller must ensure ord is in bounds
        assert idx < numIndexTerms;
        fillResult(idx, result);
      }
    }
  }

  @Override
  public void loadTermsIndex(int indexDivisor) throws IOException {
    if (!indexLoaded) {

      this.indexDivisor = indexDivisor;
      this.totalIndexInterval = indexInterval * indexDivisor;

      Iterator<FieldIndexReader> it = fields.values().iterator();
      while(it.hasNext()) {
        it.next().loadTermsIndex();
      }

      indexLoaded = true;
      in.close();
      termBytesReader = termBytes.freeze(true);
    }
  }

  @Override
  public FieldReader getField(FieldInfo fieldInfo) {
    return fields.get(fieldInfo);
  }

  public static void files(Directory dir, SegmentInfo info, Collection<String> files) {
    files.add(IndexFileNames.segmentFileName(info.name, "", StandardCodec.TERMS_INDEX_EXTENSION));
  }

  public static void getIndexExtensions(Collection<String> extensions) {
    extensions.add(StandardCodec.TERMS_INDEX_EXTENSION);
  }

  @Override
  public void getExtensions(Collection<String> extensions) {
    getIndexExtensions(extensions);
  }

  @Override
  public void close() throws IOException {
    if (in != null && !indexLoaded) {
      in.close();
    }
    if (termBytesReader != null) {
      termBytesReader.close();
    }
  }

  protected void seekDir(IndexInput input, long dirOffset) throws IOException {
    input.seek(dirOffset);
  }
}
