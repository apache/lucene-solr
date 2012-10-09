package org.apache.lucene.codecs.blockterms;

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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.PackedInts;

import java.util.HashMap;
import java.util.Comparator;
import java.io.IOException;

import org.apache.lucene.index.IndexFileNames;

/** 
 * TermsIndexReader for simple every Nth terms indexes.
 *
 * @see FixedGapTermsIndexWriter
 * @lucene.experimental 
 */
public class FixedGapTermsIndexReader extends TermsIndexReaderBase {

  // NOTE: long is overkill here, since this number is 128
  // by default and only indexDivisor * 128 if you change
  // the indexDivisor at search time.  But, we use this in a
  // number of places to multiply out the actual ord, and we
  // will overflow int during those multiplies.  So to avoid
  // having to upgrade each multiple to long in multiple
  // places (error prone), we use long here:
  private long totalIndexInterval;

  private int indexDivisor;
  final private int indexInterval;

  // Closed if indexLoaded is true:
  private IndexInput in;
  private volatile boolean indexLoaded;

  private final Comparator<BytesRef> termComp;

  private final static int PAGED_BYTES_BITS = 15;

  // all fields share this single logical byte[]
  private final PagedBytes termBytes = new PagedBytes(PAGED_BYTES_BITS);
  private PagedBytes.Reader termBytesReader;

  final HashMap<FieldInfo,FieldIndexData> fields = new HashMap<FieldInfo,FieldIndexData>();
  
  // start of the field info data
  private long dirOffset;
  
  private final int version;

  public FixedGapTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, int indexDivisor, Comparator<BytesRef> termComp, String segmentSuffix, IOContext context)
    throws IOException {

    this.termComp = termComp;

    assert indexDivisor == -1 || indexDivisor > 0;

    in = dir.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, FixedGapTermsIndexWriter.TERMS_INDEX_EXTENSION), context);
    
    boolean success = false;

    try {
      
      version = readHeader(in);
      indexInterval = in.readInt();
      if (indexInterval < 1) {
        throw new CorruptIndexException("invalid indexInterval: " + indexInterval + " (resource=" + in + ")");
      }
      this.indexDivisor = indexDivisor;

      if (indexDivisor < 0) {
        totalIndexInterval = indexInterval;
      } else {
        // In case terms index gets loaded, later, on demand
        totalIndexInterval = indexInterval * indexDivisor;
      }
      assert totalIndexInterval > 0;
      
      seekDir(in, dirOffset);

      // Read directory
      final int numFields = in.readVInt();     
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields + " (resource=" + in + ")");
      }
      //System.out.println("FGR: init seg=" + segment + " div=" + indexDivisor + " nF=" + numFields);
      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final int numIndexTerms = in.readVInt();
        if (numIndexTerms < 0) {
          throw new CorruptIndexException("invalid numIndexTerms: " + numIndexTerms + " (resource=" + in + ")");
        }
        final long termsStart = in.readVLong();
        final long indexStart = in.readVLong();
        final long packedIndexStart = in.readVLong();
        final long packedOffsetsStart = in.readVLong();
        if (packedIndexStart < indexStart) {
          throw new CorruptIndexException("invalid packedIndexStart: " + packedIndexStart + " indexStart: " + indexStart + "numIndexTerms: " + numIndexTerms + " (resource=" + in + ")");
        }
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        FieldIndexData previous = fields.put(fieldInfo, new FieldIndexData(fieldInfo, numIndexTerms, indexStart, termsStart, packedIndexStart, packedOffsetsStart));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name + " (resource=" + in + ")");
        }
      }
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
      if (indexDivisor > 0) {
        in.close();
        in = null;
        if (success) {
          indexLoaded = true;
        }
        termBytesReader = termBytes.freeze(true);
      }
    }
  }
  
  @Override
  public int getDivisor() {
    return indexDivisor;
  }

  private int readHeader(IndexInput input) throws IOException {
    int version = CodecUtil.checkHeader(input, FixedGapTermsIndexWriter.CODEC_NAME,
      FixedGapTermsIndexWriter.VERSION_START, FixedGapTermsIndexWriter.VERSION_CURRENT);
    if (version < FixedGapTermsIndexWriter.VERSION_APPEND_ONLY) {
      dirOffset = input.readLong();
    }
    return version;
  }

  private class IndexEnum extends FieldIndexEnum {
    private final FieldIndexData.CoreFieldIndex fieldIndex;
    private final BytesRef term = new BytesRef();
    private long ord;

    public IndexEnum(FieldIndexData.CoreFieldIndex fieldIndex) {
      this.fieldIndex = fieldIndex;
    }

    @Override
    public BytesRef term() {
      return term;
    }

    @Override
    public long seek(BytesRef target) {
      int lo = 0;          // binary search
      int hi = fieldIndex.numIndexTerms - 1;
      assert totalIndexInterval > 0 : "totalIndexInterval=" + totalIndexInterval;

      while (hi >= lo) {
        int mid = (lo + hi) >>> 1;

        final long offset = fieldIndex.termOffsets.get(mid);
        final int length = (int) (fieldIndex.termOffsets.get(1+mid) - offset);
        termBytesReader.fillSlice(term, fieldIndex.termBytesStart + offset, length);

        int delta = termComp.compare(target, term);
        if (delta < 0) {
          hi = mid - 1;
        } else if (delta > 0) {
          lo = mid + 1;
        } else {
          assert mid >= 0;
          ord = mid*totalIndexInterval;
          return fieldIndex.termsStart + fieldIndex.termsDictOffsets.get(mid);
        }
      }

      if (hi < 0) {
        assert hi == -1;
        hi = 0;
      }

      final long offset = fieldIndex.termOffsets.get(hi);
      final int length = (int) (fieldIndex.termOffsets.get(1+hi) - offset);
      termBytesReader.fillSlice(term, fieldIndex.termBytesStart + offset, length);

      ord = hi*totalIndexInterval;
      return fieldIndex.termsStart + fieldIndex.termsDictOffsets.get(hi);
    }

    @Override
    public long next() {
      final int idx = 1 + (int) (ord / totalIndexInterval);
      if (idx >= fieldIndex.numIndexTerms) {
        return -1;
      }
      ord += totalIndexInterval;

      final long offset = fieldIndex.termOffsets.get(idx);
      final int length = (int) (fieldIndex.termOffsets.get(1+idx) - offset);
      termBytesReader.fillSlice(term, fieldIndex.termBytesStart + offset, length);
      return fieldIndex.termsStart + fieldIndex.termsDictOffsets.get(idx);
    }

    @Override
    public long ord() {
      return ord;
    }

    @Override
    public long seek(long ord) {
      int idx = (int) (ord / totalIndexInterval);
      // caller must ensure ord is in bounds
      assert idx < fieldIndex.numIndexTerms;
      final long offset = fieldIndex.termOffsets.get(idx);
      final int length = (int) (fieldIndex.termOffsets.get(1+idx) - offset);
      termBytesReader.fillSlice(term, fieldIndex.termBytesStart + offset, length);
      this.ord = idx * totalIndexInterval;
      return fieldIndex.termsStart + fieldIndex.termsDictOffsets.get(idx);
    }
  }

  @Override
  public boolean supportsOrd() {
    return true;
  }

  private final class FieldIndexData {

    volatile CoreFieldIndex coreIndex;

    private final long indexStart;
    private final long termsStart;
    private final long packedIndexStart;
    private final long packedOffsetsStart;

    private final int numIndexTerms;

    public FieldIndexData(FieldInfo fieldInfo, int numIndexTerms, long indexStart, long termsStart, long packedIndexStart,
                          long packedOffsetsStart) throws IOException {

      this.termsStart = termsStart;
      this.indexStart = indexStart;
      this.packedIndexStart = packedIndexStart;
      this.packedOffsetsStart = packedOffsetsStart;
      this.numIndexTerms = numIndexTerms;

      if (indexDivisor > 0) {
        loadTermsIndex();
      }
    }

    private void loadTermsIndex() throws IOException {
      if (coreIndex == null) {
        coreIndex = new CoreFieldIndex(indexStart, termsStart, packedIndexStart, packedOffsetsStart, numIndexTerms);
      }
    }

    private final class CoreFieldIndex {

      // where this field's terms begin in the packed byte[]
      // data
      final long termBytesStart;

      // offset into index termBytes
      final PackedInts.Reader termOffsets;

      // index pointers into main terms dict
      final PackedInts.Reader termsDictOffsets;

      final int numIndexTerms;
      final long termsStart;

      public CoreFieldIndex(long indexStart, long termsStart, long packedIndexStart, long packedOffsetsStart, int numIndexTerms) throws IOException {

        this.termsStart = termsStart;
        termBytesStart = termBytes.getPointer();

        IndexInput clone = in.clone();
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
          final IndexInput clone1 = in.clone();
          final IndexInput clone2 = in.clone();

          try {
            // Subsample the index terms
            clone1.seek(packedIndexStart);
            final PackedInts.ReaderIterator termsDictOffsetsIter = PackedInts.getReaderIterator(clone1, PackedInts.DEFAULT_BUFFER_SIZE);

            clone2.seek(packedOffsetsStart);
            final PackedInts.ReaderIterator termOffsetsIter = PackedInts.getReaderIterator(clone2,  PackedInts.DEFAULT_BUFFER_SIZE);

            // TODO: often we can get by w/ fewer bits per
            // value, below.. .but this'd be more complex:
            // we'd have to try @ fewer bits and then grow
            // if we overflowed it.

            PackedInts.Mutable termsDictOffsetsM = PackedInts.getMutable(this.numIndexTerms, termsDictOffsetsIter.getBitsPerValue(), PackedInts.DEFAULT);
            PackedInts.Mutable termOffsetsM = PackedInts.getMutable(this.numIndexTerms+1, termOffsetsIter.getBitsPerValue(), PackedInts.DEFAULT);

            termsDictOffsets = termsDictOffsetsM;
            termOffsets = termOffsetsM;

            int upto = 0;

            long termOffsetUpto = 0;

            while(upto < this.numIndexTerms) {
              // main file offset copies straight over
              termsDictOffsetsM.set(upto, termsDictOffsetsIter.next());

              termOffsetsM.set(upto, termOffsetUpto);

              long termOffset = termOffsetsIter.next();
              long nextTermOffset = termOffsetsIter.next();
              final int numTermBytes = (int) (nextTermOffset - termOffset);

              clone.seek(indexStart + termOffset);
              assert indexStart + termOffset < clone.length() : "indexStart=" + indexStart + " termOffset=" + termOffset + " len=" + clone.length();
              assert indexStart + termOffset + numTermBytes < clone.length();

              termBytes.copy(clone, numTermBytes);
              termOffsetUpto += numTermBytes;

              upto++;
              if (upto == this.numIndexTerms) {
                break;
              }

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
    }
  }

  @Override
  public FieldIndexEnum getFieldEnum(FieldInfo fieldInfo) {
    final FieldIndexData fieldData = fields.get(fieldInfo);
    if (fieldData.coreIndex == null) {
      return null;
    } else {
      return new IndexEnum(fieldData.coreIndex);
    }
  }

  @Override
  public void close() throws IOException {
    if (in != null && !indexLoaded) {
      in.close();
    }
  }

  private void seekDir(IndexInput input, long dirOffset) throws IOException {
    if (version >= FixedGapTermsIndexWriter.VERSION_APPEND_ONLY) {
      input.seek(input.length() - 8);
      dirOffset = input.readLong();
    }
    input.seek(dirOffset);
  }
}
