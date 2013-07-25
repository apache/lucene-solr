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
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;
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

  // NOTE: long is overkill here, but we use this in a
  // number of places to multiply out the actual ord, and we
  // will overflow int during those multiplies.  So to avoid
  // having to upgrade each multiple to long in multiple
  // places (error prone), we use long here:
  private final long indexInterval;
  private final int indexDivisor;

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
  
  public FixedGapTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, Comparator<BytesRef> termComp, String segmentSuffix, IOContext context)
    throws IOException {

    this.termComp = termComp;

    this.indexDivisor = 1; // nocommit
    
    in = dir.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, FixedGapTermsIndexWriter.TERMS_INDEX_EXTENSION), context);
    
    boolean success = false;

    try {
      
      readHeader(in);
      indexInterval = in.readInt();
      if (indexInterval < 1) {
        throw new CorruptIndexException("invalid indexInterval: " + indexInterval + " (resource=" + in + ")");
      }
      
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

  private void readHeader(IndexInput input) throws IOException {
    CodecUtil.checkHeader(input, FixedGapTermsIndexWriter.CODEC_NAME,
      FixedGapTermsIndexWriter.VERSION_CURRENT, FixedGapTermsIndexWriter.VERSION_CURRENT);
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
          ord = mid*indexInterval;
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

      ord = hi*indexInterval;
      return fieldIndex.termsStart + fieldIndex.termsDictOffsets.get(hi);
    }

    @Override
    public long next() {
      final int idx = 1 + (int) (ord / indexInterval);
      if (idx >= fieldIndex.numIndexTerms) {
        return -1;
      }
      ord += indexInterval;

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
      int idx = (int) (ord / indexInterval);
      // caller must ensure ord is in bounds
      assert idx < fieldIndex.numIndexTerms;
      final long offset = fieldIndex.termOffsets.get(idx);
      final int length = (int) (fieldIndex.termOffsets.get(1+idx) - offset);
      termBytesReader.fillSlice(term, fieldIndex.termBytesStart + offset, length);
      this.ord = idx * indexInterval;
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
      final MonotonicBlockPackedReader termOffsets;

      // index pointers into main terms dict
      final MonotonicBlockPackedReader termsDictOffsets;

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

        // slurp in the images from disk:
          
        try {
          final long numTermBytes = packedIndexStart - indexStart;
          termBytes.copy(clone, numTermBytes);

          // records offsets into main terms dict file
          // nocommit: actually write these params
          termsDictOffsets = new MonotonicBlockPackedReader(clone, PackedInts.VERSION_CURRENT, FixedGapTermsIndexWriter.BLOCKSIZE, numIndexTerms, false);

          // records offsets into byte[] term data
          // nocommit: actually write these params
          termOffsets = new MonotonicBlockPackedReader(clone, PackedInts.VERSION_CURRENT, FixedGapTermsIndexWriter.BLOCKSIZE, 1+numIndexTerms, false);
        } finally {
          clone.close();
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
    input.seek(input.length() - 8);
    dirOffset = input.readLong();
    input.seek(dirOffset);
  }
}
