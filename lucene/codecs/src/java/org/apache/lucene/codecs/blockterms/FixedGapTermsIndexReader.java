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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.MonotonicBlockPackedReader;

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
  
  private final int packedIntsVersion;
  private final int blocksize;

  private final Comparator<BytesRef> termComp;

  private final static int PAGED_BYTES_BITS = 15;

  // all fields share this single logical byte[]
  private final PagedBytes termBytes = new PagedBytes(PAGED_BYTES_BITS);
  private PagedBytes.Reader termBytesReader;

  final HashMap<FieldInfo,FieldIndexData> fields = new HashMap<>();
  
  // start of the field info data
  private long dirOffset;
  
  private int version;
  
  public FixedGapTermsIndexReader(Directory dir, FieldInfos fieldInfos, String segment, Comparator<BytesRef> termComp, String segmentSuffix, IOContext context)
    throws IOException {

    this.termComp = termComp;
    
    final IndexInput in = dir.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, FixedGapTermsIndexWriter.TERMS_INDEX_EXTENSION), context);
    
    boolean success = false;

    try {
      
      readHeader(in);
      
      if (version >= FixedGapTermsIndexWriter.VERSION_CHECKSUM) {
        CodecUtil.checksumEntireFile(in);
      }
      
      indexInterval = in.readVInt();
      if (indexInterval < 1) {
        throw new CorruptIndexException("invalid indexInterval: " + indexInterval + " (resource=" + in + ")");
      }
      packedIntsVersion = in.readVInt();
      blocksize = in.readVInt();
      
      seekDir(in, dirOffset);

      // Read directory
      final int numFields = in.readVInt();     
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields + " (resource=" + in + ")");
      }
      //System.out.println("FGR: init seg=" + segment + " div=" + indexDivisor + " nF=" + numFields);
      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long numIndexTerms = in.readVInt(); // TODO: change this to a vLong if we fix writer to support > 2B index terms
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
        FieldIndexData previous = fields.put(fieldInfo, new FieldIndexData(in, indexStart, termsStart, packedIndexStart, packedOffsetsStart, numIndexTerms));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name + " (resource=" + in + ")");
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(in);
      } else {
        IOUtils.closeWhileHandlingException(in);
      }
      termBytesReader = termBytes.freeze(true);
    }
  }

  private void readHeader(IndexInput input) throws IOException {
    version = CodecUtil.checkHeader(input, FixedGapTermsIndexWriter.CODEC_NAME,
      FixedGapTermsIndexWriter.VERSION_CURRENT, FixedGapTermsIndexWriter.VERSION_CURRENT);
  }

  private class IndexEnum extends FieldIndexEnum {
    private final FieldIndexData fieldIndex;
    private final BytesRef term = new BytesRef();
    private long ord;

    public IndexEnum(FieldIndexData fieldIndex) {
      this.fieldIndex = fieldIndex;
    }

    @Override
    public BytesRef term() {
      return term;
    }

    @Override
    public long seek(BytesRef target) {
      long lo = 0;          // binary search
      long hi = fieldIndex.numIndexTerms - 1;

      while (hi >= lo) {
        long mid = (lo + hi) >>> 1;

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
      final long idx = 1 + (ord / indexInterval);
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
      long idx = ord / indexInterval;
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

  private final class FieldIndexData implements Accountable {
    // where this field's terms begin in the packed byte[]
    // data
    final long termBytesStart;
    
    // offset into index termBytes
    final MonotonicBlockPackedReader termOffsets;
    
    // index pointers into main terms dict
    final MonotonicBlockPackedReader termsDictOffsets;
    
    final long numIndexTerms;
    final long termsStart;
    
    public FieldIndexData(IndexInput in, long indexStart, long termsStart, long packedIndexStart, long packedOffsetsStart, long numIndexTerms) throws IOException {
      
      this.termsStart = termsStart;
      termBytesStart = termBytes.getPointer();
      
      IndexInput clone = in.clone();
      clone.seek(indexStart);
      
      this.numIndexTerms = numIndexTerms;
      assert this.numIndexTerms  > 0: "numIndexTerms=" + numIndexTerms;
      
      // slurp in the images from disk:
      
      try {
        final long numTermBytes = packedIndexStart - indexStart;
        termBytes.copy(clone, numTermBytes);
        
        // records offsets into main terms dict file
        termsDictOffsets = new MonotonicBlockPackedReader(clone, packedIntsVersion, blocksize, numIndexTerms, false);
        
        // records offsets into byte[] term data
        termOffsets = new MonotonicBlockPackedReader(clone, packedIntsVersion, blocksize, 1+numIndexTerms, false);
      } finally {
        clone.close();
      }
    }
    
    @Override
    public long ramBytesUsed() {
      return ((termOffsets!=null)? termOffsets.ramBytesUsed() : 0) + 
          ((termsDictOffsets!=null)? termsDictOffsets.ramBytesUsed() : 0);
    }
  }

  @Override
  public FieldIndexEnum getFieldEnum(FieldInfo fieldInfo) {
    return new IndexEnum(fields.get(fieldInfo));
  }

  @Override
  public void close() throws IOException {}

  private void seekDir(IndexInput input, long dirOffset) throws IOException {
    if (version >= FixedGapTermsIndexWriter.VERSION_CHECKSUM) {
      input.seek(input.length() - CodecUtil.footerLength() - 8);
    } else {
      input.seek(input.length() - 8);
    }
    dirOffset = input.readLong();
    input.seek(dirOffset);
  }

  @Override
  public long ramBytesUsed() {
    long sizeInBytes = ((termBytes!=null) ? termBytes.ramBytesUsed() : 0) + 
        ((termBytesReader!=null)? termBytesReader.ramBytesUsed() : 0);
    
    for(FieldIndexData entry : fields.values()) {
      sizeInBytes += entry.ramBytesUsed();
    }
    return sizeInBytes;
  }
}
