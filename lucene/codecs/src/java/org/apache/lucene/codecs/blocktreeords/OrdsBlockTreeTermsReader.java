package org.apache.lucene.codecs.blocktreeords;

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
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.blocktreeords.FSTOrdsOutputs.Output;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * See {@link OrdsBlockTreeTermsWriter}.
 *
 * @lucene.experimental
 */

public final class OrdsBlockTreeTermsReader extends FieldsProducer {

  // Open input to the main terms dict file (_X.tiv)
  final IndexInput in;

  //private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  final PostingsReaderBase postingsReader;

  private final TreeMap<String,OrdsFieldReader> fields = new TreeMap<>();

  /** File offset where the directory starts in the terms file. */
  private long dirOffset;

  /** File offset where the directory starts in the index file. */
  private long indexDirOffset;

  final String segment;
  
  private final int version;

  /** Sole constructor. */
  public OrdsBlockTreeTermsReader(Directory dir, FieldInfos fieldInfos, SegmentInfo info,
                                  PostingsReaderBase postingsReader, IOContext ioContext,
                                  String segmentSuffix)
    throws IOException {
    
    this.postingsReader = postingsReader;

    this.segment = info.name;
    in = dir.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, OrdsBlockTreeTermsWriter.TERMS_EXTENSION),
                       ioContext);

    boolean success = false;
    IndexInput indexIn = null;

    try {
      version = CodecUtil.checkHeader(in,
                                      OrdsBlockTreeTermsWriter.TERMS_CODEC_NAME,
                                      OrdsBlockTreeTermsWriter.VERSION_START,
                                      OrdsBlockTreeTermsWriter.VERSION_CURRENT);
      indexIn = dir.openInput(IndexFileNames.segmentFileName(segment, segmentSuffix, OrdsBlockTreeTermsWriter.TERMS_INDEX_EXTENSION),
                              ioContext);
      int indexVersion = CodecUtil.checkHeader(indexIn,
                                               OrdsBlockTreeTermsWriter.TERMS_INDEX_CODEC_NAME,
                                               OrdsBlockTreeTermsWriter.VERSION_START,
                                               OrdsBlockTreeTermsWriter.VERSION_CURRENT);
      if (indexVersion != version) {
        throw new CorruptIndexException("mixmatched version files: " + in + "=" + version + "," + indexIn + "=" + indexVersion);
      }
      
      // verify
      CodecUtil.checksumEntireFile(indexIn);

      // Have PostingsReader init itself
      postingsReader.init(in);
      
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(in);

      // Read per-field details
      seekDir(in, dirOffset);
      seekDir(indexIn, indexDirOffset);

      final int numFields = in.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields + " (resource=" + in + ")");
      }

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long numTerms = in.readVLong();
        assert numTerms >= 0;
        // System.out.println("read field=" + field + " numTerms=" + numTerms + " i=" + i);
        final int numBytes = in.readVInt();
        final BytesRef code = new BytesRef(new byte[numBytes]);
        in.readBytes(code.bytes, 0, numBytes);
        code.length = numBytes;
        final Output rootCode = OrdsBlockTreeTermsWriter.FST_OUTPUTS.newOutput(code, 0, numTerms);
        final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
        assert fieldInfo != null: "field=" + field;
        assert numTerms <= Integer.MAX_VALUE;
        final long sumTotalTermFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS_ONLY ? -1 : in.readVLong();
        final long sumDocFreq = in.readVLong();
        final int docCount = in.readVInt();
        final int longsSize = in.readVInt();
        // System.out.println("  longsSize=" + longsSize);

        BytesRef minTerm = readBytesRef(in);
        BytesRef maxTerm = readBytesRef(in);
        if (docCount < 0 || docCount > info.getDocCount()) { // #docs with field must be <= #docs
          throw new CorruptIndexException("invalid docCount: " + docCount + " maxDoc: " + info.getDocCount() + " (resource=" + in + ")");
        }
        if (sumDocFreq < docCount) {  // #postings must be >= #docs with field
          throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount + " (resource=" + in + ")");
        }
        if (sumTotalTermFreq != -1 && sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
          throw new CorruptIndexException("invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq + " (resource=" + in + ")");
        }
        final long indexStartFP = indexIn.readVLong();
        OrdsFieldReader previous = fields.put(fieldInfo.name,       
                                              new OrdsFieldReader(this, fieldInfo, numTerms, rootCode, sumTotalTermFreq, sumDocFreq, docCount,
                                                                  indexStartFP, longsSize, indexIn, minTerm, maxTerm));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name + " (resource=" + in + ")");
        }
      }
      indexIn.close();

      success = true;
    } finally {
      if (!success) {
        // this.close() will close in:
        IOUtils.closeWhileHandlingException(indexIn, this);
      }
    }
  }

  private static BytesRef readBytesRef(IndexInput in) throws IOException {
    BytesRef bytes = new BytesRef();
    bytes.length = in.readVInt();
    bytes.bytes = new byte[bytes.length];
    in.readBytes(bytes.bytes, 0, bytes.length);
    return bytes;
  }

  /** Seek {@code input} to the directory offset. */
  private void seekDir(IndexInput input, long dirOffset)
      throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    dirOffset = input.readLong();
    input.seek(dirOffset);
  }

  // for debugging
  // private static String toHex(int v) {
  //   return "0x" + Integer.toHexString(v);
  // }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(in, postingsReader);
    } finally { 
      // Clear so refs to terms index is GCable even if
      // app hangs onto us:
      fields.clear();
    }
  }

  @Override
  public Iterator<String> iterator() {
    return Collections.unmodifiableSet(fields.keySet()).iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    assert field != null;
    return fields.get(field);
  }

  @Override
  public int size() {
    return fields.size();
  }

  // for debugging
  String brToString(BytesRef b) {
    if (b == null) {
      return "null";
    } else {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    long sizeInByes = ((postingsReader!=null) ? postingsReader.ramBytesUsed() : 0);
    for (OrdsFieldReader reader : fields.values()) {
      sizeInByes += reader.ramBytesUsed();
    }
    return sizeInByes;
  }

  @Override
  public void checkIntegrity() throws IOException {
    // term dictionary
    CodecUtil.checksumEntireFile(in);
      
    // postings
    postingsReader.checkIntegrity();
  }
}
