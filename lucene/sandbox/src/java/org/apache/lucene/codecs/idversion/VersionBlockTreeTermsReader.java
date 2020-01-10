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
package org.apache.lucene.codecs.idversion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.PairOutputs.Pair;

/**
 * See {@link VersionBlockTreeTermsWriter}.
 *
 * @lucene.experimental
 */

public final class VersionBlockTreeTermsReader extends FieldsProducer {

  // Open input to the main terms dict file (_X.tiv)
  final IndexInput in;

  //private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  final PostingsReaderBase postingsReader;

  private final TreeMap<String,VersionFieldReader> fields = new TreeMap<>();

  /** Sole constructor. */
  public VersionBlockTreeTermsReader(PostingsReaderBase postingsReader, SegmentReadState state) throws IOException {
    
    this.postingsReader = postingsReader;

    String termsFile = IndexFileNames.segmentFileName(state.segmentInfo.name, 
                                                      state.segmentSuffix, 
                                                      VersionBlockTreeTermsWriter.TERMS_EXTENSION);
    in = state.directory.openInput(termsFile, state.context);

    boolean success = false;
    IndexInput indexIn = null;

    try {
      int termsVersion = CodecUtil.checkIndexHeader(in, VersionBlockTreeTermsWriter.TERMS_CODEC_NAME,
                                                          VersionBlockTreeTermsWriter.VERSION_START,
                                                          VersionBlockTreeTermsWriter.VERSION_CURRENT,
                                                          state.segmentInfo.getId(), state.segmentSuffix);
      
      String indexFile = IndexFileNames.segmentFileName(state.segmentInfo.name, 
                                                        state.segmentSuffix, 
                                                        VersionBlockTreeTermsWriter.TERMS_INDEX_EXTENSION);
      indexIn = state.directory.openInput(indexFile, state.context);
      int indexVersion = CodecUtil.checkIndexHeader(indexIn, VersionBlockTreeTermsWriter.TERMS_INDEX_CODEC_NAME,
                                                               VersionBlockTreeTermsWriter.VERSION_START,
                                                               VersionBlockTreeTermsWriter.VERSION_CURRENT,
                                                               state.segmentInfo.getId(), state.segmentSuffix);
      
      if (indexVersion != termsVersion) {
        throw new CorruptIndexException("mixmatched version files: " + in + "=" + termsVersion + "," + indexIn + "=" + indexVersion, indexIn);
      }
      
      // verify
      CodecUtil.checksumEntireFile(indexIn);

      // Have PostingsReader init itself
      postingsReader.init(in, state);
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(in);

      // Read per-field details
      seekDir(in);
      seekDir(indexIn);

      final int numFields = in.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields, in);
      }

      for(int i=0;i<numFields;i++) {
        final int field = in.readVInt();
        final long numTerms = in.readVLong();
        assert numTerms >= 0;
        final int numBytes = in.readVInt();
        final BytesRef code = new BytesRef(new byte[numBytes]);
        in.readBytes(code.bytes, 0, numBytes);
        code.length = numBytes;
        final long version = in.readVLong();
        final Pair<BytesRef,Long> rootCode = VersionBlockTreeTermsWriter.FST_OUTPUTS.newPair(code, version);
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        assert fieldInfo != null: "field=" + field;
        final long sumTotalTermFreq = numTerms;
        final long sumDocFreq = numTerms;
        assert numTerms <= Integer.MAX_VALUE;
        final int docCount = (int) numTerms;
        final int longsSize = in.readVInt();

        BytesRef minTerm = readBytesRef(in);
        BytesRef maxTerm = readBytesRef(in);
        if (docCount < 0 || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
          throw new CorruptIndexException("invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(), in);
        }
        if (sumDocFreq < docCount) {  // #postings must be >= #docs with field
          throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, in);
        }
        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
          throw new CorruptIndexException("invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq, in);
        }
        final long indexStartFP = indexIn.readVLong();
        VersionFieldReader previous = fields.put(fieldInfo.name,       
                                                 new VersionFieldReader(this, fieldInfo, numTerms, rootCode, sumTotalTermFreq, sumDocFreq, docCount,
                                                                        indexStartFP, longsSize, indexIn, minTerm, maxTerm));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name, in);
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
  private void seekDir(IndexInput input) throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    long dirOffset = input.readLong();
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
    long sizeInBytes = postingsReader.ramBytesUsed();
    for(VersionFieldReader reader : fields.values()) {
      sizeInBytes += reader.ramBytesUsed();
    }
    return sizeInBytes;
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    List<Accountable> resources = new ArrayList<>(Accountables.namedAccountables("field", fields));
    resources.add(Accountables.namedAccountable("delegate", postingsReader));
    return Collections.unmodifiableList(resources);
  }

  @Override
  public void checkIntegrity() throws IOException {
    // term dictionary
    CodecUtil.checksumEntireFile(in);
      
    // postings
    postingsReader.checkIntegrity();
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ",delegate=" + postingsReader.toString() + ")";
  }
}
