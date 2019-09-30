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
package org.apache.lucene.codecs.blocktree;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.Outputs;

/** A block-based terms index and dictionary that assigns
 *  terms to variable length blocks according to how they
 *  share prefixes.  The terms index is a prefix trie
 *  whose leaves are term blocks.  The advantage of this
 *  approach is that seekExact is often able to
 *  determine a term cannot exist without doing any IO, and
 *  intersection with Automata is very fast.  Note that this
 *  terms dictionary has its own fixed terms index (ie, it
 *  does not support a pluggable terms index
 *  implementation).
 *
 *  <p><b>NOTE</b>: this terms dictionary supports
 *  min/maxItemsPerBlock during indexing to control how
 *  much memory the terms index uses.</p>
 *
 *  <p>The data structure used by this implementation is very
 *  similar to a burst trie
 *  (http://citeseer.ist.psu.edu/viewdoc/summary?doi=10.1.1.18.3499),
 *  but with added logic to break up too-large blocks of all
 *  terms sharing a given prefix into smaller ones.</p>
 *
 *  <p>Use {@link org.apache.lucene.index.CheckIndex} with the <code>-verbose</code>
 *  option to see summary statistics on the blocks in the
 *  dictionary.
 *
 *  See {@link BlockTreeTermsWriter}.
 *
 * @lucene.experimental
 */

public final class BlockTreeTermsReader extends FieldsProducer {

  /**
   * An enum that allows to control if term index FSTs are loaded into memory or read off-heap
   */
  public enum FSTLoadMode {
    /**
     * Always read FSTs from disk.
     * NOTE: If this option is used the FST will be read off-heap even if buffered directory implementations
     * are used.
     */
    OFF_HEAP,
    /**
     * Never read FSTs from disk ie. all fields FSTs are loaded into memory
     */
    ON_HEAP,
    /**
     * Always read FSTs from disk.
     * An exception is made for ID fields in an IndexWriter context which are always loaded into memory.
     * This is useful to guarantee best update performance even if a non MMapDirectory is used.
     * NOTE: If this option is used the FST will be read off-heap even if buffered directory implementations
     * are used.
     * See {@link FSTLoadMode#AUTO}
     */
    OPTIMIZE_UPDATES_OFF_HEAP,
    /**
     * Automatically make the decision if FSTs are read from disk depending if the segment read from an MMAPDirectory
     * An exception is made for ID fields in an IndexWriter context which are always loaded into memory.
     */
    AUTO
  }

  /** Attribute key for fst mode. */
  public static final String FST_MODE_KEY = "blocktree.terms.fst";

  static final Outputs<BytesRef> FST_OUTPUTS = ByteSequenceOutputs.getSingleton();
  
  static final BytesRef NO_OUTPUT = FST_OUTPUTS.getNoOutput();

  static final int OUTPUT_FLAGS_NUM_BITS = 2;
  static final int OUTPUT_FLAGS_MASK = 0x3;
  static final int OUTPUT_FLAG_IS_FLOOR = 0x1;
  static final int OUTPUT_FLAG_HAS_TERMS = 0x2;

  /** Extension of terms file */
  static final String TERMS_EXTENSION = "tim";
  final static String TERMS_CODEC_NAME = "BlockTreeTermsDict";

  /** Initial terms format. */
  public static final int VERSION_START = 2;

  /** Auto-prefix terms have been superseded by points. */
  public static final int VERSION_AUTO_PREFIX_TERMS_REMOVED = 3;

  /** Current terms format. */
  public static final int VERSION_CURRENT = VERSION_AUTO_PREFIX_TERMS_REMOVED;

  /** Extension of terms index file */
  static final String TERMS_INDEX_EXTENSION = "tip";
  final static String TERMS_INDEX_CODEC_NAME = "BlockTreeTermsIndex";

  // Open input to the main terms dict file (_X.tib)
  final IndexInput termsIn;
  // Open input to the terms index file (_X.tip)
  final IndexInput indexIn;

  //private static final boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // Reads the terms dict entries, to gather state to
  // produce DocsEnum on demand
  final PostingsReaderBase postingsReader;

  private final TreeMap<String,FieldReader> fields = new TreeMap<>();

  final String segment;
  
  final int version;

  /** Sole constructor. */
  public BlockTreeTermsReader(PostingsReaderBase postingsReader, SegmentReadState state, FSTLoadMode defaultLoadMode) throws IOException {
    boolean success = false;
    
    this.postingsReader = postingsReader;
    this.segment = state.segmentInfo.name;
    
    String termsName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_EXTENSION);
    try {
      termsIn = state.directory.openInput(termsName, state.context);
      version = CodecUtil.checkIndexHeader(termsIn, TERMS_CODEC_NAME, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

      if (version < VERSION_AUTO_PREFIX_TERMS_REMOVED) {
        // pre-6.2 index, records whether auto-prefix terms are enabled in the header
        byte b = termsIn.readByte();
        if (b != 0) {
          throw new CorruptIndexException("Index header pretends the index has auto-prefix terms: " + b, termsIn);
        }
      }

      String indexName = IndexFileNames.segmentFileName(segment, state.segmentSuffix, TERMS_INDEX_EXTENSION);
      indexIn = state.directory.openInput(indexName, state.context);
      CodecUtil.checkIndexHeader(indexIn, TERMS_INDEX_CODEC_NAME, version, version, state.segmentInfo.getId(), state.segmentSuffix);
      CodecUtil.checksumEntireFile(indexIn);

      // Have PostingsReader init itself
      postingsReader.init(termsIn, state);
      
      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(termsIn);

      // Read per-field details
      seekDir(termsIn);
      seekDir(indexIn);

      final FSTLoadMode fstLoadMode = getLoadMode(state.readerAttributes, FST_MODE_KEY, defaultLoadMode);
      final int numFields = termsIn.readVInt();
      if (numFields < 0) {
        throw new CorruptIndexException("invalid numFields: " + numFields, termsIn);
      }
      for (int i = 0; i < numFields; ++i) {
        final int field = termsIn.readVInt();
        final long numTerms = termsIn.readVLong();
        if (numTerms <= 0) {
          throw new CorruptIndexException("Illegal numTerms for field number: " + field, termsIn);
        }
        final BytesRef rootCode = readBytesRef(termsIn);
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
        if (fieldInfo == null) {
          throw new CorruptIndexException("invalid field number: " + field, termsIn);
        }
        final long sumTotalTermFreq = termsIn.readVLong();
        // when frequencies are omitted, sumDocFreq=sumTotalTermFreq and only one value is written.
        final long sumDocFreq = fieldInfo.getIndexOptions() == IndexOptions.DOCS ? sumTotalTermFreq : termsIn.readVLong();
        final int docCount = termsIn.readVInt();
        final int longsSize = termsIn.readVInt();
        if (longsSize < 0) {
          throw new CorruptIndexException("invalid longsSize for field: " + fieldInfo.name + ", longsSize=" + longsSize, termsIn);
        }
        BytesRef minTerm = readBytesRef(termsIn);
        BytesRef maxTerm = readBytesRef(termsIn);
        if (docCount < 0 || docCount > state.segmentInfo.maxDoc()) { // #docs with field must be <= #docs
          throw new CorruptIndexException("invalid docCount: " + docCount + " maxDoc: " + state.segmentInfo.maxDoc(), termsIn);
        }
        if (sumDocFreq < docCount) {  // #postings must be >= #docs with field
          throw new CorruptIndexException("invalid sumDocFreq: " + sumDocFreq + " docCount: " + docCount, termsIn);
        }
        if (sumTotalTermFreq < sumDocFreq) { // #positions must be >= #postings
          throw new CorruptIndexException("invalid sumTotalTermFreq: " + sumTotalTermFreq + " sumDocFreq: " + sumDocFreq, termsIn);
        }
        final FSTLoadMode perFieldLoadMode = getLoadMode(state.readerAttributes, FST_MODE_KEY + "." + fieldInfo.name, fstLoadMode);
        final long indexStartFP = indexIn.readVLong();
        FieldReader previous = fields.put(fieldInfo.name,
                                          new FieldReader(this, fieldInfo, numTerms, rootCode, sumTotalTermFreq, sumDocFreq, docCount,
                                                          indexStartFP, longsSize, indexIn, minTerm, maxTerm, state.openedFromWriter, perFieldLoadMode));
        if (previous != null) {
          throw new CorruptIndexException("duplicate field: " + fieldInfo.name, termsIn);
        }
      }
      success = true;
    } finally {
      if (!success) {
        // this.close() will close in:
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  private static FSTLoadMode getLoadMode(Map<String, String> attributes, String key, FSTLoadMode defaultValue) {
    String value = attributes.get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return FSTLoadMode.valueOf(value);
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Invalid value for " + key + " expected one of: "
          + Arrays.toString(FSTLoadMode.values()) + " but was: " + value, ex);
    }
  }

  private static BytesRef readBytesRef(IndexInput in) throws IOException {
    int numBytes = in.readVInt();
    if (numBytes < 0) {
      throw new CorruptIndexException("invalid bytes length: " + numBytes, in);
    }
    
    BytesRef bytes = new BytesRef();
    bytes.length = numBytes;
    bytes.bytes = new byte[numBytes];
    in.readBytes(bytes.bytes, 0, numBytes);

    return bytes;
  }

  /** Seek {@code input} to the directory offset. */
  private static void seekDir(IndexInput input) throws IOException {
    input.seek(input.length() - CodecUtil.footerLength() - 8);
    long offset = input.readLong();
    input.seek(offset);
  }

  // for debugging
  // private static String toHex(int v) {
  //   return "0x" + Integer.toHexString(v);
  // }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(indexIn, termsIn, postingsReader);
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
    for(FieldReader reader : fields.values()) {
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
    CodecUtil.checksumEntireFile(termsIn);
      
    // postings
    postingsReader.checkIntegrity();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(fields=" + fields.size() + ",delegate=" + postingsReader + ")";
  }
}
