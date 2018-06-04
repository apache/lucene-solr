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

package org.apache.lucene.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * This reader filters out documents that have a doc values value in the given field and treat these
 * documents as soft deleted. Hard deleted documents will also be filtered out in the life docs of this reader.
 * @see IndexWriterConfig#setSoftDeletesField(String)
 * @see IndexWriter#softUpdateDocument(Term, Iterable, Field...)
 * @see SoftDeletesRetentionMergePolicy
 */
public final class SoftDeletesDirectoryReaderWrapper extends FilterDirectoryReader {
  private final String field;
  private final CacheHelper readerCacheHelper;
  /**
   * Creates a new soft deletes wrapper.
   * @param in the incoming directory reader
   * @param field the soft deletes field
   */
  public SoftDeletesDirectoryReaderWrapper(DirectoryReader in, String field) throws IOException {
    this(in, new SoftDeletesSubReaderWrapper(Collections.emptyMap(), field));
  }

  private SoftDeletesDirectoryReaderWrapper(DirectoryReader in, SoftDeletesSubReaderWrapper wrapper) throws IOException {
    super(in, wrapper);
    this.field = wrapper.field;
    readerCacheHelper = in.getReaderCacheHelper() == null ? null : new DelegatingCacheHelper(in.getReaderCacheHelper());
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
    Map<CacheKey, LeafReader> readerCache = new HashMap<>();
    for (LeafReader reader : getSequentialSubReaders()) {
      // we try to reuse the life docs instances here if the reader cache key didn't change
      if (reader instanceof SoftDeletesFilterLeafReader && reader.getReaderCacheHelper() != null) {
        readerCache.put(((SoftDeletesFilterLeafReader) reader).reader.getReaderCacheHelper().getKey(), reader);
      } else if (reader instanceof SoftDeletesFilterCodecReader && reader.getReaderCacheHelper() != null) {
        readerCache.put(((SoftDeletesFilterCodecReader) reader).reader.getReaderCacheHelper().getKey(), reader);
      }

    }
    return new SoftDeletesDirectoryReaderWrapper(in, new SoftDeletesSubReaderWrapper(readerCache, field));
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return readerCacheHelper;
  }

  private static class SoftDeletesSubReaderWrapper extends SubReaderWrapper {
    private final Map<CacheKey, LeafReader> mapping;
    private final String field;

    public SoftDeletesSubReaderWrapper(Map<CacheKey, LeafReader> oldReadersCache, String field) {
      Objects.requireNonNull(field, "Field must not be null");
      assert oldReadersCache != null;
      this.mapping = oldReadersCache;
      this.field = field;
    }

    @Override
    public LeafReader wrap(LeafReader reader) {
      CacheHelper readerCacheHelper = reader.getReaderCacheHelper();
      if (readerCacheHelper != null && mapping.containsKey(readerCacheHelper.getKey())) {
        // if the reader cache helper didn't change and we have it in the cache don't bother creating a new one
        return mapping.get(readerCacheHelper.getKey());
      }
      try {
        return SoftDeletesDirectoryReaderWrapper.wrap(reader, field);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  static LeafReader wrap(LeafReader reader, String field) throws IOException {
      DocIdSetIterator iterator = DocValuesFieldExistsQuery.getDocValuesDocIdSetIterator(field, reader);
      if (iterator == null) {
        return reader;
      }
      Bits liveDocs = reader.getLiveDocs();
      final FixedBitSet bits;
      if (liveDocs != null) {
        bits = SoftDeletesRetentionMergePolicy.cloneLiveDocs(liveDocs);
      } else {
        bits = new FixedBitSet(reader.maxDoc());
        bits.set(0, reader.maxDoc());
      }
      int numSoftDeletes = PendingSoftDeletes.applySoftDeletes(iterator, bits);
      int numDeletes = reader.numDeletedDocs() + numSoftDeletes;
      int numDocs = reader.maxDoc() - numDeletes;
      assert assertDocCounts(numDocs, numSoftDeletes, reader);
      return reader instanceof CodecReader ? new SoftDeletesFilterCodecReader((CodecReader) reader, bits, numDocs)
          : new SoftDeletesFilterLeafReader(reader, bits, numDocs);
  }

  private static boolean assertDocCounts(int expectedNumDocs, int numSoftDeletes, LeafReader reader) {
    if (reader instanceof SegmentReader) {
      SegmentReader segmentReader = (SegmentReader) reader;
      SegmentCommitInfo segmentInfo = segmentReader.getSegmentInfo();
      if (segmentReader.isNRT == false) {
        int numDocs = segmentInfo.info.maxDoc() - segmentInfo.getSoftDelCount() - segmentInfo.getDelCount();
        assert numDocs == expectedNumDocs : "numDocs: " + numDocs + " expected: " + expectedNumDocs
            + " maxDoc: " + segmentInfo.info.maxDoc()
            + " getDelCount: " + segmentInfo.getDelCount()
            + " getSoftDelCount: " + segmentInfo.getSoftDelCount()
            + " numSoftDeletes: " + numSoftDeletes
            + " reader.numDeletedDocs(): " + reader.numDeletedDocs();
      }
      // in the NRT case we don't have accurate numbers for getDelCount and getSoftDelCount since they might not be
      // flushed to disk when this reader is opened. We don't necessarily flush deleted doc on reopen but
      // we do for docValues.


    }

    return true;
  }

  static final class SoftDeletesFilterLeafReader extends FilterLeafReader {
    private final LeafReader reader;
    private final FixedBitSet bits;
    private final int numDocs;
    private final CacheHelper readerCacheHelper;

    private SoftDeletesFilterLeafReader(LeafReader reader, FixedBitSet bits, int numDocs) {
      super(reader);
      this.reader = reader;
      this.bits = bits;
      this.numDocs = numDocs;
      this.readerCacheHelper = reader.getReaderCacheHelper() == null ? null :
          new DelegatingCacheHelper(reader.getReaderCacheHelper());
    }

    @Override
    public Bits getLiveDocs() {
      return bits;
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return reader.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return readerCacheHelper;
    }
  }

  final static class SoftDeletesFilterCodecReader extends FilterCodecReader {
    private final LeafReader reader;
    private final FixedBitSet bits;
    private final int numDocs;
    private final CacheHelper readerCacheHelper;

    private SoftDeletesFilterCodecReader(CodecReader reader, FixedBitSet bits, int numDocs) {
      super(reader);
      this.reader = reader;
      this.bits = bits;
      this.numDocs = numDocs;
      this.readerCacheHelper = reader.getReaderCacheHelper() == null ? null :
          new DelegatingCacheHelper(reader.getReaderCacheHelper());
    }

    @Override
    public Bits getLiveDocs() {
      return bits;
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return reader.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return readerCacheHelper;
    }
  }

  private static class DelegatingCacheHelper implements CacheHelper {
    private final CacheHelper delegate;
    private final CacheKey cacheKey = new CacheKey();

    public DelegatingCacheHelper(CacheHelper delegate) {
      this.delegate = delegate;
    }

    @Override
    public CacheKey getKey() {
      return cacheKey;
    }

    @Override
    public void addClosedListener(ClosedListener listener) {
      // here we wrap the listener and call it with our cache key
      // this is important since this key will be used to cache the reader and otherwise we won't free caches etc.
      delegate.addClosedListener(unused -> listener.onClose(cacheKey));
    }
  }
}

