package org.apache.lucene.index;

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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.AtomicReader.CoreClosedListener;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** Holds core readers that are shared (unchanged) when
 * SegmentReader is cloned or reopened */
final class SegmentCoreReaders implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SegmentCoreReaders.class);

  // Counts how many other readers share the core objects
  // (freqStream, proxStream, tis, etc.) of this reader;
  // when coreRef drops to 0, these core objects may be
  // closed.  A given instance of SegmentReader may be
  // closed, even though it shares core objects with other
  // SegmentReaders:
  private final AtomicInteger ref = new AtomicInteger(1);
  
  final FieldsProducer fields;
  final DocValuesProducer normsProducer;

  final int termsIndexDivisor;

  final StoredFieldsReader fieldsReaderOrig;
  final TermVectorsReader termVectorsReaderOrig;
  final CompoundFileDirectory cfsReader;

  // TODO: make a single thread local w/ a
  // Thingy class holding fieldsReader, termVectorsReader,
  // normsProducer

  final CloseableThreadLocal<StoredFieldsReader> fieldsReaderLocal = new CloseableThreadLocal<StoredFieldsReader>() {
    @Override
    protected StoredFieldsReader initialValue() {
      return fieldsReaderOrig.clone();
    }
  };
  
  final CloseableThreadLocal<TermVectorsReader> termVectorsLocal = new CloseableThreadLocal<TermVectorsReader>() {
    @Override
    protected TermVectorsReader initialValue() {
      return (termVectorsReaderOrig == null) ? null : termVectorsReaderOrig.clone();
    }
  };

  final CloseableThreadLocal<Map<String,Object>> normsLocal = new CloseableThreadLocal<Map<String,Object>>() {
    @Override
    protected Map<String,Object> initialValue() {
      return new HashMap<>();
    }
  };

  private final Set<CoreClosedListener> coreClosedListeners = 
      Collections.synchronizedSet(new LinkedHashSet<CoreClosedListener>());
  
  SegmentCoreReaders(SegmentReader owner, Directory dir, SegmentCommitInfo si, IOContext context, int termsIndexDivisor) throws IOException {

    if (termsIndexDivisor == 0) {
      throw new IllegalArgumentException("indexDivisor must be < 0 (don't load terms index) or greater than 0 (got 0)");
    }
    
    final Codec codec = si.info.getCodec();
    final Directory cfsDir; // confusing name: if (cfs) its the cfsdir, otherwise its the segment's directory.

    boolean success = false;
    
    try {
      if (si.info.getUseCompoundFile()) {
        cfsDir = cfsReader = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(si.info.name, "", IndexFileNames.COMPOUND_FILE_EXTENSION), context, false);
      } else {
        cfsReader = null;
        cfsDir = dir;
      }

      final FieldInfos fieldInfos = owner.fieldInfos;
      
      this.termsIndexDivisor = termsIndexDivisor;
      final PostingsFormat format = codec.postingsFormat();
      final SegmentReadState segmentReadState = new SegmentReadState(cfsDir, si.info, fieldInfos, context, termsIndexDivisor);
      // Ask codec for its Fields
      fields = format.fieldsProducer(segmentReadState);
      assert fields != null;
      // ask codec for its Norms: 
      // TODO: since we don't write any norms file if there are no norms,
      // kinda jaky to assume the codec handles the case of no norms file at all gracefully?!

      if (fieldInfos.hasNorms()) {
        normsProducer = codec.normsFormat().normsProducer(segmentReadState);
        assert normsProducer != null;
      } else {
        normsProducer = null;
      }
  
      fieldsReaderOrig = si.info.getCodec().storedFieldsFormat().fieldsReader(cfsDir, si.info, fieldInfos, context);

      if (fieldInfos.hasVectors()) { // open term vector files only as needed
        termVectorsReaderOrig = si.info.getCodec().termVectorsFormat().vectorsReader(cfsDir, si.info, fieldInfos, context);
      } else {
        termVectorsReaderOrig = null;
      }

      success = true;
    } finally {
      if (!success) {
        decRef();
      }
    }
  }
  
  int getRefCount() {
    return ref.get();
  }
  
  void incRef() {
    int count;
    while ((count = ref.get()) > 0) {
      if (ref.compareAndSet(count, count+1)) {
        return;
      }
    }
    throw new AlreadyClosedException("SegmentCoreReaders is already closed");
  }

  NumericDocValues getNormValues(FieldInfos infos, String field) throws IOException {
    Map<String,Object> normFields = normsLocal.get();

    NumericDocValues norms = (NumericDocValues) normFields.get(field);
    if (norms != null) {
      return norms;
    } else {
      FieldInfo fi = infos.fieldInfo(field);
      if (fi == null || !fi.hasNorms()) {
        // Field does not exist or does not index norms
        return null;
      }
      assert normsProducer != null;
      norms = normsProducer.getNumeric(fi);
      normFields.put(field, norms);
      return norms;
    }
  }

  void decRef() throws IOException {
    if (ref.decrementAndGet() == 0) {
//      System.err.println("--- closing core readers");
      Throwable th = null;
      try {
        IOUtils.close(termVectorsLocal, fieldsReaderLocal, normsLocal, fields, termVectorsReaderOrig, fieldsReaderOrig,
            cfsReader, normsProducer);
      } catch (Throwable throwable) {
        th = throwable;
      } finally {
        notifyCoreClosedListeners(th);
      }
    }
  }
  
  private void notifyCoreClosedListeners(Throwable th) {
    synchronized(coreClosedListeners) {
      for (CoreClosedListener listener : coreClosedListeners) {
        // SegmentReader uses our instance as its
        // coreCacheKey:
        try {
          listener.onClose(this);
        } catch (Throwable t) {
          if (th == null) {
            th = t;
          } else {
            th.addSuppressed(t);
          }
        }
      }
      IOUtils.reThrowUnchecked(th);
    }
  }

  void addCoreClosedListener(CoreClosedListener listener) {
    coreClosedListeners.add(listener);
  }
  
  void removeCoreClosedListener(CoreClosedListener listener) {
    coreClosedListeners.remove(listener);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED +
        ((normsProducer!=null) ? normsProducer.ramBytesUsed() : 0) +
        ((fields!=null) ? fields.ramBytesUsed() : 0) + 
        ((fieldsReaderOrig!=null)? fieldsReaderOrig.ramBytesUsed() : 0) + 
        ((termVectorsReaderOrig!=null) ? termVectorsReaderOrig.ramBytesUsed() : 0);
  }
}
