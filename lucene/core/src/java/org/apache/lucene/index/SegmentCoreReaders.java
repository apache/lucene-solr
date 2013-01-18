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
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.SegmentReader.CoreClosedListener;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;

/** Holds core readers that are shared (unchanged) when
 * SegmentReader is cloned or reopened */
final class SegmentCoreReaders {
  
  // Counts how many other reader share the core objects
  // (freqStream, proxStream, tis, etc.) of this reader;
  // when coreRef drops to 0, these core objects may be
  // closed.  A given instance of SegmentReader may be
  // closed, even those it shares core objects with other
  // SegmentReaders:
  private final AtomicInteger ref = new AtomicInteger(1);
  
  final FieldInfos fieldInfos;
  
  final FieldsProducer fields;
  final DocValuesProducer simpleDVProducer;
  final DocValuesProducer simpleNormsProducer;

  final int termsIndexDivisor;
  
  private final SegmentReader owner;
  
  final StoredFieldsReader fieldsReaderOrig;
  final TermVectorsReader termVectorsReaderOrig;
  final CompoundFileDirectory cfsReader;

  // TODO: make a single thread local w/ a
  // Thingy class holding fieldsReader, termVectorsReader,
  // simpleNormsProducer, simpleDVProducer

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

  final CloseableThreadLocal<Map<String,Object>> simpleDocValuesLocal = new CloseableThreadLocal<Map<String,Object>>() {
    @Override
    protected Map<String,Object> initialValue() {
      return new HashMap<String,Object>();
    }
  };

  final CloseableThreadLocal<Map<String,Object>> simpleNormsLocal = new CloseableThreadLocal<Map<String,Object>>() {
    @Override
    protected Map<String,Object> initialValue() {
      return new HashMap<String,Object>();
    }
  };

  // nocommit norms too
  
  private final Set<CoreClosedListener> coreClosedListeners = 
      Collections.synchronizedSet(new LinkedHashSet<CoreClosedListener>());
  
  SegmentCoreReaders(SegmentReader owner, Directory dir, SegmentInfoPerCommit si, IOContext context, int termsIndexDivisor) throws IOException {
    
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
      fieldInfos = codec.fieldInfosFormat().getFieldInfosReader().read(cfsDir, si.info.name, IOContext.READONCE);

      this.termsIndexDivisor = termsIndexDivisor;
      final PostingsFormat format = codec.postingsFormat();
      final SegmentReadState segmentReadState = new SegmentReadState(cfsDir, si.info, fieldInfos, context, termsIndexDivisor);
      // Ask codec for its Fields
      fields = format.fieldsProducer(segmentReadState);
      assert fields != null;
      // ask codec for its Norms: 
      // TODO: since we don't write any norms file if there are no norms,
      // kinda jaky to assume the codec handles the case of no norms file at all gracefully?!
      // nocommit shouldn't need null check:
      if (codec.docValuesFormat() != null) {
        if (fieldInfos.hasDocValues()) {
          simpleDVProducer = codec.docValuesFormat().fieldsProducer(segmentReadState);
        } else {
          simpleDVProducer = null;
        }
      } else {
        simpleDVProducer = null;
      }
      // nocommit shouldn't need null check:
      if (codec.normsFormat() != null) {
        if (fieldInfos.hasNorms()) {
          simpleNormsProducer = codec.normsFormat().normsProducer(segmentReadState);
        } else {
          simpleNormsProducer = null;
        }
      } else {
        simpleNormsProducer = null;
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
    
    // Must assign this at the end -- if we hit an
    // exception above core, we don't want to attempt to
    // purge the FieldCache (will hit NPE because core is
    // not assigned yet).
    this.owner = owner;
  }
  
  void incRef() {
    ref.incrementAndGet();
  }

  NumericDocValues getNumericDocValues(String field) throws IOException {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == null) {
      // Field was not indexed with doc values
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.NUMERIC) {
      // DocValues were not numeric
      return null;
    }

    // nocommit change to assert != null!!
    if (simpleDVProducer == null) {
      return null;
    }

    Map<String,Object> dvFields = simpleDocValuesLocal.get();

    NumericDocValues dvs = (NumericDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = simpleDVProducer.getNumeric(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  BinaryDocValues getBinaryDocValues(String field) throws IOException {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == null) {
      // Field was not indexed with doc values
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.BINARY) {
      // DocValues were not binary
      return null;
    }

    // nocommit change to assert != null!!
    if (simpleDVProducer == null) {
      return null;
    }

    Map<String,Object> dvFields = simpleDocValuesLocal.get();

    BinaryDocValues dvs = (BinaryDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = simpleDVProducer.getBinary(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  SortedDocValues getSortedDocValues(String field) throws IOException {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (fi.getDocValuesType() == null) {
      // Field was not indexed with doc values
      return null;
    }
    if (fi.getDocValuesType() != DocValuesType.SORTED) {
      // DocValues were not sorted
      return null;
    }

    // nocommit change to assert != null!!
    if (simpleDVProducer == null) {
      return null;
    }

    Map<String,Object> dvFields = simpleDocValuesLocal.get();

    SortedDocValues dvs = (SortedDocValues) dvFields.get(field);
    if (dvs == null) {
      dvs = simpleDVProducer.getSorted(fi);
      dvFields.put(field, dvs);
    }

    return dvs;
  }

  NumericDocValues getSimpleNormValues(String field) throws IOException {
    FieldInfo fi = fieldInfos.fieldInfo(field);
    if (fi == null) {
      // Field does not exist
      return null;
    }
    if (!fi.hasNorms()) {
      return null;
    }
    // nocommit change to assert != null!!
    if (simpleNormsProducer == null) {
      return null;
    }

    Map<String,Object> normFields = simpleNormsLocal.get();

    NumericDocValues norms = (NumericDocValues) normFields.get(field);
    if (norms == null) {
      norms = simpleNormsProducer.getNumeric(fi);
      normFields.put(field, norms);
    }

    return norms;
  }

  void decRef() throws IOException {
    if (ref.decrementAndGet() == 0) {
      IOUtils.close(termVectorsLocal, fieldsReaderLocal, simpleDocValuesLocal, simpleNormsLocal, fields, simpleDVProducer,
                    termVectorsReaderOrig, fieldsReaderOrig, cfsReader, simpleNormsProducer);
      notifyCoreClosedListeners();
    }
  }
  
  private void notifyCoreClosedListeners() {
    synchronized(coreClosedListeners) {
      for (CoreClosedListener listener : coreClosedListeners) {
        listener.onClose(owner);
      }
    }
  }

  void addCoreClosedListener(CoreClosedListener listener) {
    coreClosedListeners.add(listener);
  }
  
  void removeCoreClosedListener(CoreClosedListener listener) {
    coreClosedListeners.remove(listener);
  }

  @Override
  public String toString() {
    return "SegmentCoreReader(owner=" + owner + ")";
  }
}
