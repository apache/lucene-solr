package org.apache.lucene.index;

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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.NormsReader;
import org.apache.lucene.index.codecs.PostingsFormat;
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.StoredFieldsReader;
import org.apache.lucene.index.codecs.TermVectorsReader;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
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
  
  final String segment;
  final FieldInfos fieldInfos;
  
  final FieldsProducer fields;
  final PerDocValues perDocProducer;
  final NormsReader norms;

  final Directory dir;
  final Directory cfsDir;
  final IOContext context;
  final int termsIndexDivisor;
  
  private final SegmentReader owner;
  
  StoredFieldsReader fieldsReaderOrig;
  TermVectorsReader termVectorsReaderOrig;
  CompoundFileDirectory cfsReader;
  CompoundFileDirectory storeCFSReader;

  
  
  SegmentCoreReaders(SegmentReader owner, Directory dir, SegmentInfo si, IOContext context, int termsIndexDivisor) throws IOException {
    
    if (termsIndexDivisor == 0) {
      throw new IllegalArgumentException("indexDivisor must be < 0 (don't load terms index) or greater than 0 (got 0)");
    }
    
    segment = si.name;
    final Codec codec = si.getCodec();
    this.context = context;
    this.dir = dir;
    
    boolean success = false;
    
    try {
      Directory dir0 = dir;
      if (si.getUseCompoundFile()) {
        cfsReader = new CompoundFileDirectory(dir, IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION), context, false);
        dir0 = cfsReader;
      }
      cfsDir = dir0;
      si.loadFieldInfos(cfsDir, false); // prevent opening the CFS to load fieldInfos
      fieldInfos = si.getFieldInfos();
      
      this.termsIndexDivisor = termsIndexDivisor;
      final PostingsFormat format = codec.postingsFormat();
      final SegmentReadState segmentReadState = new SegmentReadState(cfsDir, si, fieldInfos, context, termsIndexDivisor);
      // Ask codec for its Fields
      fields = format.fieldsProducer(segmentReadState);
      assert fields != null;
      // ask codec for its Norms: 
      // TODO: since we don't write any norms file if there are no norms,
      // kinda jaky to assume the codec handles the case of no norms file at all gracefully?!
      norms = codec.normsFormat().normsReader(cfsDir, si, fieldInfos, context, dir);
      perDocProducer = codec.docValuesFormat().docsProducer(segmentReadState);
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
  
  synchronized TermVectorsReader getTermVectorsReaderOrig() {
    return termVectorsReaderOrig;
  }
  
  synchronized StoredFieldsReader getFieldsReaderOrig() {
    return fieldsReaderOrig;
  }
  
  synchronized void incRef() {
    ref.incrementAndGet();
  }
  
  synchronized Directory getCFSReader() {
    return cfsReader;
  }
  
  synchronized void decRef() throws IOException {
    if (ref.decrementAndGet() == 0) {
      IOUtils.close(fields, perDocProducer, termVectorsReaderOrig,
          fieldsReaderOrig, cfsReader, storeCFSReader, norms);
      // Now, notify any ReaderFinished listeners:
      if (owner != null) {
        owner.notifyReaderFinishedListeners();
      }
    }
  }
  
  synchronized void openDocStores(SegmentInfo si) throws IOException {
    
    assert si.name.equals(segment);
    
    if (fieldsReaderOrig == null) {
      final Directory storeDir;
      if (si.getDocStoreOffset() != -1) {
        if (si.getDocStoreIsCompoundFile()) {
          assert storeCFSReader == null;
          storeCFSReader = new CompoundFileDirectory(dir,
              IndexFileNames.segmentFileName(si.getDocStoreSegment(), "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION),
              context, false);
          storeDir = storeCFSReader;
          assert storeDir != null;
        } else {
          storeDir = dir;
          assert storeDir != null;
        }
      } else if (si.getUseCompoundFile()) {
        // In some cases, we were originally opened when CFS
        // was not used, but then we are asked to open doc
        // stores after the segment has switched to CFS
        if (cfsReader == null) {
          cfsReader = new CompoundFileDirectory(dir,IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION), context, false);
        }
        storeDir = cfsReader;
        assert storeDir != null;
      } else {
        storeDir = dir;
        assert storeDir != null;
      }
      
      fieldsReaderOrig = si.getCodec().storedFieldsFormat().fieldsReader(storeDir, si, fieldInfos, context);
 
      if (si.getHasVectors()) { // open term vector files only as needed
        termVectorsReaderOrig = si.getCodec().termVectorsFormat().vectorsReader(storeDir, si, fieldInfos, context);
      }
    }
  }

  @Override
  public String toString() {
    return "SegmentCoreReader(owner=" + owner + ")";
  }
}
