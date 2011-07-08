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
import org.apache.lucene.index.codecs.FieldsProducer;
import org.apache.lucene.index.codecs.PerDocValues;
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

  final Directory dir;
  final Directory cfsDir;
  final IOContext context;
  final int termsIndexDivisor;
  
  private final SegmentReader owner;
  
  FieldsReader fieldsReaderOrig;
  TermVectorsReader termVectorsReaderOrig;
  CompoundFileDirectory cfsReader;
  CompoundFileDirectory storeCFSReader;

  
  
  SegmentCoreReaders(SegmentReader owner, Directory dir, SegmentInfo si, IOContext context, int termsIndexDivisor) throws IOException {
    
    if (termsIndexDivisor == 0) {
      throw new IllegalArgumentException("indexDivisor must be < 0 (don't load terms index) or greater than 0 (got 0)");
    }
    
    segment = si.name;
    final SegmentCodecs segmentCodecs = si.getSegmentCodecs();
    this.context = context;
    this.dir = dir;
    
    boolean success = false;
    
    try {
      Directory dir0 = dir;
      if (si.getUseCompoundFile()) {
        cfsReader = dir.openCompoundInput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION), context);
        dir0 = cfsReader;
      }
      cfsDir = dir0;
      si.loadFieldInfos(cfsDir, false); // prevent opening the CFS to load fieldInfos
      fieldInfos = si.getFieldInfos();
      
      this.termsIndexDivisor = termsIndexDivisor;
      final Codec codec = segmentCodecs.codec();
      final SegmentReadState segmentReadState = new SegmentReadState(cfsDir, si, fieldInfos, context, termsIndexDivisor);
      // Ask codec for its Fields
      fields = codec.fieldsProducer(segmentReadState);
      assert fields != null;
      perDocProducer = codec.docsProducer(segmentReadState);
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
  
  synchronized FieldsReader getFieldsReaderOrig() {
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
      IOUtils.closeSafely(false, fields, perDocProducer, termVectorsReaderOrig,
          fieldsReaderOrig, cfsReader, storeCFSReader);
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
          storeCFSReader = dir.openCompoundInput(
              IndexFileNames.segmentFileName(si.getDocStoreSegment(), "", IndexFileNames.COMPOUND_FILE_STORE_EXTENSION),
              context);
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
          cfsReader = dir.openCompoundInput(IndexFileNames.segmentFileName(segment, "", IndexFileNames.COMPOUND_FILE_EXTENSION), context);
        }
        storeDir = cfsReader;
        assert storeDir != null;
      } else {
        storeDir = dir;
        assert storeDir != null;
      }
      
      final String storesSegment = si.getDocStoreSegment();
      fieldsReaderOrig = new FieldsReader(storeDir, storesSegment, fieldInfos, context,
          si.getDocStoreOffset(), si.docCount);
      
      // Verify two sources of "maxDoc" agree:
      if (si.getDocStoreOffset() == -1 && fieldsReaderOrig.size() != si.docCount) {
        throw new CorruptIndexException("doc counts differ for segment " + segment + ": fieldsReader shows " + fieldsReaderOrig.size() + " but segmentInfo shows " + si.docCount);
      }
      
      if (si.getHasVectors()) { // open term vector files only as needed
        termVectorsReaderOrig = new TermVectorsReader(storeDir, storesSegment, fieldInfos, context, si.getDocStoreOffset(), si.docCount);
      }
    }
  }

  @Override
  public String toString() {
    return "SegmentCoreReader(owner=" + owner + ")";
  }
}
