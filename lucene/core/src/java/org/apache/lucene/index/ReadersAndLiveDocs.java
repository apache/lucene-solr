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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.MutableBits;

// Used by IndexWriter to hold open SegmentReaders (for
// searching or merging), plus pending deletes and updates,
// for a given segment
class ReadersAndLiveDocs { // TODO (DVU_RENAME) to ReaderAndUpdates
  // Not final because we replace (clone) when we need to
  // change it and it's been shared:
  public final SegmentInfoPerCommit info;

  // Tracks how many consumers are using this instance:
  private final AtomicInteger refCount = new AtomicInteger(1);

  private final IndexWriter writer;

  // Set once (null, and then maybe set, and never set again):
  private SegmentReader reader;

  // Holds the current shared (readable and writable)
  // liveDocs.  This is null when there are no deleted
  // docs, and it's copy-on-write (cloned whenever we need
  // to change it but it's been shared to an external NRT
  // reader).
  private Bits liveDocs;

  // Holds the numeric DocValues updates.
  private final Map<String,Map<Integer,Long>> numericUpdates = new HashMap<String,Map<Integer,Long>>();
  
  // How many further deletions we've done against
  // liveDocs vs when we loaded it or last wrote it:
  private int pendingDeleteCount;

  // True if the current liveDocs is referenced by an
  // external NRT reader:
  private boolean liveDocsShared;

  // Indicates whether this segment is currently being merged. While a segment
  // is merging, all field updates are also registered in the mergingUpdates
  // map. Also, calls to writeLiveDocs merge the updates with mergingUpdates.
  // That way, when the segment is done merging, IndexWriter can apply the
  // updates on the merged segment too.
  private boolean isMerging = false;
  
  // Holds any updates that come through while this segment was being merged.
  private final Map<String,Map<Integer,Long>> mergingUpdates = new HashMap<String,Map<Integer,Long>>();

  public ReadersAndLiveDocs(IndexWriter writer, SegmentInfoPerCommit info) {
    this.info = info;
    this.writer = writer;
    liveDocsShared = true;
  }

  public void incRef() {
    final int rc = refCount.incrementAndGet();
    assert rc > 1;
  }

  public void decRef() {
    final int rc = refCount.decrementAndGet();
    assert rc >= 0;
  }

  public int refCount() {
    final int rc = refCount.get();
    assert rc >= 0;
    return rc;
  }

  public synchronized int getPendingDeleteCount() {
    return pendingDeleteCount;
  }
  
  public synchronized boolean hasFieldUpdates() {
    return numericUpdates.size() > 0;
  }
  
  public synchronized int getPendingUpdatesCount() {
    int pendingUpdatesCount = 0;
    for (Entry<String,Map<Integer,Long>> e : numericUpdates.entrySet()) {
      pendingUpdatesCount += e.getValue().size();
    }
    return pendingUpdatesCount;
  }
  
  // Call only from assert!
  public synchronized boolean verifyDocCounts() {
    int count;
    if (liveDocs != null) {
      count = 0;
      for(int docID=0;docID<info.info.getDocCount();docID++) {
        if (liveDocs.get(docID)) {
          count++;
        }
      }
    } else {
      count = info.info.getDocCount();
    }

    assert info.info.getDocCount() - info.getDelCount() - pendingDeleteCount == count: "info.docCount=" + info.info.getDocCount() + " info.getDelCount()=" + info.getDelCount() + " pendingDeleteCount=" + pendingDeleteCount + " count=" + count;
    return true;
  }

  public synchronized void reopenReader(IOContext context) throws IOException {
    if (reader != null) {
      SegmentReader newReader = new SegmentReader(info, reader, liveDocs, info.info.getDocCount() - info.getDelCount() - pendingDeleteCount);
      boolean reopened = false;
      try {
        reader.decRef();
        reader = newReader;
        reopened = true;
      } finally {
        if (!reopened) {
          newReader.decRef();
        }
      }
    }
  }
  
  private synchronized SegmentReader doGetReader(IOContext context) throws IOException {
    if (reader == null) {
      // We steal returned ref:
      reader = new SegmentReader(info, context);
      if (liveDocs == null) {
        liveDocs = reader.getLiveDocs();
      }
    }
    
    // Ref for caller
    reader.incRef();
    return reader;
  }
  
  private synchronized SegmentReader doGetReaderWithUpdates(IOContext context) throws IOException {
    assert Thread.holdsLock(writer); // when we get here, we should already have the writer lock
    boolean checkpoint = false;
    try {
      checkpoint = writeLiveDocs(info.info.dir);
      if (reader == null) {
        // We steal returned ref:
//        System.out.println("[" + Thread.currentThread().getName() + "] RLD.doGetReaderWithUpdates: newSR " + info);
        reader = new SegmentReader(info, context);
        if (liveDocs == null) {
          liveDocs = reader.getLiveDocs();
        }
      } else if (checkpoint) {
        // enroll a new reader with the applied updates
//        System.out.println("[" + Thread.currentThread().getName() + "] RLD.doGetReaderWithUpdates: reopenReader " + info);
        reopenReader(context);
      }
      
      // Ref for caller
      reader.incRef();
      return reader;
    } finally {
      if (checkpoint) {
        writer.checkpoint();
      }
    }
  }
  
  /** Returns a {@link SegmentReader} while applying field updates if requested. */
  public SegmentReader getReader(boolean applyFieldUpdates, IOContext context) throws IOException {
    // if we need to apply field updates, we call writeLiveDocs and change
    // SegmentInfos. Therefore must hold the lock on IndexWriter. This code
    // ensures that readers that don't need to apply updates don't pay the
    // cost of obtaining it.
    if (applyFieldUpdates && hasFieldUpdates()) {
      synchronized (writer) {
//        System.out.println("[" + Thread.currentThread().getName() + "] RLD.getReader: getReaderWithUpdates " + info);
        return doGetReaderWithUpdates(context);
      }
    } else {
//      System.out.println("[" + Thread.currentThread().getName() + "] RLD.getReader: getReader no updates " + info);
      return doGetReader(context);
    }
  }
  
  public synchronized void release(SegmentReader sr) throws IOException {
    assert info == sr.getSegmentInfo();
    sr.decRef();
  }

  /**
   * Updates the numeric doc value of {@code docID} under {@code field} to the
   * given {@code value}.
   */
  public synchronized void updateNumericDocValue(String field, int docID, Long value) {
    assert Thread.holdsLock(writer);
    assert docID >= 0 && docID < reader.maxDoc() : "out of bounds: docid=" + docID + " maxDoc=" + reader.maxDoc() + " seg=" + info.info.name + " docCount=" + info.info.getDocCount();
    Map<Integer,Long> updates = numericUpdates.get(field);
    if (updates == null) {
      updates = new HashMap<Integer,Long>();
      numericUpdates.put(field, updates);
    }
    updates.put(docID, value);
  }
  
  public synchronized boolean delete(int docID) {
    assert liveDocs != null;
    assert Thread.holdsLock(writer);
    assert docID >= 0 && docID < liveDocs.length() : "out of bounds: docid=" + docID + " liveDocsLength=" + liveDocs.length() + " seg=" + info.info.name + " docCount=" + info.info.getDocCount();
    assert !liveDocsShared;
    final boolean didDelete = liveDocs.get(docID);
    if (didDelete) {
      ((MutableBits) liveDocs).clear(docID);
      pendingDeleteCount++;
      //System.out.println("  new del seg=" + info + " docID=" + docID + " pendingDelCount=" + pendingDeleteCount + " totDelCount=" + (info.docCount-liveDocs.count()));
    }
    return didDelete;
  }

  // NOTE: removes callers ref
  public synchronized void dropReaders() throws IOException {
    // TODO: can we somehow use IOUtils here...?  problem is
    // we are calling .decRef not .close)...
    if (reader != null) {
      //System.out.println("  pool.drop info=" + info + " rc=" + reader.getRefCount());
      try {
        reader.decRef();
      } finally {
        reader = null;
      }
    }

    decRef();
  }

  /**
   * Returns a ref to a clone.  NOTE: this clone is not
   * enrolled in the pool, so you should simply close()
   * it when you're done (ie, do not call release()).
   */
  public synchronized SegmentReader getReadOnlyClone(IOContext context) throws IOException {
    getReader(true, context).decRef(); // make sure we enroll a new reader if there are field updates
    assert reader != null;
    liveDocsShared = true;
    if (liveDocs != null) {
      return new SegmentReader(reader.getSegmentInfo(), reader, liveDocs, info.info.getDocCount() - info.getDelCount() - pendingDeleteCount);
    } else {
      assert reader.getLiveDocs() == liveDocs;
      reader.incRef();
      return reader;
    }
  }

  public synchronized void initWritableLiveDocs() throws IOException {
    assert Thread.holdsLock(writer);
    assert info.info.getDocCount() > 0;
    //System.out.println("initWritableLivedocs seg=" + info + " liveDocs=" + liveDocs + " shared=" + shared);
    if (liveDocsShared) {
      // Copy on write: this means we've cloned a
      // SegmentReader sharing the current liveDocs
      // instance; must now make a private clone so we can
      // change it:
      LiveDocsFormat liveDocsFormat = info.info.getCodec().liveDocsFormat();
      if (liveDocs == null) {
        //System.out.println("create BV seg=" + info);
        liveDocs = liveDocsFormat.newLiveDocs(info.info.getDocCount());
      } else {
        liveDocs = liveDocsFormat.newLiveDocs(liveDocs);
      }
      liveDocsShared = false;
    }
  }

  public synchronized Bits getLiveDocs() {
    assert Thread.holdsLock(writer);
    return liveDocs;
  }

  public synchronized Bits getReadOnlyLiveDocs() {
    //System.out.println("getROLiveDocs seg=" + info);
    assert Thread.holdsLock(writer);
    liveDocsShared = true;
    //if (liveDocs != null) {
    //System.out.println("  liveCount=" + liveDocs.count());
    //}
    return liveDocs;
  }

  public synchronized void dropChanges() {
    // Discard (don't save) changes when we are dropping
    // the reader; this is used only on the sub-readers
    // after a successful merge.  If deletes had
    // accumulated on those sub-readers while the merge
    // is running, by now we have carried forward those
    // deletes onto the newly merged segment, so we can
    // discard them on the sub-readers:
    pendingDeleteCount = 0;
    numericUpdates.clear();
    dropMergingUpdates();
  }

  // Commit live docs (writes new _X_N.del files) and field updates (writes new
  // _X_N updates files) to the directory; returns true if it wrote any file
  // and false if there were no new deletes or updates to write:
  // TODO (DVU_RENAME) to writeDeletesAndUpdates
  public synchronized boolean writeLiveDocs(Directory dir) throws IOException {
    assert Thread.holdsLock(writer);
    //System.out.println("rld.writeLiveDocs seg=" + info + " pendingDelCount=" + pendingDeleteCount + " numericUpdates=" + numericUpdates);
    final boolean hasFieldUpdates = hasFieldUpdates();
    if (pendingDeleteCount == 0 && !hasFieldUpdates) {
      return false;
    }
    
    // We have new deletes or updates
    assert pendingDeleteCount == 0 || liveDocs.length() == info.info.getDocCount();
    
    // Do this so we can delete any created files on
    // exception; this saves all codecs from having to do
    // it:
    TrackingDirectoryWrapper trackingDir = new TrackingDirectoryWrapper(dir);
    
    // We can write directly to the actual name (vs to a
    // .tmp & renaming it) because the file is not live
    // until segments file is written:
    FieldInfos fieldInfos = null;
    boolean success = false;
    try {
      Codec codec = info.info.getCodec();
      if (pendingDeleteCount > 0) {
        codec.liveDocsFormat().writeLiveDocs((MutableBits)liveDocs, trackingDir, info, pendingDeleteCount, IOContext.DEFAULT);
      }
      
      // apply numeric updates if there are any
      if (hasFieldUpdates) {
        // reader could be null e.g. for a just merged segment (from
        // IndexWriter.commitMergedDeletes).
//        if (this.reader == null) System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeLiveDocs: newSR " + info);
        final SegmentReader reader = this.reader == null ? new SegmentReader(info, IOContext.READONCE) : this.reader;
        try {
          // clone FieldInfos so that we can update their dvGen separately from
          // the reader's infos and write them to a new fieldInfos_gen file
          FieldInfos.Builder builder = new FieldInfos.Builder(writer.globalFieldNumberMap);
          // cannot use builder.add(reader.getFieldInfos()) because it does not
          // clone FI.attributes as well FI.dvGen
          for (FieldInfo fi : reader.getFieldInfos()) {
            FieldInfo clone = builder.add(fi);
            // copy the stuff FieldInfos.Builder doesn't copy
            if (fi.attributes() != null) {
              for (Entry<String,String> e : fi.attributes().entrySet()) {
                clone.putAttribute(e.getKey(), e.getValue());
              }
            }
            clone.setDocValuesGen(fi.getDocValuesGen());
          }
          // create new fields or update existing ones to have NumericDV type
          for (String f : numericUpdates.keySet()) {
            builder.addOrUpdate(f, NumericDocValuesField.TYPE);
          }
          
          fieldInfos = builder.finish();
          final long nextFieldInfosGen = info.getNextFieldInfosGen();
          final String segmentSuffix = Long.toString(nextFieldInfosGen, Character.MAX_RADIX);
          final SegmentWriteState state = new SegmentWriteState(null, trackingDir, info.info, fieldInfos, null, IOContext.DEFAULT, segmentSuffix);
          final DocValuesFormat docValuesFormat = codec.docValuesFormat();
          final DocValuesConsumer fieldsConsumer = docValuesFormat.fieldsConsumer(state);
          boolean fieldsConsumerSuccess = false;
          try {
//            System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeLiveDocs: applying updates; seg=" + info + " updates=" + numericUpdates);
            for (Entry<String,Map<Integer,Long>> e : numericUpdates.entrySet()) {
              final String field = e.getKey();
              final Map<Integer,Long> updates = e.getValue();
              final FieldInfo fieldInfo = fieldInfos.fieldInfo(field);

              assert fieldInfo != null;

              fieldInfo.setDocValuesGen(nextFieldInfosGen);
              
              // write the numeric updates to a new gen'd docvalues file
              fieldsConsumer.addNumericField(fieldInfo, new Iterable<Number>() {
                @SuppressWarnings("synthetic-access")
                final NumericDocValues currentValues = reader.getNumericDocValues(field);
                final Bits docsWithField = reader.getDocsWithField(field);
                @Override
                public Iterator<Number> iterator() {
                  return new Iterator<Number>() {

                    @SuppressWarnings("synthetic-access")
                    final int maxDoc = reader.maxDoc();
                    int curDoc = -1;
                    
                    @Override
                    public boolean hasNext() {
                      return curDoc < maxDoc - 1;
                    }

                    @Override
                    public Number next() {
                      if (++curDoc >= maxDoc) {
                        throw new NoSuchElementException("no more documents to return values for");
                      }
                      Long updatedValue = updates.get(curDoc);
                      if (updatedValue == null) {
                        // only read the current value if the document had a value before
                        if (currentValues != null && docsWithField.get(curDoc)) {
                          updatedValue = currentValues.get(curDoc);
                        }
                      } else if (updatedValue == NumericUpdate.MISSING) {
                        updatedValue = null;
                      }
                      return updatedValue;
                    }

                    @Override
                    public void remove() {
                      throw new UnsupportedOperationException("this iterator does not support removing elements");
                    }
                    
                  };
                }
              });
            }
            
            codec.fieldInfosFormat().getFieldInfosWriter().write(trackingDir, info.info.name, segmentSuffix, fieldInfos, IOContext.DEFAULT);
            fieldsConsumerSuccess = true;
          } finally {
            if (fieldsConsumerSuccess) {
              fieldsConsumer.close();
            } else {
              IOUtils.closeWhileHandlingException(fieldsConsumer);
            }
          }
        } finally {
          if (reader != this.reader) {
//            System.out.println("[" + Thread.currentThread().getName() + "] RLD.writeLiveDocs: closeReader " + reader);
            reader.close();
          }
        }
      }
      success = true;
    } finally {
      if (!success) {
        // Advance only the nextWriteDelGen so that a 2nd
        // attempt to write will write to a new file
        if (pendingDeleteCount > 0) {
          info.advanceNextWriteDelGen();
        }
        
        // Advance only the nextWriteDocValuesGen so that a 2nd
        // attempt to write will write to a new file
        if (hasFieldUpdates) {
          info.advanceNextWriteFieldInfosGen();
        }
        
        // Delete any partially created file(s):
        for (String fileName : trackingDir.getCreatedFiles()) {
          try {
            dir.deleteFile(fileName);
          } catch (Throwable t) {
            // Ignore so we throw only the first exc
          }
        }
      }
    }
    
    // If we hit an exc in the line above (eg disk full)
    // then info's delGen remains pointing to the previous
    // (successfully written) del docs:
    if (pendingDeleteCount > 0) {
      info.advanceDelGen();
      info.setDelCount(info.getDelCount() + pendingDeleteCount);
      pendingDeleteCount = 0;
    }
    
    if (hasFieldUpdates) {
      info.advanceFieldInfosGen();
      // copy all the updates to mergingUpdates, so they can later be applied to the merged segment
      if (isMerging) {
        copyUpdatesToMerging();
      }
      numericUpdates.clear();
      
      // create a new map, keeping only the gens that are in use
      Map<Long,Set<String>> genUpdatesFiles = info.getUpdatesFiles();
      Map<Long,Set<String>> newGenUpdatesFiles = new HashMap<Long,Set<String>>();
      final long fieldInfosGen = info.getFieldInfosGen();
      for (FieldInfo fi : fieldInfos) {
        long dvGen = fi.getDocValuesGen();
        if (dvGen != -1 && !newGenUpdatesFiles.containsKey(dvGen)) {
          if (dvGen == fieldInfosGen) {
            newGenUpdatesFiles.put(fieldInfosGen, trackingDir.getCreatedFiles());
          } else {
            newGenUpdatesFiles.put(dvGen, genUpdatesFiles.get(dvGen));
          }
        }
      }
      
      info.setGenUpdatesFiles(newGenUpdatesFiles);
    }

    return true;
  }

  private void copyUpdatesToMerging() {
//    System.out.println("[" + Thread.currentThread().getName() + "] RLD.copyUpdatesToMerging: " + numericUpdates);
    // cannot do a simple putAll, even if mergingUpdates is empty, because we
    // need a shallow copy of the values (maps)
    for (Entry<String,Map<Integer,Long>> e : numericUpdates.entrySet()) {
      String field = e.getKey();
      Map<Integer,Long> merging = mergingUpdates.get(field);
      if (merging == null) {
        mergingUpdates.put(field, new HashMap<Integer,Long>(e.getValue()));
      } else {
        merging.putAll(e.getValue());
      }
    }
  }
  
  /**
   * Returns a reader for merge. This method applies field updates if there are
   * any and marks that this segment is currently merging.
   */
  SegmentReader getReaderForMerge(IOContext context) throws IOException {
    // lock ordering must be IW -> RLD, otherwise could cause deadlocks
    synchronized (writer) {
      synchronized (this) {
        // must execute these two statements as atomic operation, otherwise we
        // could lose updates if e.g. another thread calls writeLiveDocs in
        // between, or the updates are applied to the obtained reader, but then
        // re-applied in IW.commitMergedDeletes (unnecessary work and potential
        // bugs.
        isMerging = true;
        return getReader(true, context);
      }
    }
  }
  
  /**
   * Drops all merging updates. Called from IndexWriter after this segment
   * finished merging (whether successfully or not).
   */
  public synchronized void dropMergingUpdates() {
    mergingUpdates.clear();
    isMerging = false;
  }
  
  /**
   * Called from IndexWriter after applying deletes to the merged segment, while
   * it was being merged.
   */
  public synchronized void setMergingUpdates(Map<Integer,Map<String,Long>> updates) {
    for (Entry<Integer,Map<String,Long>> e : updates.entrySet()) {
      int doc = e.getKey().intValue();
      for (Entry<String,Long> docUpdates : e.getValue().entrySet()) {
        String field = docUpdates.getKey();
        Long value = docUpdates.getValue();
        Map<Integer,Long> fieldUpdates = numericUpdates.get(field);
        if (fieldUpdates == null) {
          fieldUpdates = new HashMap<Integer,Long>();
          numericUpdates.put(field, fieldUpdates);
        }
        fieldUpdates.put(doc, value);
      }
    }
  }
  
  /** Returns updates that came in while this segment was merging. */
  public synchronized Map<Integer,Map<String,Long>> getMergingUpdates() {
    copyUpdatesToMerging();
    if (mergingUpdates.isEmpty()) {
      return null;
    }
    
    Map<Integer,Map<String,Long>> updates = new HashMap<Integer,Map<String,Long>>();
    for (Entry<String,Map<Integer,Long>> e : mergingUpdates.entrySet()) {
      String field = e.getKey();
      for (Entry<Integer,Long> fieldUpdates : e.getValue().entrySet()) {
        Integer doc = fieldUpdates.getKey();
        Long value = fieldUpdates.getValue();
        Map<String,Long> docUpdates = updates.get(doc);
        if (docUpdates == null) {
          docUpdates = new HashMap<String,Long>();
          updates.put(doc, docUpdates);
        }
        docUpdates.put(field, value);
      }
    }
    
    mergingUpdates.clear();
    return updates;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReadersAndLiveDocs(seg=").append(info);
    sb.append(" pendingDeleteCount=").append(pendingDeleteCount);
    sb.append(" liveDocsShared=").append(liveDocsShared);
    sb.append(" pendingUpdatesCount=").append(getPendingUpdatesCount());
    return sb.toString();
  }
  
}
