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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.PerDocConsumer;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ReaderUtil;

/**
 * The SegmentMerger class combines two or more Segments, represented by an IndexReader ({@link #add},
 * into a single Segment.  After adding the appropriate readers, call the merge method to combine the
 * segments.
 *
 * @see #merge
 * @see #add
 */
final class SegmentMerger {
  private final Directory directory;
  private final String segment;
  private final int termIndexInterval;

  private final Codec codec;
  
  private final IOContext context;
  
  private final MergeState mergeState = new MergeState();

  SegmentMerger(InfoStream infoStream, Directory dir, int termIndexInterval, String name, MergeState.CheckAbort checkAbort, PayloadProcessorProvider payloadProcessorProvider, FieldInfos fieldInfos, Codec codec, IOContext context) {
    mergeState.infoStream = infoStream;
    mergeState.readers = new ArrayList<MergeState.IndexReaderAndLiveDocs>();
    mergeState.fieldInfos = fieldInfos;
    mergeState.checkAbort = checkAbort;
    mergeState.payloadProcessorProvider = payloadProcessorProvider;
    directory = dir;
    segment = name;
    this.termIndexInterval = termIndexInterval;
    this.codec = codec;
    this.context = context;
  }

  /**
   * Add an IndexReader to the collection of readers that are to be merged
   * @param reader
   */
  final void add(IndexReader reader) {
    try {
      new ReaderUtil.Gather(reader) {
        @Override
        protected void add(int base, AtomicReader r) {
          mergeState.readers.add(new MergeState.IndexReaderAndLiveDocs(r, r.getLiveDocs()));
        }
      }.run();
    } catch (IOException ioe) {
      // won't happen
      throw new RuntimeException(ioe);
    }
  }

  final void add(SegmentReader reader, Bits liveDocs) {
    mergeState.readers.add(new MergeState.IndexReaderAndLiveDocs(reader, liveDocs));
  }

  /**
   * Merges the readers specified by the {@link #add} method into the directory passed to the constructor
   * @return The number of documents that were merged
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  final MergeState merge() throws CorruptIndexException, IOException {
    // NOTE: it's important to add calls to
    // checkAbort.work(...) if you make any changes to this
    // method that will spend alot of time.  The frequency
    // of this check impacts how long
    // IndexWriter.close(false) takes to actually stop the
    // threads.
    
    mergeState.mergedDocCount = setDocMaps();

    mergeFieldInfos();
    setMatchingSegmentReaders();
    int numMerged = mergeFields();
    assert numMerged == mergeState.mergedDocCount;

    final SegmentWriteState segmentWriteState = new SegmentWriteState(mergeState.infoStream, directory, segment, mergeState.fieldInfos, mergeState.mergedDocCount, termIndexInterval, codec, null, context);
    mergeTerms(segmentWriteState);
    mergePerDoc(segmentWriteState);
    
    if (mergeState.fieldInfos.hasNorms()) {
      mergeNorms(segmentWriteState);
    }

    if (mergeState.fieldInfos.hasVectors()) {
      numMerged = mergeVectors();
      assert numMerged == mergeState.mergedDocCount;
    }

    return mergeState;
  }

  private void setMatchingSegmentReaders() {
    // If the i'th reader is a SegmentReader and has
    // identical fieldName -> number mapping, then this
    // array will be non-null at position i:
    int numReaders = mergeState.readers.size();
    mergeState.matchingSegmentReaders = new SegmentReader[numReaders];

    // If this reader is a SegmentReader, and all of its
    // field name -> number mappings match the "merged"
    // FieldInfos, then we can do a bulk copy of the
    // stored fields:
    for (int i = 0; i < numReaders; i++) {
      MergeState.IndexReaderAndLiveDocs reader = mergeState.readers.get(i);
      // TODO: we may be able to broaden this to
      // non-SegmentReaders, since FieldInfos is now
      // required?  But... this'd also require exposing
      // bulk-copy (TVs and stored fields) API in foreign
      // readers..
      if (reader.reader instanceof SegmentReader) {
        SegmentReader segmentReader = (SegmentReader) reader.reader;
        boolean same = true;
        FieldInfos segmentFieldInfos = segmentReader.getFieldInfos();
        for (FieldInfo fi : segmentFieldInfos) {
          if (!mergeState.fieldInfos.fieldName(fi.number).equals(fi.name)) {
            same = false;
            break;
          }
        }
        if (same) {
          mergeState.matchingSegmentReaders[i] = segmentReader;
          mergeState.matchedCount++;
        }
      }
    }

    if (mergeState.infoStream.isEnabled("SM")) {
      mergeState.infoStream.message("SM", "merge store matchedCount=" + mergeState.matchedCount + " vs " + mergeState.readers.size());
      if (mergeState.matchedCount != mergeState.readers.size()) {
        mergeState.infoStream.message("SM", "" + (mergeState.readers.size() - mergeState.matchedCount) + " non-bulk merges");
      }
    }
  }
  
  // returns an updated typepromoter (tracking type and size) given a previous one,
  // and a newly encountered docvalues
  private TypePromoter mergeDocValuesType(TypePromoter previous, DocValues docValues) {
    TypePromoter incoming = TypePromoter.create(docValues.getType(),  docValues.getValueSize());
    if (previous == null) {
      previous = TypePromoter.getIdentityPromoter();
    }
    TypePromoter promoted = previous.promote(incoming);
    if (promoted == null) {
      // type is incompatible: promote to BYTES_VAR_STRAIGHT
      return TypePromoter.create(DocValues.Type.BYTES_VAR_STRAIGHT, TypePromoter.VAR_TYPE_VALUE_SIZE);
    } else {
      return promoted;
    }
  }

  private void mergeFieldInfos() throws IOException {
    mergeDocValuesAndNormsFieldInfos();
    // write the merged infos
    FieldInfosWriter fieldInfosWriter = codec.fieldInfosFormat()
        .getFieldInfosWriter();
    fieldInfosWriter.write(directory, segment, mergeState.fieldInfos, context);
  }

  public void mergeDocValuesAndNormsFieldInfos() throws IOException {
    // mapping from all docvalues fields found to their promoted types
    // this is because FieldInfos does not store the valueSize
    Map<FieldInfo,TypePromoter> docValuesTypes = new HashMap<FieldInfo,TypePromoter>();
    Map<FieldInfo,TypePromoter> normValuesTypes = new HashMap<FieldInfo,TypePromoter>();

    for (MergeState.IndexReaderAndLiveDocs readerAndLiveDocs : mergeState.readers) {
      final AtomicReader reader = readerAndLiveDocs.reader;
      FieldInfos readerFieldInfos = reader.getFieldInfos();
      for (FieldInfo fi : readerFieldInfos) {
        FieldInfo merged = mergeState.fieldInfos.add(fi);
        // update the type promotion mapping for this reader
        if (fi.hasDocValues()) {
          TypePromoter previous = docValuesTypes.get(merged);
          docValuesTypes.put(merged, mergeDocValuesType(previous, reader.docValues(fi.name))); 
        }
        if (fi.hasNorms()) {
          TypePromoter previous = normValuesTypes.get(merged);
          normValuesTypes.put(merged, mergeDocValuesType(previous, reader.normValues(fi.name))); 
        }
      }
    }
    updatePromoted(normValuesTypes, true);
    updatePromoted(docValuesTypes, false);
  }
  
  protected void updatePromoted(Map<FieldInfo,TypePromoter> infoAndPromoter, boolean norms) {
    // update any promoted doc values types:
    for (Map.Entry<FieldInfo,TypePromoter> e : infoAndPromoter.entrySet()) {
      FieldInfo fi = e.getKey();
      TypePromoter promoter = e.getValue();
      if (promoter == null) {
        if (norms) {
          fi.setNormValueType(null, true);
        } else {
          fi.setDocValuesType(null, true);
        }
      } else {
        assert promoter != TypePromoter.getIdentityPromoter();
        if (norms) {
          if (fi.getNormType() != promoter.type()) {
            // reset the type if we got promoted
            fi.setNormValueType(promoter.type(), true);
          }  
        } else {
          if (fi.getDocValuesType() != promoter.type()) {
            // reset the type if we got promoted
            fi.setDocValuesType(promoter.type(), true);
          }
        }
      }
    }
  }


  /**
   *
   * @return The number of documents in all of the readers
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   */
  private int mergeFields() throws CorruptIndexException, IOException {
    final StoredFieldsWriter fieldsWriter = codec.storedFieldsFormat().fieldsWriter(directory, segment, context);
    
    try {
      return fieldsWriter.merge(mergeState);
    } finally {
      fieldsWriter.close();
    }
  }

  /**
   * Merge the TermVectors from each of the segments into the new one.
   * @throws IOException
   */
  private final int mergeVectors() throws IOException {
    final TermVectorsWriter termVectorsWriter = codec.termVectorsFormat().vectorsWriter(directory, segment, context);
    
    try {
      return termVectorsWriter.merge(mergeState);
    } finally {
      termVectorsWriter.close();
    }
  }

  // NOTE: removes any "all deleted" readers from mergeState.readers
  private int setDocMaps() throws IOException {
    final int numReaders = mergeState.readers.size();

    // Remap docIDs
    mergeState.docMaps = new int[numReaders][];
    mergeState.docBase = new int[numReaders];
    mergeState.readerPayloadProcessor = new PayloadProcessorProvider.ReaderPayloadProcessor[numReaders];
    mergeState.currentPayloadProcessor = new PayloadProcessorProvider.PayloadProcessor[numReaders];

    int docBase = 0;

    int i = 0;
    while(i < mergeState.readers.size()) {

      final MergeState.IndexReaderAndLiveDocs reader = mergeState.readers.get(i);

      mergeState.docBase[i] = docBase;
      final int maxDoc = reader.reader.maxDoc();
      final int docCount;
      final Bits liveDocs = reader.liveDocs;
      final int[] docMap;
      if (liveDocs != null) {
        int delCount = 0;
        docMap = new int[maxDoc];
        int newDocID = 0;
        for(int j=0;j<maxDoc;j++) {
          if (!liveDocs.get(j)) {
            docMap[j] = -1;
            delCount++;
          } else {
            docMap[j] = newDocID++;
          }
        }
        docCount = maxDoc - delCount;
      } else {
        docCount = maxDoc;
        docMap = null;
      }

      mergeState.docMaps[i] = docMap;
      docBase += docCount;

      if (mergeState.payloadProcessorProvider != null) {
        mergeState.readerPayloadProcessor[i] = mergeState.payloadProcessorProvider.getReaderProcessor(reader.reader);
      }

      i++;
    }

    return docBase;
  }

  private final void mergeTerms(SegmentWriteState segmentWriteState) throws CorruptIndexException, IOException {
    
    final List<Fields> fields = new ArrayList<Fields>();
    final List<ReaderUtil.Slice> slices = new ArrayList<ReaderUtil.Slice>();

    int docBase = 0;

    for(int readerIndex=0;readerIndex<mergeState.readers.size();readerIndex++) {
      final MergeState.IndexReaderAndLiveDocs r = mergeState.readers.get(readerIndex);
      final Fields f = r.reader.fields();
      final int maxDoc = r.reader.maxDoc();
      if (f != null) {
        slices.add(new ReaderUtil.Slice(docBase, maxDoc, readerIndex));
        fields.add(f);
      }
      docBase += maxDoc;
    }

    final FieldsConsumer consumer = codec.postingsFormat().fieldsConsumer(segmentWriteState);
    boolean success = false;
    try {
      consumer.merge(mergeState,
                     new MultiFields(fields.toArray(Fields.EMPTY_ARRAY),
                                     slices.toArray(ReaderUtil.Slice.EMPTY_ARRAY)));
      success = true;
    } finally {
      if (success) {
        IOUtils.close(consumer);
      } else {
        IOUtils.closeWhileHandlingException(consumer);
      }
    }
  }

  private void mergePerDoc(SegmentWriteState segmentWriteState) throws IOException {
      final PerDocConsumer docsConsumer = codec.docValuesFormat()
          .docsConsumer(new PerDocWriteState(segmentWriteState));
      // TODO: remove this check when 3.x indexes are no longer supported
      // (3.x indexes don't have docvalues)
      if (docsConsumer == null) {
        return;
      }
      boolean success = false;
      try {
        docsConsumer.merge(mergeState);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(docsConsumer);
        } else {
          IOUtils.closeWhileHandlingException(docsConsumer);
        }
      }
  }
  
  private void mergeNorms(SegmentWriteState segmentWriteState) throws IOException {
    final PerDocConsumer docsConsumer = codec.normsFormat()
        .docsConsumer(new PerDocWriteState(segmentWriteState));
    // TODO: remove this check when 3.x indexes are no longer supported
    // (3.x indexes don't have docvalues)
    if (docsConsumer == null) {
      return;
    }
    boolean success = false;
    try {
      docsConsumer.merge(mergeState);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docsConsumer);
      } else {
        IOUtils.closeWhileHandlingException(docsConsumer);
      }
    }
  }
}
