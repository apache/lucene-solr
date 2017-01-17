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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/** Default general purpose indexing chain, which handles
 *  indexing all types of fields. */
final class DefaultIndexingChain extends DocConsumer {
  final Counter bytesUsed;
  final DocumentsWriterPerThread.DocState docState;
  final DocumentsWriterPerThread docWriter;
  final FieldInfos.Builder fieldInfos;

  // Writes postings and term vectors:
  final TermsHash termsHash;
  // Writes stored fields
  final StoredFieldsConsumer storedFieldsConsumer;

  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25:
  private PerField[] fieldHash = new PerField[2];
  private int hashMask = 1;

  private int totalFieldCount;
  private long nextFieldGen;

  // Holds fields seen in each document
  private PerField[] fields = new PerField[1];

  private final Set<String> finishedDocValues = new HashSet<>();

  public DefaultIndexingChain(DocumentsWriterPerThread docWriter) throws IOException {
    this.docWriter = docWriter;
    this.fieldInfos = docWriter.getFieldInfosBuilder();
    this.docState = docWriter.docState;
    this.bytesUsed = docWriter.bytesUsed;

    final TermsHash termVectorsWriter;
    if (docWriter.getSegmentInfo().getIndexSort() == null) {
      storedFieldsConsumer = new StoredFieldsConsumer(docWriter);
      termVectorsWriter = new TermVectorsConsumer(docWriter);
    } else {
      storedFieldsConsumer = new SortingStoredFieldsConsumer(docWriter);
      termVectorsWriter = new SortingTermVectorsConsumer(docWriter);
    }
    termsHash = new FreqProxTermsWriter(docWriter, termVectorsWriter);
  }

  private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    Sort indexSort = state.segmentInfo.getIndexSort();
    if (indexSort == null) {
      return null;
    }

    final List<Sorter.DocComparator> comparators = new ArrayList<>();
    for (int i = 0; i < indexSort.getSort().length; i++) {
      SortField sortField = indexSort.getSort()[i];
      PerField perField = getPerField(sortField.getField());
      if (perField != null && perField.docValuesWriter != null &&
          finishedDocValues.contains(perField.fieldInfo.name) == false) {
          perField.docValuesWriter.finish(state.segmentInfo.maxDoc());
          Sorter.DocComparator cmp = perField.docValuesWriter.getDocComparator(state.segmentInfo.maxDoc(), sortField);
          comparators.add(cmp);
          finishedDocValues.add(perField.fieldInfo.name);
      } else {
        // safe to ignore, sort field with no values or already seen before
      }
    }
    Sorter.DocComparator docComparator = new Sorter.DocComparator() {
      @Override
      public int compare(int docID1, int docID2) {
        for (Sorter.DocComparator comparator : comparators) {
          int comp = comparator.compare(docID1, docID2);
          if (comp != 0) {
            return comp;
          }
        }
        return Integer.compare(docID1, docID2); // docid order tiebreak
      }
    };
    // returns null if documents are already sorted
    return Sorter.sort(state.segmentInfo.maxDoc(), docComparator);
  }

  @Override
  public Sorter.DocMap flush(SegmentWriteState state) throws IOException, AbortingException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.DocMap sortMap = maybeSortSegment(state);
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write norms");
    }
    
    t0 = System.nanoTime();
    writeDocValues(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write points");
    }
    
    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);
    storedFieldsConsumer.flush(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to finish stored fields");
    }

    t0 = System.nanoTime();
    Map<String,TermsHashPerField> fieldsToFlush = new HashMap<>();
    for (int i=0;i<fieldHash.length;i++) {
      PerField perField = fieldHash[i];
      while (perField != null) {
        if (perField.invertState != null) {
          fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
        }
        perField = perField.next;
      }
    }

    termsHash.flush(fieldsToFlush, state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime();
    docWriter.codec.fieldInfosFormat().write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write fieldInfos");
    }

    return sortMap;
  }

  /** Writes all buffered points. */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    PointsWriter pointsWriter = null;
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.pointValuesWriter != null) {
            if (perField.fieldInfo.getPointDimensionCount() == 0) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no points but wrote them");
            }
            if (pointsWriter == null) {
              // lazy init
              PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat();
              if (fmt == null) {
                throw new IllegalStateException("field=\"" + perField.fieldInfo.name + "\" was indexed as points but codec does not support points");
              }
              pointsWriter = fmt.fieldsWriter(state);
            }

            perField.pointValuesWriter.flush(state, sortMap, pointsWriter);
            perField.pointValuesWriter = null;
          } else if (perField.fieldInfo.getPointDimensionCount() != 0) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has points but did not write them");
          }
          perField = perField.next;
        }
      }
      if (pointsWriter != null) {
        pointsWriter.finish();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(pointsWriter);
      } else {
        IOUtils.closeWhileHandlingException(pointsWriter);
      }
    }
  }

  /** Writes all buffered doc values (called from {@link #flush}). */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    int maxDoc = state.segmentInfo.maxDoc();
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.docValuesWriter != null) {
            if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no docValues but wrote them");
            }
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
              dvConsumer = fmt.fieldsConsumer(state);
            }

            if (finishedDocValues.contains(perField.fieldInfo.name) == false) {
              perField.docValuesWriter.finish(maxDoc);
            }
            perField.docValuesWriter.flush(state, sortMap, dvConsumer);
            perField.docValuesWriter = null;
          } else if (perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has docValues but did not write them");
          }
          perField = perField.next;
        }
      }

      // TODO: catch missing DV fields here?  else we have
      // null/"" depending on how docs landed in segments?
      // but we can't detect all cases, and we should leave
      // this behavior undefined. dv is not "schemaless": it's column-stride.
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dvConsumer);
      } else {
        IOUtils.closeWhileHandlingException(dvConsumer);
      }
    }

    if (state.fieldInfos.hasDocValues() == false) {
      if (dvConsumer != null) {
        // BUG
        throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
      }
    } else if (dvConsumer == null) {
      // BUG
      throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
    }
  }

  private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    boolean success = false;
    NormsConsumer normsConsumer = null;
    try {
      if (state.fieldInfos.hasNorms()) {
        NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
        assert normsFormat != null;
        normsConsumer = normsFormat.normsConsumer(state);

        for (FieldInfo fi : state.fieldInfos) {
          PerField perField = getPerField(fi.name);
          assert perField != null;

          // we must check the final value of omitNorms for the fieldinfo: it could have 
          // changed for this field since the first time we added it.
          if (fi.omitsNorms() == false && fi.getIndexOptions() != IndexOptions.NONE) {
            assert perField.norms != null: "field=" + fi.name;
            perField.norms.finish(state.segmentInfo.maxDoc());
            perField.norms.flush(state, sortMap, normsConsumer);
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(normsConsumer);
      } else {
        IOUtils.closeWhileHandlingException(normsConsumer);
      }
    }
  }

  @Override
  public void abort() {
    storedFieldsConsumer.abort();

    try {
      // E.g. close any open files in the term vectors writer:
      termsHash.abort();
    } catch (Throwable t) {
    }

    Arrays.fill(fieldHash, null);
  }  

  private void rehash() {
    int newHashSize = (fieldHash.length*2);
    assert newHashSize > fieldHash.length;

    PerField newHashArray[] = new PerField[newHashSize];

    // Rehash
    int newHashMask = newHashSize-1;
    for(int j=0;j<fieldHash.length;j++) {
      PerField fp0 = fieldHash[j];
      while(fp0 != null) {
        final int hashPos2 = fp0.fieldInfo.name.hashCode() & newHashMask;
        PerField nextFP0 = fp0.next;
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  /** Calls StoredFieldsWriter.startDocument, aborting the
   *  segment if it hits any exception. */
  private void startStoredFields(int docID) throws IOException, AbortingException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      throw AbortingException.wrap(th);
    }
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the
   *  segment if it hits any exception. */
  private void finishStoredFields() throws IOException, AbortingException {
    try {
      storedFieldsConsumer.finishDocument();
    } catch (Throwable th) {
      throw AbortingException.wrap(th);
    }
  }

  @Override
  public void processDocument() throws IOException, AbortingException {

    // How many indexed field names we've seen (collapses
    // multiple field instances by the same name):
    int fieldCount = 0;

    long fieldGen = nextFieldGen++;

    // NOTE: we need two passes here, in case there are
    // multi-valued fields, because we must process all
    // instances of a given field at once, since the
    // analyzer is free to reuse TokenStream across fields
    // (i.e., we cannot have more than one TokenStream
    // running "at once"):

    termsHash.startDocument();

    startStoredFields(docState.docID);

    boolean aborting = false;
    try {
      for (IndexableField field : docState.doc) {
        fieldCount = processField(field, fieldGen, fieldCount);
      }
    } catch (AbortingException ae) {
      aborting = true;
      throw ae;
    } finally {
      if (aborting == false) {
        // Finish each indexed field name seen in the document:
        for (int i=0;i<fieldCount;i++) {
          fields[i].finish();
        }
        finishStoredFields();
      }
    }

    try {
      termsHash.finishDocument();
    } catch (Throwable th) {
      // Must abort, on the possibility that on-disk term
      // vectors are now corrupt:
      throw AbortingException.wrap(th);
    }
  }

  private int processField(IndexableField field, long fieldGen, int fieldCount) throws IOException, AbortingException {
    String fieldName = field.name();
    IndexableFieldType fieldType = field.fieldType();

    PerField fp = null;

    if (fieldType.indexOptions() == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + field.name() + "\")");
    }

    // Invert indexed fields:
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      
      // if the field omits norms, the boost cannot be indexed.
      if (fieldType.omitNorms() && field.boost() != 1.0f) {
        throw new UnsupportedOperationException("You cannot set an index-time boost: norms are omitted for field '" + field.name() + "'");
      }
      
      fp = getOrAddField(fieldName, fieldType, true);
      boolean first = fp.fieldGen != fieldGen;
      fp.invert(field, first);

      if (first) {
        fields[fieldCount++] = fp;
        fp.fieldGen = fieldGen;
      }
    } else {
      verifyUnIndexedFieldType(fieldName, fieldType);
    }

    // Add stored fields:
    if (fieldType.stored()) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      if (fieldType.stored()) {
        String value = field.stringValue();
        if (value != null && value.length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
          throw new IllegalArgumentException("stored field \"" + field.name() + "\" is too large (" + value.length() + " characters) to store");
        }
        try {
          storedFieldsConsumer.writeField(fp.fieldInfo, field);
        } catch (Throwable th) {
          throw AbortingException.wrap(th);
        }
      }
    }

    DocValuesType dvType = fieldType.docValuesType();
    if (dvType == null) {
      throw new NullPointerException("docValuesType must not be null (field: \"" + fieldName + "\")");
    }
    if (dvType != DocValuesType.NONE) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      indexDocValue(fp, dvType, field);
    }
    if (fieldType.pointDimensionCount() != 0) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      indexPoint(fp, field);
    }
    
    return fieldCount;
  }

  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    if (ft.storeTermVectors()) {
      throw new IllegalArgumentException("cannot store term vectors "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorPositions()) {
      throw new IllegalArgumentException("cannot store term vector positions "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorOffsets()) {
      throw new IllegalArgumentException("cannot store term vector offsets "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorPayloads()) {
      throw new IllegalArgumentException("cannot store term vector payloads "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
  }

  /** Called from processDocument to index one field's point */
  private void indexPoint(PerField fp, IndexableField field) throws IOException {
    int pointDimensionCount = field.fieldType().pointDimensionCount();

    int dimensionNumBytes = field.fieldType().pointNumBytes();

    // Record dimensions for this field; this setter will throw IllegalArgExc if
    // the dimensions were already set to something different:
    if (fp.fieldInfo.getPointDimensionCount() == 0) {
      fieldInfos.globalFieldNumbers.setDimensions(fp.fieldInfo.number, fp.fieldInfo.name, pointDimensionCount, dimensionNumBytes);
    }

    fp.fieldInfo.setPointDimensions(pointDimensionCount, dimensionNumBytes);

    if (fp.pointValuesWriter == null) {
      fp.pointValuesWriter = new PointValuesWriter(docWriter, fp.fieldInfo);
    }
    fp.pointValuesWriter.addPackedValue(docState.docID, field.binaryValue());
  }

  /** Called from processDocument to index one field's doc value */
  private void indexDocValue(PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

    if (fp.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
      // This is the first time we are seeing this field indexed with doc values, so we
      // now record the DV type so that any future attempt to (illegally) change
      // the DV type of this field, will throw an IllegalArgExc:
      fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType);
    }
    fp.fieldInfo.setDocValuesType(dvType);

    int docID = docState.docID;

    switch(dvType) {

      case NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((NumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
        break;

      case BINARY:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new BinaryDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;
        
      case SORTED_NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedNumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedNumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
        break;

      case SORTED_SET:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedSetDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
  }

  /** Returns a previously created {@link PerField}, or null
   *  if this field name wasn't seen yet. */
  private PerField getPerField(String name) {
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldInfo.name.equals(name)) {
      fp = fp.next;
    }
    return fp;
  }

  /** Returns a previously created {@link PerField},
   *  absorbing the type information from {@link FieldType},
   *  and creates a new {@link PerField} if this field name
   *  wasn't seen yet. */
  private PerField getOrAddField(String name, IndexableFieldType fieldType, boolean invert) {

    // Make sure we have a PerField allocated
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldInfo.name.equals(name)) {
      fp = fp.next;
    }

    if (fp == null) {
      // First time we are seeing this field in this segment

      FieldInfo fi = fieldInfos.getOrAdd(name);
      // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the initial
      // IndexOptions to decide what arrays it must create).  Then, we also must set it in
      // PerField.invert to allow for later downgrading of the index options:
      fi.setIndexOptions(fieldType.indexOptions());
      
      fp = new PerField(fi, invert);
      fp.next = fieldHash[hashPos];
      fieldHash[hashPos] = fp;
      totalFieldCount++;

      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length/2) {
        rehash();
      }

      if (totalFieldCount > fields.length) {
        PerField[] newFields = new PerField[ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }

    } else if (invert && fp.invertState == null) {
      // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the initial
      // IndexOptions to decide what arrays it must create).  Then, we also must set it in
      // PerField.invert to allow for later downgrading of the index options:
      fp.fieldInfo.setIndexOptions(fieldType.indexOptions());
      fp.setInvertState();
    }

    return fp;
  }

  /** NOTE: not static: accesses at least docState, termsHash. */
  private final class PerField implements Comparable<PerField> {

    final FieldInfo fieldInfo;
    final Similarity similarity;

    FieldInvertState invertState;
    TermsHashPerField termsHashPerField;

    // Non-null if this field ever had doc values in this
    // segment:
    DocValuesWriter docValuesWriter;

    // Non-null if this field ever had points in this segment:
    PointValuesWriter pointValuesWriter;

    /** We use this to know when a PerField is seen for the
     *  first time in the current document. */
    long fieldGen = -1;

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NormValuesWriter norms;
    
    // reused
    TokenStream tokenStream;

    public PerField(FieldInfo fieldInfo, boolean invert) {
      this.fieldInfo = fieldInfo;
      similarity = docState.similarity;
      if (invert) {
        setInvertState();
      }
    }

    void setInvertState() {
      invertState = new FieldInvertState(fieldInfo.name);
      termsHashPerField = termsHash.addField(invertState, fieldInfo);
      if (fieldInfo.omitsNorms() == false) {
        assert norms == null;
        // Even if no documents actually succeed in setting a norm, we still write norms for this segment:
        norms = new NormValuesWriter(fieldInfo, docState.docWriter.bytesUsed);
      }
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldInfo.name.compareTo(other.fieldInfo.name);
    }

    public void finish() throws IOException {
      if (fieldInfo.omitsNorms() == false && invertState.length != 0) {
        norms.addValue(docState.docID, similarity.computeNorm(invertState));
      }

      termsHashPerField.finish();
    }

    /** Inverts one field for one document; first is true
     *  if this is the first time we are seeing this field
     *  name in this document. */
    public void invert(IndexableField field, boolean first) throws IOException, AbortingException {
      if (first) {
        // First time we're seeing this field (indexed) in
        // this document:
        invertState.reset();
      }

      IndexableFieldType fieldType = field.fieldType();

      IndexOptions indexOptions = fieldType.indexOptions();
      fieldInfo.setIndexOptions(indexOptions);

      if (fieldType.omitNorms()) {
        fieldInfo.setOmitsNorms();
      }

      final boolean analyzed = fieldType.tokenized() && docState.analyzer != null;
        
      // only bother checking offsets if something will consume them.
      // TODO: after we fix analyzers, also check if termVectorOffsets will be indexed.
      final boolean checkOffsets = indexOptions == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
       * but rather a finally that takes note of the problem.
       */
      boolean succeededInProcessingField = false;
      try (TokenStream stream = tokenStream = field.tokenStream(docState.analyzer, tokenStream)) {
        // reset the TokenStream to the first token
        stream.reset();
        invertState.setAttributeSource(stream);
        termsHashPerField.start(field, first);

        while (stream.incrementToken()) {

          // If we hit an exception in stream.next below
          // (which is fairly common, e.g. if analyzer
          // chokes on a given document), then it's
          // non-aborting and (above) this one document
          // will be marked as deleted, but still
          // consume a docID

          int posIncr = invertState.posIncrAttribute.getPositionIncrement();
          invertState.position += posIncr;
          if (invertState.position < invertState.lastPosition) {
            if (posIncr == 0) {
              throw new IllegalArgumentException("first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            } else if (posIncr < 0) {
              throw new IllegalArgumentException("position increment must be >= 0 (got " + posIncr + ") for field '" + field.name() + "'");
            } else {
              throw new IllegalArgumentException("position overflowed Integer.MAX_VALUE (got posIncr=" + posIncr + " lastPosition=" + invertState.lastPosition + " position=" + invertState.position + ") for field '" + field.name() + "'");
            }
          } else if (invertState.position > IndexWriter.MAX_POSITION) {
            throw new IllegalArgumentException("position " + invertState.position + " is too large for field '" + field.name() + "': max allowed position is " + IndexWriter.MAX_POSITION);
          }
          invertState.lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }
              
          if (checkOffsets) {
            int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
            int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
            if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
              throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                                 + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + invertState.lastStartOffset + " for field '" + field.name() + "'");
            }
            invertState.lastStartOffset = startOffset;
          }

          invertState.length++;
          if (invertState.length < 0) {
            throw new IllegalArgumentException("too many tokens in field '" + field.name() + "'");
          }
          //System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          try {
            termsHashPerField.add();
          } catch (MaxBytesLengthExceededException e) {
            byte[] prefix = new byte[30];
            BytesRef bigTerm = invertState.termAttribute.getBytesRef();
            System.arraycopy(bigTerm.bytes, bigTerm.offset, prefix, 0, 30);
            String msg = "Document contains at least one immense term in field=\"" + fieldInfo.name + "\" (whose UTF8 encoding is longer than the max length " + DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8 + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + Arrays.toString(prefix) + "...', original message: " + e.getMessage();
            if (docState.infoStream.isEnabled("IW")) {
              docState.infoStream.message("IW", "ERROR: " + msg);
            }
            // Document will be deleted above:
            throw new IllegalArgumentException(msg, e);
          } catch (Throwable th) {
            throw AbortingException.wrap(th);
          }
        }

        // trigger streams to perform end-of-stream operations
        stream.end();

        // TODO: maybe add some safety? then again, it's already checked 
        // when we come back around to the field...
        invertState.position += invertState.posIncrAttribute.getPositionIncrement();
        invertState.offset += invertState.offsetAttribute.endOffset();

        /* if there is an exception coming through, we won't set this to true here:*/
        succeededInProcessingField = true;
      } finally {
        if (!succeededInProcessingField && docState.infoStream.isEnabled("DW")) {
          docState.infoStream.message("DW", "An exception was thrown while processing field " + fieldInfo.name);
        }
      }

      if (analyzed) {
        invertState.position += docState.analyzer.getPositionIncrementGap(fieldInfo.name);
        invertState.offset += docState.analyzer.getOffsetGap(fieldInfo.name);
      }

      invertState.boost *= field.boost();
    }
  }
}
