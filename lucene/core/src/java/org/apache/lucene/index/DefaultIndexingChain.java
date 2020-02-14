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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.VectorsFormat;
import org.apache.lucene.codecs.VectorsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
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

    List<Sorter.DocComparator> comparators = new ArrayList<>();
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
    Sorter sorter = new Sorter(indexSort);
    // returns null if the documents are already sorted
    return sorter.sort(state.segmentInfo.maxDoc(), comparators.toArray(new Sorter.DocComparator[comparators.size()]));
  }

  @Override
  public Sorter.DocMap flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.DocMap sortMap = maybeSortSegment(state);
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write norms");
    }
    SegmentReadState readState = new SegmentReadState(state.directory, state.segmentInfo, state.fieldInfos, true, IOContext.READ, state.segmentSuffix, Collections.emptyMap());

    t0 = System.nanoTime();
    writeDocValues(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write points");
    }

    t0 = System.nanoTime();
    writeVectors(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write vectors");
    }

    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);
    storedFieldsConsumer.flush(state, sortMap);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to finish stored fields");
    }

    t0 = System.nanoTime();
    Map<String, TermsHashPerField> fieldsToFlush = new HashMap<>();
    for (int i = 0; i < fieldHash.length; i++) {
      PerField perField = fieldHash[i];
      while (perField != null) {
        if (perField.invertState != null) {
          fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
        }
        perField = perField.next;
      }
    }

    try (NormsProducer norms = readState.fieldInfos.hasNorms()
        ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
        : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      }
      termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance);
    }
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime();
    docWriter.codec.fieldInfosFormat().write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
    if (docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write fieldInfos");
    }

    return sortMap;
  }

  /**
   * Writes all buffered points.
   */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    PointsWriter pointsWriter = null;
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
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

  /**
   * Writes all buffered doc values (called from {@link #flush}).
   */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    int maxDoc = state.segmentInfo.maxDoc();
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
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

  /**
   * Writes all buffered vectors and knn graph.
   */
  private void writeVectors(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    VectorsWriter vectorsWriter = null;
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.vectorValuesWriter != null) {
            if (perField.fieldInfo.getVectorDimension() == 0) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no vectors but wrote them");
            }
            if (vectorsWriter == null) {
              // lazy init
              VectorsFormat fmt = state.segmentInfo.getCodec().vectorsFormat();
              if (fmt == null) {
                throw new IllegalStateException("field=\"" + perField.fieldInfo.name + "\" was indexed as vectors but codec does not support vectors");
              }
              vectorsWriter = fmt.fieldsWriter(state);
            }

            perField.vectorValuesWriter.flush(state, sortMap, vectorsWriter);
            perField.vectorValuesWriter = null;
          } else if (perField.fieldInfo.getVectorDimension() != 0) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has vectors but did not write them");
          }
          perField = perField.next;
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(vectorsWriter);
      } else {
        IOUtils.closeWhileHandlingException(vectorsWriter);
      }
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
            assert perField.norms != null : "field=" + fi.name;
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
  @SuppressWarnings("try")
  public void abort() throws IOException {
    // finalizer will e.g. close any open files in the term vectors writer:
    try (Closeable finalizer = termsHash::abort) {
      storedFieldsConsumer.abort();
    } finally {
      Arrays.fill(fieldHash, null);
    }
  }

  private void rehash() {
    int newHashSize = (fieldHash.length * 2);
    assert newHashSize > fieldHash.length;

    PerField newHashArray[] = new PerField[newHashSize];

    // Rehash
    int newHashMask = newHashSize - 1;
    for (int j = 0; j < fieldHash.length; j++) {
      PerField fp0 = fieldHash[j];
      while (fp0 != null) {
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

  /**
   * Calls StoredFieldsWriter.startDocument, aborting the
   * segment if it hits any exception.
   */
  private void startStoredFields(int docID) throws IOException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      docWriter.onAbortingException(th);
      throw th;
    }
  }

  /**
   * Calls StoredFieldsWriter.finishDocument, aborting the
   * segment if it hits any exception.
   */
  private void finishStoredFields() throws IOException {
    try {
      storedFieldsConsumer.finishDocument();
    } catch (Throwable th) {
      docWriter.onAbortingException(th);
      throw th;
    }
  }

  @Override
  public void processDocument() throws IOException {

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
    try {
      for (IndexableField field : docState.doc) {
        fieldCount = processField(field, fieldGen, fieldCount);
      }
    } finally {
      if (docWriter.hasHitAbortingException() == false) {
        // Finish each indexed field name seen in the document:
        for (int i = 0; i < fieldCount; i++) {
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
      docWriter.onAbortingException(th);
      throw th;
    }
  }

  private int processField(IndexableField field, long fieldGen, int fieldCount) throws IOException {
    String fieldName = field.name();
    IndexableFieldType fieldType = field.fieldType();

    PerField fp = null;

    if (fieldType.indexOptions() == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + field.name() + "\")");
    }

    // Invert indexed fields:
    if (fieldType.indexOptions() != IndexOptions.NONE && fieldType.vectorDimension() == 0) {
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
          docWriter.onAbortingException(th);
          throw th;
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
    if (fieldType.vectorDimension() != 0) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      indexVector(fp, field);
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

  /**
   * Called from processDocument to index one field's point
   */
  private void indexPoint(PerField fp, IndexableField field) throws IOException {
    int pointDataDimensionCount = field.fieldType().pointDimensionCount();
    int pointIndexDimensionCount = field.fieldType().pointIndexDimensionCount();

    int dimensionNumBytes = field.fieldType().pointNumBytes();

    // Record dimensions for this field; this setter will throw IllegalArgExc if
    // the dimensions were already set to something different:
    if (fp.fieldInfo.getPointDimensionCount() == 0) {
      fieldInfos.globalFieldNumbers.setDimensions(fp.fieldInfo.number, fp.fieldInfo.name, pointDataDimensionCount, pointIndexDimensionCount, dimensionNumBytes);
    }

    fp.fieldInfo.setPointDimensions(pointDataDimensionCount, pointIndexDimensionCount, dimensionNumBytes);

    if (fp.pointValuesWriter == null) {
      fp.pointValuesWriter = new PointValuesWriter(docWriter, fp.fieldInfo);
    }
    fp.pointValuesWriter.addPackedValue(docState.docID, field.binaryValue());
  }

  private void validateIndexSortDVType(Sort indexSort, String fieldName, DocValuesType dvType) {
    for (SortField sortField : indexSort.getSort()) {
      if (sortField.getField().equals(fieldName)) {
        switch (dvType) {
          case NUMERIC:
            if (sortField.getType().equals(SortField.Type.INT) == false &&
                 sortField.getType().equals(SortField.Type.LONG) == false &&
                 sortField.getType().equals(SortField.Type.FLOAT) == false &&
                 sortField.getType().equals(SortField.Type.DOUBLE) == false) {
              throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
            }
            break;

          case BINARY:
            throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);

          case SORTED:
            if (sortField.getType().equals(SortField.Type.STRING) == false) {
              throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
            }
            break;

          case SORTED_NUMERIC:
            if (sortField instanceof SortedNumericSortField == false) {
              throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
            }
            break;

          case SORTED_SET:
            if (sortField instanceof SortedSetSortField == false) {
              throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
            }
            break;

          default:
            throw new IllegalArgumentException("invalid doc value type:" + dvType + " for sortField:" + sortField);
        }
        break;
      }
    }
  }

  /**
   * Called from processDocument to index one field's doc value
   */
  private void indexDocValue(PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

    if (fp.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
      // This is the first time we are seeing this field indexed with doc values, so we
      // now record the DV type so that any future attempt to (illegally) change
      // the DV type of this field, will throw an IllegalArgExc:
      if (docWriter.getSegmentInfo().getIndexSort() != null) {
        final Sort indexSort = docWriter.getSegmentInfo().getIndexSort();
        validateIndexSortDVType(indexSort, fp.fieldInfo.name, dvType);
      }
      fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType);

    }
    fp.fieldInfo.setDocValuesType(dvType);

    int docID = docState.docID;

    switch (dvType) {

      case NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        if (field.numericValue() == null) {
          throw new IllegalArgumentException("field=\"" + fp.fieldInfo.name + "\": null value not allowed");
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

  /**
   * Called from processDocument to index one field's doc value
   */
  private void indexVector(PerField fp, IndexableField field) throws IOException {
    int numDimensions = field.fieldType().vectorDimension();

    // Record dimensions and distance function for this field; this setter will throw IllegalArgExc if
    // the dimensions or distance function were already set to something different:
    if (fp.fieldInfo.getVectorDimension() == 0) {
      fieldInfos.globalFieldNumbers.setVectorProps(fp.fieldInfo.number, fp.fieldInfo.name, numDimensions);
    }
    fp.fieldInfo.setVectorDimension(numDimensions);
    if (fp.vectorValuesWriter == null) {
      fp.vectorValuesWriter = new VectorValuesWriter(fp.fieldInfo, bytesUsed);
    }

    int docID = docState.docID;
    fp.vectorValuesWriter.addValue(docID, field.binaryValue());
  }

  /**
   * Returns a previously created {@link PerField}, or null
   * if this field name wasn't seen yet.
   */
  private PerField getPerField(String name) {
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldInfo.name.equals(name)) {
      fp = fp.next;
    }
    return fp;
  }

  /**
   * Returns a previously created {@link PerField},
   * absorbing the type information from {@link FieldType},
   * and creates a new {@link PerField} if this field name
   * wasn't seen yet.
   */
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
      initIndexOptions(fi, fieldType.indexOptions());
      Map<String, String> attributes = fieldType.getAttributes();
      if (attributes != null) {
        attributes.forEach((k, v) -> fi.putAttribute(k, v));
      }

      fp = new PerField(docWriter.getIndexCreatedVersionMajor(), fi, invert);
      fp.next = fieldHash[hashPos];
      fieldHash[hashPos] = fp;
      totalFieldCount++;

      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length / 2) {
        rehash();
      }

      if (totalFieldCount > fields.length) {
        PerField[] newFields = new PerField[ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }

      if (fieldType.omitNorms()) {
        fi.setOmitsNorms();
      }

    } else if (invert && fp.invertState == null) {
      initIndexOptions(fp.fieldInfo, fieldType.indexOptions());
      fp.setInvertState();
    }

    return fp;
  }

  private void initIndexOptions(FieldInfo info, IndexOptions indexOptions) {
    // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the initial
    // IndexOptions to decide what arrays it must create).
    assert info.getIndexOptions() == IndexOptions.NONE;
    // This is the first time we are seeing this field indexed, so we now
    // record the index options so that any future attempt to (illegally)
    // change the index options of this field, will throw an IllegalArgExc:
    fieldInfos.globalFieldNumbers.setIndexOptions(info.number, info.name, indexOptions);
    info.setIndexOptions(indexOptions);
  }

  /**
   * NOTE: not static: accesses at least docState, termsHash.
   */
  private final class PerField implements Comparable<PerField> {

    final int indexCreatedVersionMajor;
    final FieldInfo fieldInfo;
    final Similarity similarity;

    FieldInvertState invertState;
    TermsHashPerField termsHashPerField;

    // Non-null if this field ever had doc values in this
    // segment:
    DocValuesWriter docValuesWriter;

    // Non-null if this field ever had points in this segment:
    PointValuesWriter pointValuesWriter;

    VectorValuesWriter vectorValuesWriter;

    /**
     * We use this to know when a PerField is seen for the
     * first time in the current document.
     */
    long fieldGen = -1;

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NormValuesWriter norms;

    // reused
    TokenStream tokenStream;

    public PerField(int indexCreatedVersionMajor, FieldInfo fieldInfo, boolean invert) {
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.fieldInfo = fieldInfo;
      similarity = docState.similarity;
      if (invert) {
        setInvertState();
      }
    }

    void setInvertState() {
      invertState = new FieldInvertState(indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
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
      if (fieldInfo.omitsNorms() == false) {
        long normValue;
        if (invertState.length == 0) {
          // the field exists in this document, but it did not have
          // any indexed tokens, so we assign a default value of zero
          // to the norm
          normValue = 0;
        } else {
          normValue = similarity.computeNorm(invertState);
          if (normValue == 0) {
            throw new IllegalStateException("Similarity " + similarity + " return 0 for non-empty field");
          }
        }
        norms.addValue(docState.docID, normValue);
      }

      termsHashPerField.finish();
    }

    /**
     * Inverts one field for one document; first is true
     * if this is the first time we are seeing this field
     * name in this document.
     */
    public void invert(IndexableField field, boolean first) throws IOException {
      if (first) {
        // First time we're seeing this field (indexed) in
        // this document:
        invertState.reset();
      }

      IndexableFieldType fieldType = field.fieldType();

      IndexOptions indexOptions = fieldType.indexOptions();
      fieldInfo.setIndexOptions(indexOptions);

      final boolean analyzed = fieldType.tokenized() && docState.analyzer != null;

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

          int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
          int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
          if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
            throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                               + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + invertState.lastStartOffset + " for field '" + field.name() + "'");
          }
          invertState.lastStartOffset = startOffset;

          try {
            invertState.length = Math.addExact(invertState.length, invertState.termFreqAttribute.getTermFrequency());
          } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("too many tokens for field \"" + field.name() + "\"");
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
            docWriter.onAbortingException(th);
            throw th;
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
    }
  }

  @Override
  DocIdSetIterator getHasDocValues(String field) {
    PerField perField = getPerField(field);
    if (perField != null) {
      if (perField.docValuesWriter != null) {
        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
          return null;
        }

        return perField.docValuesWriter.getDocIdSet();
      }
    }
    return null;
  }
}
