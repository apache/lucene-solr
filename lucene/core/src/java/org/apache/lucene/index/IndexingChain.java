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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.VectorFormat;
import org.apache.lucene.codecs.VectorWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.VectorField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.RamUsageEstimator;

/** Default general purpose indexing chain, which handles indexing all types of fields. */
final class IndexingChain implements Accountable {

  final Counter bytesUsed = Counter.newCounter();
  final FieldInfos.Builder fieldInfos;

  // Writes postings and term vectors:
  final TermsHash termsHash;
  // Writes stored fields
  final StoredFieldsConsumer storedFieldsConsumer;
  final TermVectorsConsumer termVectorsWriter;

  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25:
  private PerField[] fieldHash = new PerField[2];
  private int hashMask = 1;

  private int totalFieldCount;
  private long nextFieldGen;

  // Holds fields seen in each document
  private PerField[] fields = new PerField[1];
  private PerField[] docFields = new PerField[10];
  private final InfoStream infoStream;
  private final ByteBlockPool.Allocator byteBlockAllocator;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final int indexCreatedVersionMajor;
  private final Consumer<Throwable> abortingExceptionConsumer;
  private boolean hasHitAbortingException;

  IndexingChain(
      int indexCreatedVersionMajor,
      SegmentInfo segmentInfo,
      Directory directory,
      FieldInfos.Builder fieldInfos,
      LiveIndexWriterConfig indexWriterConfig,
      Consumer<Throwable> abortingExceptionConsumer) {
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(bytesUsed);
    IntBlockPool.Allocator intBlockAllocator = new IntBlockAllocator(bytesUsed);
    this.indexWriterConfig = indexWriterConfig;
    assert segmentInfo.getIndexSort() == indexWriterConfig.getIndexSort();
    this.fieldInfos = fieldInfos;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.abortingExceptionConsumer = abortingExceptionConsumer;

    if (segmentInfo.getIndexSort() == null) {
      storedFieldsConsumer =
          new StoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new TermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    } else {
      storedFieldsConsumer =
          new SortingStoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new SortingTermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    }
    termsHash =
        new FreqProxTermsWriter(
            intBlockAllocator, byteBlockAllocator, bytesUsed, termVectorsWriter);
  }

  private void onAbortingException(Throwable th) {
    assert th != null;
    this.hasHitAbortingException = true;
    abortingExceptionConsumer.accept(th);
  }

  private LeafReader getDocValuesLeafReader() {
    return new DocValuesLeafReader() {
      @Override
      public NumericDocValues getNumericDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
          return (NumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
          return (BinaryDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          return (SortedDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
          return (SortedNumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
          return (SortedSetDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return fieldInfos.finish();
      }
    };
  }

  private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    Sort indexSort = state.segmentInfo.getIndexSort();
    if (indexSort == null) {
      return null;
    }

    LeafReader docValuesReader = getDocValuesLeafReader();

    List<IndexSorter.DocComparator> comparators = new ArrayList<>();
    for (int i = 0; i < indexSort.getSort().length; i++) {
      SortField sortField = indexSort.getSort()[i];
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new UnsupportedOperationException("Cannot sort index using sort field " + sortField);
      }
      comparators.add(sorter.getDocComparator(docValuesReader, state.segmentInfo.maxDoc()));
    }
    Sorter sorter = new Sorter(indexSort);
    // returns null if the documents are already sorted
    return sorter.sort(
        state.segmentInfo.maxDoc(), comparators.toArray(IndexSorter.DocComparator[]::new));
  }

  Sorter.DocMap flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.DocMap sortMap = maybeSortSegment(state);
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write norms");
    }
    SegmentReadState readState =
        new SegmentReadState(
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            IOContext.READ,
            state.segmentSuffix);

    t0 = System.nanoTime();
    writeDocValues(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write points");
    }

    t0 = System.nanoTime();
    writeVectors(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write vectors");
    }

    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);
    storedFieldsConsumer.flush(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", ((System.nanoTime() - t0) / 1000000) + " msec to finish stored fields");
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

    try (NormsProducer norms =
        readState.fieldInfos.hasNorms()
            ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
            : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      }
      termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance);
    }
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW",
          ((System.nanoTime() - t0) / 1000000) + " msec to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime();
    indexWriterConfig
        .getCodec()
        .fieldInfosFormat()
        .write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime() - t0) / 1000000) + " msec to write fieldInfos");
    }

    return sortMap;
  }

  /** Writes all buffered points. */
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
              throw new AssertionError(
                  "segment="
                      + state.segmentInfo
                      + ": field=\""
                      + perField.fieldInfo.name
                      + "\" has no points but wrote them");
            }
            if (pointsWriter == null) {
              // lazy init
              PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat();
              if (fmt == null) {
                throw new IllegalStateException(
                    "field=\""
                        + perField.fieldInfo.name
                        + "\" was indexed as points but codec does not support points");
              }
              pointsWriter = fmt.fieldsWriter(state);
            }

            perField.pointValuesWriter.flush(state, sortMap, pointsWriter);
            perField.pointValuesWriter = null;
          } else if (perField.fieldInfo != null
              && perField.fieldInfo.getPointDimensionCount() != 0) {
            // BUG
            throw new AssertionError(
                "segment="
                    + state.segmentInfo
                    + ": field=\""
                    + perField.fieldInfo.name
                    + "\" has points but did not write them");
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
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.docValuesWriter != null) {
            if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
              // BUG
              throw new AssertionError(
                  "segment="
                      + state.segmentInfo
                      + ": field=\""
                      + perField.fieldInfo.name
                      + "\" has no docValues but wrote them");
            }
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
              dvConsumer = fmt.fieldsConsumer(state);
            }
            perField.docValuesWriter.flush(state, sortMap, dvConsumer);
            perField.docValuesWriter = null;
          } else if (perField.fieldInfo != null
              && perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
            // BUG
            throw new AssertionError(
                "segment="
                    + state.segmentInfo
                    + ": field=\""
                    + perField.fieldInfo.name
                    + "\" has docValues but did not write them");
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
        throw new AssertionError(
            "segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
      }
    } else if (dvConsumer == null) {
      // BUG
      throw new AssertionError(
          "segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
    }
  }

  /** Writes all buffered vectors. */
  private void writeVectors(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    VectorWriter vectorWriter = null;
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.vectorValuesWriter != null) {
            if (perField.fieldInfo.getVectorDimension() == 0) {
              // BUG
              throw new AssertionError(
                  "segment="
                      + state.segmentInfo
                      + ": field=\""
                      + perField.fieldInfo.name
                      + "\" has no vectors but wrote them");
            }
            if (vectorWriter == null) {
              // lazy init
              VectorFormat fmt = state.segmentInfo.getCodec().vectorFormat();
              if (fmt == null) {
                throw new IllegalStateException(
                    "field=\""
                        + perField.fieldInfo.name
                        + "\" was indexed as vectors but codec does not support vectors");
              }
              vectorWriter = fmt.fieldsWriter(state);
            }

            perField.vectorValuesWriter.flush(sortMap, vectorWriter);
            perField.vectorValuesWriter = null;
          } else if (perField.fieldInfo != null && perField.fieldInfo.getVectorDimension() != 0) {
            // BUG
            throw new AssertionError(
                "segment="
                    + state.segmentInfo
                    + ": field=\""
                    + perField.fieldInfo.name
                    + "\" has vectors but did not write them");
          }
          perField = perField.next;
        }
      }
      if (vectorWriter != null) {
        vectorWriter.finish();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(vectorWriter);
      } else {
        IOUtils.closeWhileHandlingException(vectorWriter);
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

  @SuppressWarnings("try")
  void abort() throws IOException {
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
        final int hashPos2 = fp0.fieldName.hashCode() & newHashMask;
        PerField nextFP0 = fp0.next;
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  /** Calls StoredFieldsWriter.startDocument, aborting the segment if it hits any exception. */
  private void startStoredFields(int docID) throws IOException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the segment if it hits any exception. */
  private void finishStoredFields() throws IOException {
    try {
      storedFieldsConsumer.finishDocument();
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  void processDocument(int docID, Iterable<? extends IndexableField> document) throws IOException {
    int fieldCount =
        0; // How many field names we've seen (collapses multiple field instances by the same name)
    int indexedFieldCount = 0; // number of unique fields indexed with postings
    long fieldGen = nextFieldGen++;
    int docFieldIdx = 0;

    termsHash.startDocument();
    startStoredFields(docID);
    try {
      // 1st pass over doc fields – verify that doc schema matches the index schema
      // build schema for each unique doc field
      for (IndexableField field : document) {
        IndexableFieldType fieldType = field.fieldType();
        PerField pf = getOrAddPerField(field.name(), fieldType);
        if (pf.fieldGen != fieldGen) { // first time we see this field in this document
          fields[fieldCount++] = pf;
          pf.fieldGen = fieldGen;
          pf.reset(docID);
        }
        if (docFieldIdx >= docFields.length) oversizeDocFields();
        docFields[docFieldIdx++] = pf;
        updateDocFieldSchema(field.name(), pf.schema, fieldType);
      }
      // for each field verify that its schema within the current doc matches its schema in the
      // index
      for (int i = 0; i < fieldCount; i++) {
        PerField pf = fields[i];
        if (pf.fieldInfo == null) { // the first time we see this field in this segment
          initializeFieldInfo(pf);
        } else {
          pf.schema.assertSameSchema(pf.fieldInfo);
        }
      }

      // 2nd pass over doc fields – index each field
      // also count the number of unique fields indexed with postings
      docFieldIdx = 0;
      for (IndexableField field : document) {
        if (processField(docID, field, docFields[docFieldIdx])) {
          fields[indexedFieldCount] = docFields[docFieldIdx];
          indexedFieldCount++;
        }
        docFieldIdx++;
      }
    } finally {
      if (hasHitAbortingException == false) {
        // Finish each indexed field name seen in the document:
        for (int i = 0; i < indexedFieldCount; i++) {
          fields[i].finish(docID);
        }
        finishStoredFields();
        // TODO: for broken docs, optimize termsHash.finishDocument
        try {
          termsHash.finishDocument(docID);
        } catch (Throwable th) {
          // Must abort, on the possibility that on-disk term
          // vectors are now corrupt:
          abortingExceptionConsumer.accept(th);
          throw th;
        }
      }
    }
  }

  private void oversizeDocFields() {
    PerField[] newDocFields =
        new PerField
            [ArrayUtil.oversize(docFields.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
    System.arraycopy(docFields, 0, newDocFields, 0, docFields.length);
    docFields = newDocFields;
  }

  private void initializeFieldInfo(PerField pf) throws IOException {
    // Create and add a new fieldInfo to fieldInfos for this segment.
    // During the creation of FieldInfo there is also verification of the correctness of all its
    // parameters.

    // If the fieldInfo doesn't exist in globalFieldNumbers for the whole index, it will be added
    // there.
    // If the field already exists in globalFieldNumbers (i.e. field present in other segments),
    // we check consistency of its schema with schema for the whole index.
    FieldSchema s = pf.schema;
    if (indexWriterConfig.getIndexSort() != null && s.docValuesType != DocValuesType.NONE) {
      final Sort indexSort = indexWriterConfig.getIndexSort();
      validateIndexSortDVType(indexSort, pf.fieldName, s.docValuesType);
    }

    FieldInfo fi =
        fieldInfos.add(
            pf.fieldName,
            s.storeTermVector,
            s.omitNorms,
            false,
            s.indexOptions,
            s.docValuesType,
            s.dvGen,
            s.attributes,
            s.pointDimensionCount,
            s.pointIndexDimensionCount,
            s.pointNumBytes,
            s.vectorDimension,
            s.vectorSearchStrategy);
    pf.setFieldInfo(fi);
    if (fi.getIndexOptions() != IndexOptions.NONE) {
      pf.setInvertState();
    }
    DocValuesType dvType = fi.getDocValuesType();
    if (dvType != DocValuesType.NONE) {
      switch (dvType) {
        case NUMERIC:
          pf.docValuesWriter = new NumericDocValuesWriter(fi, bytesUsed);
          break;
        case BINARY:
          pf.docValuesWriter = new BinaryDocValuesWriter(fi, bytesUsed);
          break;
        case SORTED:
          pf.docValuesWriter = new SortedDocValuesWriter(fi, bytesUsed);
          break;
        case SORTED_NUMERIC:
          pf.docValuesWriter = new SortedNumericDocValuesWriter(fi, bytesUsed);
          break;
        case SORTED_SET:
          pf.docValuesWriter = new SortedSetDocValuesWriter(fi, bytesUsed);
          break;
        default:
          throw new AssertionError("unrecognized DocValues.Type: " + dvType);
      }
    }
    if (fi.getPointDimensionCount() != 0) {
      pf.pointValuesWriter = new PointValuesWriter(byteBlockAllocator, bytesUsed, fi);
    }
    if (fi.getVectorDimension() != 0) {
      pf.vectorValuesWriter = new VectorValuesWriter(fi, bytesUsed);
    }
  }

  /** Index each field Returns {@code true}, if we are indexing a unique field with postings */
  private boolean processField(int docID, IndexableField field, PerField pf) throws IOException {
    IndexableFieldType fieldType = field.fieldType();
    boolean indexedField = false;

    // Invert indexed fields
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      if (pf.first) { // first time we see this field in this doc
        pf.invert(docID, field, true);
        pf.first = false;
        indexedField = true;
      } else {
        pf.invert(docID, field, false);
      }
    }

    // Add stored fields
    if (fieldType.stored()) {
      String value = field.stringValue();
      if (value != null && value.length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
        throw new IllegalArgumentException(
            "stored field \""
                + field.name()
                + "\" is too large ("
                + value.length()
                + " characters) to store");
      }
      try {
        storedFieldsConsumer.writeField(pf.fieldInfo, field);
      } catch (Throwable th) {
        onAbortingException(th);
        throw th;
      }
    }

    DocValuesType dvType = fieldType.docValuesType();
    if (dvType != DocValuesType.NONE) {
      indexDocValue(docID, pf, dvType, field);
    }
    if (fieldType.pointDimensionCount() != 0) {
      pf.pointValuesWriter.addPackedValue(docID, field.binaryValue());
    }
    if (fieldType.vectorDimension() != 0) {
      pf.vectorValuesWriter.addValue(docID, ((VectorField) field).vectorValue());
    }
    return indexedField;
  }

  /**
   * Returns a previously created {@link PerField}, absorbing the type information from {@link
   * FieldType}, and creates a new {@link PerField} if this field name wasn't seen yet.
   */
  private PerField getOrAddPerField(String fieldName, IndexableFieldType fieldType) {
    final int hashPos = fieldName.hashCode() & hashMask;
    PerField pf = fieldHash[hashPos];
    while (pf != null && pf.fieldName.equals(fieldName) == false) {
      pf = pf.next;
    }
    if (pf == null) {
      // first time we encounter field with this name in this segment
      FieldSchema schema = new FieldSchema(fieldName);
      Map<String, String> attributes = fieldType.getAttributes();
      if (attributes != null) {
        attributes.forEach((k, v) -> schema.putAttribute(k, v));
      }
      pf =
          new PerField(
              fieldName,
              indexCreatedVersionMajor,
              schema,
              indexWriterConfig.getSimilarity(),
              indexWriterConfig.getInfoStream(),
              indexWriterConfig.getAnalyzer());
      pf.next = fieldHash[hashPos];
      fieldHash[hashPos] = pf;
      totalFieldCount++;
      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length / 2) {
        rehash();
      }
      if (totalFieldCount > fields.length) {
        PerField[] newFields =
            new PerField
                [ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }
    }
    return pf;
  }

  // update schema for field as seen in a particular document
  private static void updateDocFieldSchema(
      String fieldName, FieldSchema schema, IndexableFieldType fieldType) {
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      schema.setIndexOptions(
          fieldType.indexOptions(), fieldType.omitNorms(), fieldType.storeTermVectors());
    } else {
      // TODO: should this be checked when a fieldType is created?
      verifyUnIndexedFieldType(fieldName, fieldType);
    }
    if (fieldType.docValuesType() != DocValuesType.NONE) {
      schema.setDocValues(fieldType.docValuesType(), -1);
    }
    if (fieldType.pointDimensionCount() != 0) {
      schema.setPoints(
          fieldType.pointDimensionCount(),
          fieldType.pointIndexDimensionCount(),
          fieldType.pointNumBytes());
    }
    if (fieldType.vectorDimension() != 0) {
      schema.setVectors(fieldType.vectorSearchStrategy(), fieldType.vectorDimension());
    }
  }

  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    if (ft.storeTermVectors()) {
      throw new IllegalArgumentException(
          "cannot store term vectors "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorPositions()) {
      throw new IllegalArgumentException(
          "cannot store term vector positions "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorOffsets()) {
      throw new IllegalArgumentException(
          "cannot store term vector offsets "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorPayloads()) {
      throw new IllegalArgumentException(
          "cannot store term vector payloads "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
  }

  private void validateIndexSortDVType(Sort indexSort, String fieldToValidate, DocValuesType dvType)
      throws IOException {
    for (SortField sortField : indexSort.getSort()) {
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new IllegalStateException("Cannot sort index with sort order " + sortField);
      }
      sorter.getDocComparator(
          new DocValuesLeafReader() {
            @Override
            public NumericDocValues getNumericDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.NUMERIC) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be NUMERIC but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptyNumeric();
            }

            @Override
            public BinaryDocValues getBinaryDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.BINARY) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be BINARY but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptyBinary();
            }

            @Override
            public SortedDocValues getSortedDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySorted();
            }

            @Override
            public SortedNumericDocValues getSortedNumericDocValues(String field) {
              if (Objects.equals(field, fieldToValidate)
                  && dvType != DocValuesType.SORTED_NUMERIC) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED_NUMERIC but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySortedNumeric();
            }

            @Override
            public SortedSetDocValues getSortedSetDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_SET) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED_SET but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySortedSet();
            }

            @Override
            public FieldInfos getFieldInfos() {
              throw new UnsupportedOperationException();
            }
          },
          0);
    }
  }

  /** Called from processDocument to index one field's doc value */
  private void indexDocValue(int docID, PerField fp, DocValuesType dvType, IndexableField field) {
    switch (dvType) {
      case NUMERIC:
        if (field.numericValue() == null) {
          throw new IllegalArgumentException(
              "field=\"" + fp.fieldInfo.name + "\": null value not allowed");
        }
        ((NumericDocValuesWriter) fp.docValuesWriter)
            .addValue(docID, field.numericValue().longValue());
        break;

      case BINARY:
        ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED:
        ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED_NUMERIC:
        ((SortedNumericDocValuesWriter) fp.docValuesWriter)
            .addValue(docID, field.numericValue().longValue());
        break;

      case SORTED_SET:
        ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
  }

  /** Returns a previously created {@link PerField}, or null if this field name wasn't seen yet. */
  private PerField getPerField(String name) {
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldName.equals(name)) {
      fp = fp.next;
    }
    return fp;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get()
        + storedFieldsConsumer.accountable.ramBytesUsed()
        + termVectorsWriter.accountable.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return List.of(storedFieldsConsumer.accountable, termVectorsWriter.accountable);
  }

  /** NOTE: not static: accesses at least docState, termsHash. */
  private final class PerField implements Comparable<PerField> {
    final String fieldName;
    final int indexCreatedVersionMajor;
    final FieldSchema schema;
    FieldInfo fieldInfo;
    final Similarity similarity;

    FieldInvertState invertState;
    TermsHashPerField termsHashPerField;

    // Non-null if this field ever had doc values in this
    // segment:
    DocValuesWriter<?> docValuesWriter;

    // Non-null if this field ever had points in this segment:
    PointValuesWriter pointValuesWriter;

    // Non-null if this field ever had vector values in this segment:
    VectorValuesWriter vectorValuesWriter;

    /** We use this to know when a PerField is seen for the first time in the current document. */
    long fieldGen = -1;

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NormValuesWriter norms;

    // reused
    TokenStream tokenStream;
    private final InfoStream infoStream;
    private final Analyzer analyzer;
    private boolean first; // first in a document

    PerField(
        String fieldName,
        int indexCreatedVersionMajor,
        FieldSchema schema,
        Similarity similarity,
        InfoStream infoStream,
        Analyzer analyzer) {
      this.fieldName = fieldName;
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.schema = schema;
      this.similarity = similarity;
      this.infoStream = infoStream;
      this.analyzer = analyzer;
    }

    void reset(int docId) {
      first = true;
      schema.reset(docId);
    }

    void setFieldInfo(FieldInfo fieldInfo) {
      assert this.fieldInfo == null;
      this.fieldInfo = fieldInfo;
    }

    void setInvertState() {
      invertState =
          new FieldInvertState(
              indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
      termsHashPerField = termsHash.addField(invertState, fieldInfo);
      if (fieldInfo.omitsNorms() == false) {
        assert norms == null;
        // Even if no documents actually succeed in setting a norm, we still write norms for this
        // segment
        norms = new NormValuesWriter(fieldInfo, bytesUsed);
      }
      if (fieldInfo.hasVectors()) {
        termVectorsWriter.setHasVectors();
      }
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldName.compareTo(other.fieldName);
    }

    public void finish(int docID) throws IOException {
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
            throw new IllegalStateException(
                "Similarity " + similarity + " return 0 for non-empty field");
          }
        }
        norms.addValue(docID, normValue);
      }
      termsHashPerField.finish();
    }

    /**
     * Inverts one field for one document; first is true if this is the first time we are seeing
     * this field name in this document.
     */
    public void invert(int docID, IndexableField field, boolean first) throws IOException {
      if (first) {
        // First time we're seeing this field (indexed) in this document
        invertState.reset();
      }

      final boolean analyzed = field.fieldType().tokenized() && analyzer != null;
      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
       * but rather a finally that takes note of the problem.
       */
      boolean succeededInProcessingField = false;
      try (TokenStream stream = tokenStream = field.tokenStream(analyzer, tokenStream)) {
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
              throw new IllegalArgumentException(
                  "first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            } else if (posIncr < 0) {
              throw new IllegalArgumentException(
                  "position increment must be >= 0 (got "
                      + posIncr
                      + ") for field '"
                      + field.name()
                      + "'");
            } else {
              throw new IllegalArgumentException(
                  "position overflowed Integer.MAX_VALUE (got posIncr="
                      + posIncr
                      + " lastPosition="
                      + invertState.lastPosition
                      + " position="
                      + invertState.position
                      + ") for field '"
                      + field.name()
                      + "'");
            }
          } else if (invertState.position > IndexWriter.MAX_POSITION) {
            throw new IllegalArgumentException(
                "position "
                    + invertState.position
                    + " is too large for field '"
                    + field.name()
                    + "': max allowed position is "
                    + IndexWriter.MAX_POSITION);
          }
          invertState.lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }

          int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
          int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
          if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
            throw new IllegalArgumentException(
                "startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                    + "startOffset="
                    + startOffset
                    + ",endOffset="
                    + endOffset
                    + ",lastStartOffset="
                    + invertState.lastStartOffset
                    + " for field '"
                    + field.name()
                    + "'");
          }
          invertState.lastStartOffset = startOffset;

          try {
            invertState.length =
                Math.addExact(invertState.length, invertState.termFreqAttribute.getTermFrequency());
          } catch (ArithmeticException ae) {
            throw new IllegalArgumentException(
                "too many tokens for field \"" + field.name() + "\"");
          }

          // System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          try {
            termsHashPerField.add(invertState.termAttribute.getBytesRef(), docID);
          } catch (MaxBytesLengthExceededException e) {
            byte[] prefix = new byte[30];
            BytesRef bigTerm = invertState.termAttribute.getBytesRef();
            System.arraycopy(bigTerm.bytes, bigTerm.offset, prefix, 0, 30);
            String msg =
                "Document contains at least one immense term in field=\""
                    + fieldInfo.name
                    + "\" (whose UTF8 encoding is longer than the max length "
                    + IndexWriter.MAX_TERM_LENGTH
                    + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '"
                    + Arrays.toString(prefix)
                    + "...', original message: "
                    + e.getMessage();
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "ERROR: " + msg);
            }
            // Document will be deleted above:
            throw new IllegalArgumentException(msg, e);
          } catch (Throwable th) {
            onAbortingException(th);
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
        if (!succeededInProcessingField && infoStream.isEnabled("DW")) {
          infoStream.message(
              "DW", "An exception was thrown while processing field " + fieldInfo.name);
        }
      }

      if (analyzed) {
        invertState.position += analyzer.getPositionIncrementGap(fieldInfo.name);
        invertState.offset += analyzer.getOffsetGap(fieldInfo.name);
      }
    }
  }

  DocIdSetIterator getHasDocValues(String field) {
    PerField perField = getPerField(field);
    if (perField != null) {
      if (perField.docValuesWriter != null) {
        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
          return null;
        }

        return perField.docValuesWriter.getDocValues();
      }
    }
    return null;
  }

  private static class IntBlockAllocator extends IntBlockPool.Allocator {
    private final Counter bytesUsed;

    IntBlockAllocator(Counter bytesUsed) {
      super(IntBlockPool.INT_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }

    /* Allocate another int[] from the shared pool */
    @Override
    public int[] getIntBlock() {
      int[] b = new int[IntBlockPool.INT_BLOCK_SIZE];
      bytesUsed.addAndGet(IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES);
      return b;
    }

    @Override
    public void recycleIntBlocks(int[][] blocks, int offset, int length) {
      bytesUsed.addAndGet(-(length * (IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES)));
    }
  }

  /**
   * A schema of the field in the current document. With every new document this schema is reset. As
   * the document fields are processed, we update the schema with options encountered in this
   * document. Once the processing for the document is done, we compare the built schema of the
   * current document with the corresponding FieldInfo (FieldInfo is built on a first document in
   * the segment where we encounter this field). If there is inconsistency, we raise an error. This
   * ensures that a field has the same data structures across all documents.
   */
  private static final class FieldSchema {
    private final String name;
    private int docID = 0;
    private final Map<String, String> attributes = new HashMap<>();
    private boolean omitNorms = false;
    private boolean storeTermVector = false;
    private IndexOptions indexOptions = IndexOptions.NONE;
    private long dvGen = -1;
    private DocValuesType docValuesType = DocValuesType.NONE;
    private int pointDimensionCount = 0;
    private int pointIndexDimensionCount = 0;
    private int pointNumBytes = 0;
    private int vectorDimension = 0;
    private VectorValues.SearchStrategy vectorSearchStrategy = VectorValues.SearchStrategy.NONE;

    private static String errMsg =
        "Inconsistency of field data structures across documents for field ";

    FieldSchema(String name) {
      this.name = name;
    }

    String putAttribute(String key, String value) {
      return attributes.put(key, value);
    }

    private void assertSame(boolean same) {
      if (same == false) {
        throw new IllegalArgumentException(errMsg + "[" + name + "] of doc [" + docID + "].");
      }
    }

    void setIndexOptions(
        IndexOptions newIndexOptions, boolean newOmitNorms, boolean newStoreTermVector) {
      if (indexOptions == IndexOptions.NONE) {
        indexOptions = newIndexOptions;
        omitNorms = newOmitNorms;
        storeTermVector = newStoreTermVector;
      } else {
        assertSame(
            indexOptions == newIndexOptions
                && omitNorms == newOmitNorms
                && storeTermVector == newStoreTermVector);
      }
    }

    void setDocValues(DocValuesType newDocValuesType, long newDvGen) {
      if (docValuesType == DocValuesType.NONE) {
        this.docValuesType = newDocValuesType;
        this.dvGen = newDvGen;
      } else {
        assertSame(docValuesType == newDocValuesType && dvGen == newDvGen);
      }
    }

    void setPoints(int dimensionCount, int indexDimensionCount, int numBytes) {
      if (pointIndexDimensionCount == 0) {
        pointDimensionCount = dimensionCount;
        pointIndexDimensionCount = indexDimensionCount;
        pointNumBytes = numBytes;
      } else {
        assertSame(
            pointDimensionCount == dimensionCount
                && pointIndexDimensionCount == indexDimensionCount
                && pointNumBytes == numBytes);
      }
    }

    void setVectors(VectorValues.SearchStrategy searchStrategy, int dimension) {
      if (vectorSearchStrategy == VectorValues.SearchStrategy.NONE) {
        this.vectorDimension = dimension;
        this.vectorSearchStrategy = searchStrategy;
      } else {
        assertSame(vectorSearchStrategy == searchStrategy && vectorDimension == dimension);
      }
    }

    void reset(int doc) {
      docID = doc;
      omitNorms = false;
      storeTermVector = false;
      indexOptions = IndexOptions.NONE;
      dvGen = -1;
      docValuesType = DocValuesType.NONE;
      pointDimensionCount = 0;
      pointIndexDimensionCount = 0;
      pointNumBytes = 0;
      vectorDimension = 0;
      vectorSearchStrategy = VectorValues.SearchStrategy.NONE;
    }

    void assertSameSchema(FieldInfo fi) {
      assertSame(
          indexOptions == fi.getIndexOptions()
              && omitNorms == fi.omitsNorms()
              && storeTermVector == fi.hasVectors()
              && docValuesType == fi.getDocValuesType()
              && dvGen == fi.getDocValuesGen()
              && pointDimensionCount == fi.getPointDimensionCount()
              && pointIndexDimensionCount == fi.getPointIndexDimensionCount()
              && pointNumBytes == fi.getPointNumBytes()
              && vectorDimension == fi.getVectorDimension()
              && vectorSearchStrategy == fi.getVectorSearchStrategy());
    }
  }
}
