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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
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

  // lazy init:
  private StoredFieldsWriter storedFieldsWriter;
  private int lastStoredDocID; 

  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25:
  private PerField[] fieldHash = new PerField[2];
  private int hashMask = 1;

  private int totalFieldCount;
  private long nextFieldGen;

  // Holds fields seen in each document
  private PerField[] fields = new PerField[1];

  public DefaultIndexingChain(DocumentsWriterPerThread docWriter) throws IOException {
    this.docWriter = docWriter;
    this.fieldInfos = docWriter.getFieldInfosBuilder();
    this.docState = docWriter.docState;
    this.bytesUsed = docWriter.bytesUsed;

    TermsHash termVectorsWriter = new TermVectorsConsumer(docWriter);
    termsHash = new FreqProxTermsWriter(docWriter, termVectorsWriter);
  }
  
  // TODO: can we remove this lazy-init / make cleaner / do it another way...? 
  private void initStoredFieldsWriter() throws IOException {
    if (storedFieldsWriter == null) {
      storedFieldsWriter = docWriter.codec.storedFieldsFormat().fieldsWriter(docWriter.directory, docWriter.getSegmentInfo(), IOContext.DEFAULT);
    }
  }

  @Override
  public void flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method

    int numDocs = state.segmentInfo.getDocCount();
    writeNorms(state);
    writeDocValues(state);
    
    // its possible all docs hit non-aborting exceptions...
    initStoredFieldsWriter();
    fillStoredFields(numDocs);
    storedFieldsWriter.finish(state.fieldInfos, numDocs);
    storedFieldsWriter.close();

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

    termsHash.flush(fieldsToFlush, state);

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    FieldInfosWriter infosWriter = docWriter.codec.fieldInfosFormat().getFieldInfosWriter();
    infosWriter.write(state.directory, state.segmentInfo.name, "", state.fieldInfos, IOContext.DEFAULT);
  }

  /** Writes all buffered doc values (called from {@link #flush}). */
  private void writeDocValues(SegmentWriteState state) throws IOException {
    int docCount = state.segmentInfo.getDocCount();
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.docValuesWriter != null) {
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
              dvConsumer = fmt.fieldsConsumer(state);
            }

            perField.docValuesWriter.finish(docCount);
            perField.docValuesWriter.flush(state, dvConsumer);
            perField.docValuesWriter = null;
          }
          perField = perField.next;
        }
      }

      // TODO: catch missing DV fields here?  else we have
      // null/"" depending on how docs landed in segments?
      // but we can't detect all cases, and we should leave
      // this behavior undefined. dv is not "schemaless": its column-stride.
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dvConsumer);
      } else {
        IOUtils.closeWhileHandlingException(dvConsumer);
      }
    }
  }

  /** Catch up for all docs before us that had no stored
   *  fields, or hit non-aborting exceptions before writing
   *  stored fields. */
  private void fillStoredFields(int docID) throws IOException {
    while (lastStoredDocID < docID) {
      startStoredFields();
      finishStoredFields();
    }
  }

  private void writeNorms(SegmentWriteState state) throws IOException {
    boolean success = false;
    DocValuesConsumer normsConsumer = null;
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
          if (fi.omitsNorms() == false) {
            if (perField.norms != null) {
              perField.norms.finish(state.segmentInfo.getDocCount());
              perField.norms.flush(state, normsConsumer);
              assert fi.getNormType() == DocValuesType.NUMERIC;
            } else if (fi.isIndexed()) {
              assert fi.getNormType() == null: "got " + fi.getNormType() + "; field=" + fi.name;
            }
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
    try {
      // E.g. close any open files in the stored fields writer:
      storedFieldsWriter.abort();
    } catch (Throwable t) {
    }

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
  private void startStoredFields() throws IOException {
    boolean success = false;
    try {
      initStoredFieldsWriter();
      storedFieldsWriter.startDocument();
      success = true;
    } finally {
      if (success == false) {
        docWriter.setAborting();        
      }
    }
    lastStoredDocID++;
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the
   *  segment if it hits any exception. */
  private void finishStoredFields() throws IOException {
    boolean success = false;
    try {
      storedFieldsWriter.finishDocument();
      success = true;
    } finally {
      if (success == false) {
        docWriter.setAborting();        
      }
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

    fillStoredFields(docState.docID);
    startStoredFields();

    try {
      for (IndexableField field : docState.doc) {
        fieldCount = processField(field, fieldGen, fieldCount);
      }
    } finally {
      if (docWriter.aborting == false) {
        // Finish each indexed field name seen in the document:
        for (int i=0;i<fieldCount;i++) {
          fields[i].finish();
        }
        finishStoredFields();
      }
    }

    boolean success = false;
    try {
      termsHash.finishDocument();
      success = true;
    } finally {
      if (success == false) {
        // Must abort, on the possibility that on-disk term
        // vectors are now corrupt:
        docWriter.setAborting();
      }
    }
  }
  
  private int processField(IndexableField field, long fieldGen, int fieldCount) throws IOException {
    String fieldName = field.name();
    IndexableFieldType fieldType = field.fieldType();

    PerField fp = null;

    // Invert indexed fields:
    if (fieldType.indexed()) {
      
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
        boolean success = false;
        try {
          storedFieldsWriter.writeField(fp.fieldInfo, field);
          success = true;
        } finally {
          if (!success) {
            docWriter.setAborting();
          }
        }
      }
    }

    DocValuesType dvType = fieldType.docValueType();
    if (dvType != null) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      indexDocValue(fp, dvType, field);
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

  /** Called from processDocument to index one field's doc
   *  value */
  private void indexDocValue(PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

    boolean hasDocValues = fp.fieldInfo.hasDocValues();

    // This will throw an exc if the caller tried to
    // change the DV type for the field:
    fp.fieldInfo.setDocValuesType(dvType);
    if (hasDocValues == false) {
      fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType);
    }

    int docID = docState.docID;

    switch(dvType) {

      case NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed, true);
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

      FieldInfo fi = fieldInfos.addOrUpdate(name, fieldType);
      
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

    } else {
      fp.fieldInfo.update(fieldType);

      if (invert && fp.invertState == null) {
        fp.setInvertState();
      }
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

    /** We use this to know when a PerField is seen for the
     *  first time in the current document. */
    long fieldGen = -1;

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NumericDocValuesWriter norms;
    
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
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldInfo.name.compareTo(other.fieldInfo.name);
    }

    public void finish() throws IOException {
      if (fieldInfo.omitsNorms() == false) {
        if (norms == null) {
          fieldInfo.setNormValueType(FieldInfo.DocValuesType.NUMERIC);
          norms = new NumericDocValuesWriter(fieldInfo, docState.docWriter.bytesUsed, false);
        }
        norms.addValue(docState.docID, similarity.computeNorm(invertState));
      }

      termsHashPerField.finish();
    }

    /** Inverts one field for one document; first is true
     *  if this is the first time we are seeing this field
     *  name in this document. */
    public void invert(IndexableField field, boolean first) throws IOException {
      if (first) {
        // First time we're seeing this field (indexed) in
        // this document:
        invertState.reset();
      }

      IndexableFieldType fieldType = field.fieldType();

      final boolean analyzed = fieldType.tokenized() && docState.analyzer != null;
        
      // only bother checking offsets if something will consume them.
      // TODO: after we fix analyzers, also check if termVectorOffsets will be indexed.
      final boolean checkOffsets = fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

      int lastStartOffset = 0;
      int lastPosition = 0;
        
      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
       * but rather a finally that takes note of the problem.
       */
      boolean aborting = false;
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
          if (invertState.position < lastPosition) {
            if (posIncr == 0) {
              throw new IllegalArgumentException("first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            }
            throw new IllegalArgumentException("position increments (and gaps) must be >= 0 (got " + posIncr + ") for field '" + field.name() + "'");
          }
          lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }
              
          if (checkOffsets) {
            int startOffset = invertState.offset + invertState.offsetAttribute.startOffset();
            int endOffset = invertState.offset + invertState.offsetAttribute.endOffset();
            if (startOffset < lastStartOffset || endOffset < startOffset) {
              throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                                 + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + lastStartOffset + " for field '" + field.name() + "'");
            }
            lastStartOffset = startOffset;
          }

          //System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          aborting = true;
          termsHashPerField.add();
          aborting = false;

          invertState.length++;
        }

        // trigger streams to perform end-of-stream operations
        stream.end();

        // TODO: maybe add some safety? then again, its already checked 
        // when we come back around to the field...
        invertState.position += invertState.posIncrAttribute.getPositionIncrement();
        invertState.offset += invertState.offsetAttribute.endOffset();

        /* if there is an exception coming through, we won't set this to true here:*/
        succeededInProcessingField = true;
      } catch (MaxBytesLengthExceededException e) {
        aborting = false;
        byte[] prefix = new byte[30];
        BytesRef bigTerm = invertState.termAttribute.getBytesRef();
        System.arraycopy(bigTerm.bytes, bigTerm.offset, prefix, 0, 30);
        String msg = "Document contains at least one immense term in field=\"" + fieldInfo.name + "\" (whose UTF8 encoding is longer than the max length " + DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8 + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + Arrays.toString(prefix) + "...', original message: " + e.getMessage();
        if (docState.infoStream.isEnabled("IW")) {
          docState.infoStream.message("IW", "ERROR: " + msg);
        }
        // Document will be deleted above:
        throw new IllegalArgumentException(msg, e);
      } finally {
        if (succeededInProcessingField == false && aborting) {
          docState.docWriter.setAborting();
        }

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
