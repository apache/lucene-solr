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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.index.DocumentsWriterPerThread.DocState;
import org.apache.lucene.index.codecs.Codec;
import org.apache.lucene.index.codecs.DocValuesConsumer;
import org.apache.lucene.index.codecs.PerDocConsumer;
import org.apache.lucene.index.values.PerDocFieldValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;


/**
 * This is a DocConsumer that gathers all fields under the
 * same name, and calls per-field consumers to process field
 * by field.  This class doesn't doesn't do any "real" work
 * of its own: it just forwards the fields to a
 * DocFieldConsumer.
 */

final class DocFieldProcessor extends DocConsumer {

  final DocFieldConsumer consumer;
  final StoredFieldsWriter fieldsWriter;

  // Holds all fields seen in current doc
  DocFieldProcessorPerField[] fields = new DocFieldProcessorPerField[1];
  int fieldCount;

  // Hash table for all fields ever seen
  DocFieldProcessorPerField[] fieldHash = new DocFieldProcessorPerField[2];
  int hashMask = 1;
  int totalFieldCount;

  float docBoost;
  int fieldGen;
  final DocumentsWriterPerThread.DocState docState;

  public DocFieldProcessor(DocumentsWriterPerThread docWriter, DocFieldConsumer consumer) {
    this.docState = docWriter.docState;
    this.consumer = consumer;
    fieldsWriter = new StoredFieldsWriter(docWriter);
  }

  @Override
  public void flush(SegmentWriteState state) throws IOException {

    Map<FieldInfo, DocFieldConsumerPerField> childFields = new HashMap<FieldInfo, DocFieldConsumerPerField>();
    Collection<DocFieldConsumerPerField> fields = fields();
    for (DocFieldConsumerPerField f : fields) {
      childFields.put(f.getFieldInfo(), f);
    }

    fieldsWriter.flush(state);
    consumer.flush(childFields, state);

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    final String fileName = IndexFileNames.segmentFileName(state.segmentName, "", IndexFileNames.FIELD_INFOS_EXTENSION);
    state.fieldInfos.write(state.directory, fileName);
    for (DocValuesConsumerAndDocID consumers : docValues.values()) {
      consumers.docValuesConsumer.finish(state.numDocs);
    }
    // close perDocConsumer during flush to ensure all files are flushed due to PerCodec CFS
    IOUtils.close(perDocConsumers.values());
  }

  @Override
  public void abort() {
    Throwable th = null;
    
    for (DocFieldProcessorPerField field : fieldHash) {
      while (field != null) {
        final DocFieldProcessorPerField next = field.next;
        try {
          field.abort();
        } catch (Throwable t) {
          if (th == null) {
            th = t;
          }
        }
        field = next;
      }
    }
    try {
      IOUtils.closeWhileHandlingException(perDocConsumers.values());
      // TODO add abort to PerDocConsumer!
    } catch (IOException e) {
      // ignore on abort!
    }
    
    try {
      fieldsWriter.abort();
    } catch (Throwable t) {
      if (th == null) {
        th = t;
      }
    }
    
    try {
      consumer.abort();
    } catch (Throwable t) {
      if (th == null) {
        th = t;
      }
    }
    
    // If any errors occured, throw it.
    if (th != null) {
      if (th instanceof RuntimeException) throw (RuntimeException) th;
      if (th instanceof Error) throw (Error) th;
      // defensive code - we should not hit unchecked exceptions
      throw new RuntimeException(th);
    }
  }

  @Override
  public boolean freeRAM() {
    return consumer.freeRAM();
  }

  public Collection<DocFieldConsumerPerField> fields() {
    Collection<DocFieldConsumerPerField> fields = new HashSet<DocFieldConsumerPerField>();
    for(int i=0;i<fieldHash.length;i++) {
      DocFieldProcessorPerField field = fieldHash[i];
      while(field != null) {
        fields.add(field.consumer);
        field = field.next;
      }
    }
    assert fields.size() == totalFieldCount;
    return fields;
  }

  /** In flush we reset the fieldHash to not maintain per-field state
   *  across segments */
  @Override
  void doAfterFlush() {
    fieldHash = new DocFieldProcessorPerField[2];
    hashMask = 1;
    totalFieldCount = 0;
    perDocConsumers.clear();
    docValues.clear();
  }

  private void rehash() {
    final int newHashSize = (fieldHash.length*2);
    assert newHashSize > fieldHash.length;

    final DocFieldProcessorPerField newHashArray[] = new DocFieldProcessorPerField[newHashSize];

    // Rehash
    int newHashMask = newHashSize-1;
    for(int j=0;j<fieldHash.length;j++) {
      DocFieldProcessorPerField fp0 = fieldHash[j];
      while(fp0 != null) {
        final int hashPos2 = fp0.fieldInfo.name.hashCode() & newHashMask;
        DocFieldProcessorPerField nextFP0 = fp0.next;
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  @Override
  public void processDocument(FieldInfos fieldInfos) throws IOException {

    consumer.startDocument();
    fieldsWriter.startDocument();

    fieldCount = 0;

    final int thisFieldGen = fieldGen++;

    // Absorb any new fields first seen in this document.
    // Also absorb any changes to fields we had already
    // seen before (eg suddenly turning on norms or
    // vectors, etc.):

    for(IndexableField field : docState.doc) {
      final String fieldName = field.name();

      // Make sure we have a PerField allocated
      final int hashPos = fieldName.hashCode() & hashMask;
      DocFieldProcessorPerField fp = fieldHash[hashPos];
      while(fp != null && !fp.fieldInfo.name.equals(fieldName)) {
        fp = fp.next;
      }

      if (fp == null) {

        // TODO FI: we need to genericize the "flags" that a
        // field holds, and, how these flags are merged; it
        // needs to be more "pluggable" such that if I want
        // to have a new "thing" my Fields can do, I can
        // easily add it
        FieldInfo fi = fieldInfos.addOrUpdate(fieldName, field.fieldType(), false, field.docValuesType());

        fp = new DocFieldProcessorPerField(this, fi);
        fp.next = fieldHash[hashPos];
        fieldHash[hashPos] = fp;
        totalFieldCount++;

        if (totalFieldCount >= fieldHash.length/2) {
          rehash();
        }
      } else {
        fieldInfos.addOrUpdate(fp.fieldInfo.name, field.fieldType(), false, field.docValuesType());
      }

      if (thisFieldGen != fp.lastGen) {

        // First time we're seeing this field for this doc
        fp.fieldCount = 0;

        if (fieldCount == fields.length) {
          final int newSize = fields.length*2;
          DocFieldProcessorPerField newArray[] = new DocFieldProcessorPerField[newSize];
          System.arraycopy(fields, 0, newArray, 0, fieldCount);
          fields = newArray;
        }

        fields[fieldCount++] = fp;
        fp.lastGen = thisFieldGen;
      }

      fp.addField(field);

      if (field.fieldType().stored()) {
        fieldsWriter.addField(field, fp.fieldInfo);
      }
      final PerDocFieldValues docValues = field.docValues();
      if (docValues != null) {
        docValuesConsumer(docState, fp.fieldInfo).add(docState.docID, docValues);
      }
    }

    // If we are writing vectors then we must visit
    // fields in sorted order so they are written in
    // sorted order.  TODO: we actually only need to
    // sort the subset of fields that have vectors
    // enabled; we could save [small amount of] CPU
    // here.
    ArrayUtil.quickSort(fields, 0, fieldCount, fieldsComp);
    for(int i=0;i<fieldCount;i++) {
      final DocFieldProcessorPerField perField = fields[i];
      perField.consumer.processFields(perField.fields, perField.fieldCount);
    }

    if (docState.maxTermPrefix != null && docState.infoStream != null) {
      docState.infoStream.println("WARNING: document contains at least one immense term (whose UTF8 encoding is longer than the max length " + DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8 + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + docState.maxTermPrefix + "...'");
      docState.maxTermPrefix = null;
    }
  }

  private static final Comparator<DocFieldProcessorPerField> fieldsComp = new Comparator<DocFieldProcessorPerField>() {
    public int compare(DocFieldProcessorPerField o1, DocFieldProcessorPerField o2) {
      return o1.fieldInfo.name.compareTo(o2.fieldInfo.name);
    }
  };
  
  @Override
  void finishDocument() throws IOException {
    try {
      fieldsWriter.finishDocument();
    } finally {
      consumer.finishDocument();
    }
  }

  private static class DocValuesConsumerAndDocID {
    public int docID;
    final DocValuesConsumer docValuesConsumer;

    public DocValuesConsumerAndDocID(DocValuesConsumer docValuesConsumer) {
      this.docValuesConsumer = docValuesConsumer;
    }
  }

  final private Map<String, DocValuesConsumerAndDocID> docValues = new HashMap<String, DocValuesConsumerAndDocID>();
  final private Map<Integer, PerDocConsumer> perDocConsumers = new HashMap<Integer, PerDocConsumer>();

  DocValuesConsumer docValuesConsumer(DocState docState, FieldInfo fieldInfo) 
      throws IOException {
    DocValuesConsumerAndDocID docValuesConsumerAndDocID = docValues.get(fieldInfo.name);
    if (docValuesConsumerAndDocID != null) {
      if (docState.docID == docValuesConsumerAndDocID.docID) {
        throw new IllegalArgumentException("IndexDocValuesField \"" + fieldInfo.name + "\" appears more than once in this document (only one value is allowed, per field)");
      }
      assert docValuesConsumerAndDocID.docID < docState.docID;
      docValuesConsumerAndDocID.docID = docState.docID;
      return docValuesConsumerAndDocID.docValuesConsumer;
    }
    PerDocConsumer perDocConsumer = perDocConsumers.get(fieldInfo.getCodecId());
    if (perDocConsumer == null) {
      PerDocWriteState perDocWriteState = docState.docWriter.newPerDocWriteState(fieldInfo.getCodecId());
      SegmentCodecs codecs = perDocWriteState.segmentCodecs;
      assert codecs.codecs.length > fieldInfo.getCodecId();
      Codec codec = codecs.codecs[fieldInfo.getCodecId()];
      perDocConsumer = codec.docsConsumer(perDocWriteState);
      perDocConsumers.put(Integer.valueOf(fieldInfo.getCodecId()), perDocConsumer);
    }
    boolean success = false;
    DocValuesConsumer docValuesConsumer = null;
    try {
      docValuesConsumer = perDocConsumer.addValuesField(fieldInfo);
      fieldInfo.commitDocValues();
      success = true;
    } finally {
      if (!success) {
        fieldInfo.revertUncommitted();
      }
    }

    docValuesConsumerAndDocID = new DocValuesConsumerAndDocID(docValuesConsumer);
    docValuesConsumerAndDocID.docID = docState.docID;
    docValues.put(fieldInfo.name, docValuesConsumerAndDocID);
    return docValuesConsumer;
  }
}
