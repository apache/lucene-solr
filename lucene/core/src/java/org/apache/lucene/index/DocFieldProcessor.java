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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosWriter;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Counter;

/**
 * This is a DocConsumer that gathers all fields under the
 * same name, and calls per-field consumers to process field
 * by field.  This class doesn't doesn't do any "real" work
 * of its own: it just forwards the fields to a
 * DocFieldConsumer.
 */

final class DocFieldProcessor extends DocConsumer {

  final DocFieldConsumer consumer;
  final StoredFieldsConsumer storedConsumer;
  final Codec codec;

  // Holds all fields seen in current doc
  DocFieldProcessorPerField[] fields = new DocFieldProcessorPerField[1];
  int fieldCount;

  // Hash table for all fields ever seen
  DocFieldProcessorPerField[] fieldHash = new DocFieldProcessorPerField[2];
  int hashMask = 1;
  int totalFieldCount;

  int fieldGen;
  final DocumentsWriterPerThread.DocState docState;

  final Counter bytesUsed;

  public DocFieldProcessor(DocumentsWriterPerThread docWriter, DocFieldConsumer consumer, StoredFieldsConsumer storedConsumer) {
    this.docState = docWriter.docState;
    this.codec = docWriter.codec;
    this.bytesUsed = docWriter.bytesUsed;
    this.consumer = consumer;
    this.storedConsumer = storedConsumer;
  }

  @Override
  public void flush(SegmentWriteState state) throws IOException {

    Map<String,DocFieldConsumerPerField> childFields = new HashMap<String,DocFieldConsumerPerField>();
    Collection<DocFieldConsumerPerField> fields = fields();
    for (DocFieldConsumerPerField f : fields) {
      childFields.put(f.getFieldInfo().name, f);
    }

    assert fields.size() == totalFieldCount;

    storedConsumer.flush(state);
    consumer.flush(childFields, state);

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    FieldInfosWriter infosWriter = codec.fieldInfosFormat().getFieldInfosWriter();
    infosWriter.write(state.directory, state.segmentInfo.name, state.fieldInfos, IOContext.DEFAULT);
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
      storedConsumer.abort();
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
  public void processDocument(FieldInfos.Builder fieldInfos) throws IOException {

    consumer.startDocument();
    storedConsumer.startDocument();

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
        FieldInfo fi = fieldInfos.addOrUpdate(fieldName, field.fieldType());

        fp = new DocFieldProcessorPerField(this, fi);
        fp.next = fieldHash[hashPos];
        fieldHash[hashPos] = fp;
        totalFieldCount++;

        if (totalFieldCount >= fieldHash.length/2) {
          rehash();
        }
      } else {
        fp.fieldInfo.update(field.fieldType());
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
      storedConsumer.addField(docState.docID, field, fp.fieldInfo);
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

    if (docState.maxTermPrefix != null && docState.infoStream.isEnabled("IW")) {
      docState.infoStream.message("IW", "WARNING: document contains at least one immense term (whose UTF8 encoding is longer than the max length " + DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8 + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + docState.maxTermPrefix + "...'");
      docState.maxTermPrefix = null;
    }
  }

  private static final Comparator<DocFieldProcessorPerField> fieldsComp = new Comparator<DocFieldProcessorPerField>() {
    @Override
    public int compare(DocFieldProcessorPerField o1, DocFieldProcessorPerField o2) {
      return o1.fieldInfo.name.compareTo(o2.fieldInfo.name);
    }
  };
  
  @Override
  void finishDocument() throws IOException {
    try {
      storedConsumer.finishDocument();
    } finally {
      consumer.finishDocument();
    }
  }
}