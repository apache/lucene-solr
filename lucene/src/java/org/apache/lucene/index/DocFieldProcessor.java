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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.codecs.FieldsConsumer;
import org.apache.lucene.index.codecs.docvalues.DocValuesConsumer;
import org.apache.lucene.index.values.ValuesAttribute;
import org.apache.lucene.store.Directory;

/**
 * This is a DocConsumer that gathers all fields under the
 * same name, and calls per-field consumers to process field
 * by field.  This class doesn't doesn't do any "real" work
 * of its own: it just forwards the fields to a
 * DocFieldConsumer.
 */

final class DocFieldProcessor extends DocConsumer {

  final DocumentsWriter docWriter;
  final FieldInfos fieldInfos;
  final DocFieldConsumer consumer;
  final StoredFieldsWriter fieldsWriter;
  final private Map<String, DocValuesConsumer> docValues = new HashMap<String, DocValuesConsumer>();
  private FieldsConsumer fieldsConsumer; // TODO this should be encapsulated in DocumentsWriter

  synchronized DocValuesConsumer docValuesConsumer(Directory dir,
      String segment, String name, ValuesAttribute attr, FieldInfo fieldInfo)
      throws IOException {
    DocValuesConsumer valuesConsumer;
    if ((valuesConsumer = docValues.get(name)) == null) {
      fieldInfo.setDocValues(attr.type());

      if(fieldsConsumer == null) {
        /* nocommit -- this is a hack and only works since DocValuesCodec supports initializing the FieldsConsumer twice.
         * we need to find a way that allows us to obtain a FieldsConsumer per DocumentsWriter. Currently some codecs rely on 
         * the SegmentsWriteState passed in right at the moment when the segment is flushed (doccount etc) but we need the consumer earlier 
         * to support docvalues and later on stored fields too.  
         */
      SegmentWriteState state = docWriter.segWriteState();
      fieldsConsumer = state.segmentCodecs.codec().fieldsConsumer(state);
      }
      valuesConsumer = fieldsConsumer.addValuesField(fieldInfo);
      docValues.put(name, valuesConsumer);
    }
    return valuesConsumer;

  }

 
  public DocFieldProcessor(DocumentsWriter docWriter, DocFieldConsumer consumer) {
    this.fieldInfos = new FieldInfos();
    this.docWriter = docWriter;
    this.consumer = consumer;
    consumer.setFieldInfos(fieldInfos);
    fieldsWriter = new StoredFieldsWriter(docWriter, fieldInfos);
  }

  @Override
  public void closeDocStore(SegmentWriteState state) throws IOException {
    consumer.closeDocStore(state);
    fieldsWriter.closeDocStore(state);
  }

  @Override
  public void flush(Collection<DocConsumerPerThread> threads, SegmentWriteState state) throws IOException {

    Map<DocFieldConsumerPerThread, Collection<DocFieldConsumerPerField>> childThreadsAndFields = new HashMap<DocFieldConsumerPerThread, Collection<DocFieldConsumerPerField>>();
    for ( DocConsumerPerThread thread : threads) {
      DocFieldProcessorPerThread perThread = (DocFieldProcessorPerThread) thread;
      childThreadsAndFields.put(perThread.consumer, perThread.fields());
      perThread.trimFields(state);
    }
    fieldsWriter.flush(state);
    consumer.flush(childThreadsAndFields, state);

    for(DocValuesConsumer p : docValues.values()) {
      if (p != null) {
        p.finish(state.numDocs);
        p.files(state.flushedFiles);
      }
    }
    docValues.clear();
    if(fieldsConsumer != null) {
      fieldsConsumer.close(); // nocommit this should go away
      fieldsConsumer = null;
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    final String fileName = IndexFileNames.segmentFileName(state.segmentName, "", IndexFileNames.FIELD_INFOS_EXTENSION);
    fieldInfos.write(state.directory, fileName);
    state.flushedFiles.add(fileName);
  }

  @Override
  public void abort() {
    fieldsWriter.abort();
    consumer.abort();
  }

  @Override
  public boolean freeRAM() {
    return consumer.freeRAM();
  }

  @Override
  public DocConsumerPerThread addThread(DocumentsWriterThreadState threadState) throws IOException {
    return new DocFieldProcessorPerThread(threadState, this);
  }
}
