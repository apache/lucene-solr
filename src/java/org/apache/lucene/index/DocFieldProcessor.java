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
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

/**
 * This is a DocConsumer that gathers all fields under the
 * same name, and calls per-field consumers to process field
 * by field.  This class doesn't doesn't do any "real" work
 * of its own: it just forwards the fields to a
 * DocFieldConsumer.
 */

final class DocFieldProcessor extends DocConsumer {

  final DocumentsWriter docWriter;
  final FieldInfos fieldInfos = new FieldInfos();
  final DocFieldConsumer consumer;

  public DocFieldProcessor(DocumentsWriter docWriter, DocFieldConsumer consumer) {
    this.docWriter = docWriter;
    this.consumer = consumer;
    consumer.setFieldInfos(fieldInfos);
  }

  public void closeDocStore(DocumentsWriter.FlushState state) throws IOException {
    consumer.closeDocStore(state);
  }

  public void flush(Collection threads, DocumentsWriter.FlushState state) throws IOException {

    Map childThreadsAndFields = new HashMap();
    Iterator it = threads.iterator();
    while(it.hasNext()) {
      DocFieldProcessorPerThread perThread = (DocFieldProcessorPerThread) it.next();
      childThreadsAndFields.put(perThread.consumer, perThread.fields());
      perThread.trimFields(state);
    }

    consumer.flush(childThreadsAndFields, state);

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    fieldInfos.write(state.directory, state.segmentName + ".fnm");
  }

  public void abort() {
    consumer.abort();
  }

  public boolean freeRAM() {
    return consumer.freeRAM();
  }

  public DocConsumerPerThread addThread(DocumentsWriterThreadState threadState) throws IOException {
    return new DocFieldProcessorPerThread(threadState, this);
  }
}
