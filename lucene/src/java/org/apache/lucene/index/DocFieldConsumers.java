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
import java.util.HashMap;
import java.util.Map;

/** This is just a "splitter" class: it lets you wrap two
 *  DocFieldConsumer instances as a single consumer. */

final class DocFieldConsumers extends DocFieldConsumer {
  final DocFieldConsumer one;
  final DocFieldConsumer two;
  final DocumentsWriterPerThread.DocState docState;

  public DocFieldConsumers(DocFieldProcessor processor, DocFieldConsumer one, DocFieldConsumer two) {
    this.one = one;
    this.two = two;
    this.docState = processor.docState;
  }

  @Override
  public void flush(Map<FieldInfo, DocFieldConsumerPerField> fieldsToFlush, SegmentWriteState state) throws IOException {

    Map<FieldInfo, DocFieldConsumerPerField> oneFieldsToFlush = new HashMap<FieldInfo, DocFieldConsumerPerField>();
    Map<FieldInfo, DocFieldConsumerPerField> twoFieldsToFlush = new HashMap<FieldInfo, DocFieldConsumerPerField>();

    for (Map.Entry<FieldInfo, DocFieldConsumerPerField> fieldToFlush : fieldsToFlush.entrySet()) {
      DocFieldConsumersPerField perField = (DocFieldConsumersPerField) fieldToFlush.getValue();
      oneFieldsToFlush.put(fieldToFlush.getKey(), perField.one);
      twoFieldsToFlush.put(fieldToFlush.getKey(), perField.two);
    }

    one.flush(oneFieldsToFlush, state);
    two.flush(twoFieldsToFlush, state);
  }

  @Override
  public void abort() {
    try {
      one.abort();
    } finally {
      two.abort();
    }
  }

  @Override
  public boolean freeRAM() {
    boolean any = one.freeRAM();
    any |= two.freeRAM();
    return any;
  }

  @Override
  public void finishDocument() throws IOException {
    try {
      one.finishDocument();
    } finally {
      two.finishDocument();
    }
  }

  @Override
  public void startDocument() throws IOException {
    one.startDocument();
    two.startDocument();
  }

  @Override
  public DocFieldConsumerPerField addField(FieldInfo fi) {
    return new DocFieldConsumersPerField(this, fi, one.addField(fi), two.addField(fi));
  }

}
