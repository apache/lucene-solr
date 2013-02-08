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

/** Just switches between two {@link DocFieldConsumer}s. */

class TwoStoredFieldsConsumers extends StoredFieldsConsumer {
  private final StoredFieldsConsumer first;
  private final StoredFieldsConsumer second;

  public TwoStoredFieldsConsumers(StoredFieldsConsumer first, StoredFieldsConsumer second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void addField(int docID, IndexableField field, FieldInfo fieldInfo) throws IOException {
    first.addField(docID, field, fieldInfo);
    second.addField(docID, field, fieldInfo);
  }

  @Override
  void flush(SegmentWriteState state) throws IOException {
    first.flush(state);
    second.flush(state);
  }

  @Override
  void abort() {
    try {
      first.abort();
    } catch (Throwable t) {
    }
    try {
      second.abort();
    } catch (Throwable t) {
    }
  }

  @Override
  void startDocument() throws IOException {
    first.startDocument();
    second.startDocument();
  }

  @Override
  void finishDocument() throws IOException {
    first.finishDocument();
    second.finishDocument();
  }
}
