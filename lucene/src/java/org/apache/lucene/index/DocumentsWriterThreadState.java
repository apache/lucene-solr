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

/** Used by DocumentsWriter to maintain per-thread state.
 *  We keep a separate Posting hash and other state for each
 *  thread and then merge postings hashes from all threads
 *  when writing the segment. */
final class DocumentsWriterThreadState {

  boolean isIdle = true;                          // false if this is currently in use by a thread
  int numThreads = 1;                             // Number of threads that share this instance
  final DocConsumerPerThread consumer;
  final DocumentsWriter.DocState docState;

  final DocumentsWriter docWriter;

  public DocumentsWriterThreadState(DocumentsWriter docWriter) throws IOException {
    this.docWriter = docWriter;
    docState = new DocumentsWriter.DocState();
    docState.infoStream = docWriter.infoStream;
    docState.similarityProvider = docWriter.similarityProvider;
    docState.docWriter = docWriter;
    consumer = docWriter.consumer.addThread(this);
  }

  void doAfterFlush() {
    numThreads = 0;
  }
}
