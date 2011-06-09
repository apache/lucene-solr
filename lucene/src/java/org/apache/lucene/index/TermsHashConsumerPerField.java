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

/** Implement this class to plug into the TermsHash
 *  processor, which inverts & stores Tokens into a hash
 *  table and provides an API for writing bytes into
 *  multiple streams for each unique Token. */

import java.io.IOException;

abstract class TermsHashConsumerPerField {
  abstract boolean start(IndexableField[] fields, int count) throws IOException;
  abstract void finish() throws IOException;
  abstract void skippingLongTerm() throws IOException;
  abstract void start(IndexableField field);
  abstract void newTerm(int termID) throws IOException;
  abstract void addTerm(int termID) throws IOException;
  abstract int getStreamCount();

  abstract ParallelPostingsArray createPostingsArray(int size);
}
