package org.apache.lucene.codecs;

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
import java.io.Closeable;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfo;

/**
 * @lucene.experimental
 */

// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PostingsWriterBase extends PostingsConsumer implements Closeable {

  public abstract void start(IndexOutput termsOut) throws IOException;

  public abstract void startTerm() throws IOException;

  /** Flush count terms starting at start "backwards", as a
   *  block. start is a negative offset from the end of the
   *  terms stack, ie bigger start means further back in
   *  the stack. */
  public abstract void flushTermsBlock(int start, int count) throws IOException;

  /** Finishes the current term */
  public abstract void finishTerm(TermStats stats) throws IOException;

  public abstract void setField(FieldInfo fieldInfo);

  public abstract void close() throws IOException;
}
