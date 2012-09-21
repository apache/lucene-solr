package org.apache.lucene.codecs;

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
import java.io.Closeable;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.index.FieldInfo;

/**
 * Extension of {@link PostingsConsumer} to support pluggable term dictionaries.
 * <p>
 * This class contains additional hooks to interact with the provided
 * term dictionaries such as {@link BlockTreeTermsWriter}. If you want
 * to re-use an existing implementation and are only interested in
 * customizing the format of the postings list, extend this class
 * instead.
 * 
 * @see PostingsReaderBase
 * @lucene.experimental
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PostingsWriterBase extends PostingsConsumer implements Closeable {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected PostingsWriterBase() {
  }

  /** Called once after startup, before any terms have been
   *  added.  Implementations typically write a header to
   *  the provided {@code termsOut}. */
  public abstract void start(IndexOutput termsOut) throws IOException;

  /** Start a new term.  Note that a matching call to {@link
   *  #finishTerm(TermStats)} is done, only if the term has at least one
   *  document. */
  public abstract void startTerm() throws IOException;

  /** Flush count terms starting at start "backwards", as a
   *  block. start is a negative offset from the end of the
   *  terms stack, ie bigger start means further back in
   *  the stack. */
  public abstract void flushTermsBlock(int start, int count) throws IOException;

  /** Finishes the current term.  The provided {@link
   *  TermStats} contains the term's summary statistics. */
  public abstract void finishTerm(TermStats stats) throws IOException;

  /** Called when the writing switches to another field. */
  public abstract void setField(FieldInfo fieldInfo);

  @Override
  public abstract void close() throws IOException;
}
