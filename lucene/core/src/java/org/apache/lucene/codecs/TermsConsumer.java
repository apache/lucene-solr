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

import org.apache.lucene.index.FieldInfo; // javadocs

import org.apache.lucene.util.BytesRef;

/**
 * Abstract API that consumes terms for an individual field.
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>TermsConsumer is returned for each field 
 *       by {@link PushFieldsConsumer#addField(FieldInfo)}.
 *   <li>TermsConsumer returns a {@link PostingsConsumer} for
 *       each term in {@link #startTerm(BytesRef)}.
 *   <li>When the producer (e.g. IndexWriter)
 *       is done adding documents for the term, it calls 
 *       {@link #finishTerm(BytesRef, TermStats)}, passing in
 *       the accumulated term statistics.
 *   <li>Producer calls {@link #finish(long, long, int)} with
 *       the accumulated collection statistics when it is finished
 *       adding terms to the field.
 * </ol>
 * 
 * @lucene.experimental
 */
public abstract class TermsConsumer {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected TermsConsumer() {
  }

  /** Starts a new term in this field; this may be called
   *  with no corresponding call to finish if the term had
   *  no docs. */
  public abstract PostingsConsumer startTerm(BytesRef text) throws IOException;

  /** Finishes the current term; numDocs must be > 0.
   *  <code>stats.totalTermFreq</code> will be -1 when term 
   *  frequencies are omitted for the field. */
  public abstract void finishTerm(BytesRef text, TermStats stats) throws IOException;

  /** Called when we are done adding terms to this field.
   *  <code>sumTotalTermFreq</code> will be -1 when term 
   *  frequencies are omitted for the field. */
  public abstract void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException;
}
