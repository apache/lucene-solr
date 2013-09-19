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
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.SegmentWriteState; // javadocs

/** 
 * Abstract API that consumes terms, doc, freq, prox, offset and
 * payloads postings.  Concrete implementations of this
 * actually do "something" with the postings (write it into
 * the index in a specific format).
 *
 * @lucene.experimental
 */

public abstract class FieldsConsumer {

  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected FieldsConsumer() {
  }

  // TODO: can we somehow compute stats for you...?

  // TODO: maybe we should factor out "limited" (only
  // iterables, no counts/stats) base classes from
  // Fields/Terms/Docs/AndPositions?

  /** Write all fields, terms and postings.  This the "pull"
   *  API, allowing you to iterate more than once over the
   *  postings, somewhat analogous to using a DOM API to
   *  traverse an XML tree.  Alternatively, if you subclass
   *  {@link PushFieldsConsumer}, then all postings are
   *  pushed in a single pass, somewhat analogous to using a
   *  SAX API to traverse an XML tree.
   *
   *  <p>This API is has certain restrictions vs {@link
   *  PushFieldsConsumer}:
   *
   *  <ul>
   *    <li> You must compute index statistics yourself,
   *         including each Term's docFreq and totalTermFreq,
   *         as well as the summary sumTotalTermFreq,
   *         sumTotalDocFreq and docCount.
   *
   *    <li> You must skip terms that have no docs and
   *         fields that have no terms, even though the provided
   *         Fields API will expose them; this typically
   *         requires lazily writing the field or term until
   *         you've actually seen the first term or
   *         document.
   *
   *    <li> The provided Fields instance is limited: you
   *         cannot call any methods that return
   *         statistics/counts; you cannot pass a non-null
   *         live docs when pulling docs/positions enums.
   *  </ul>
   */
  public abstract void write(Fields fields) throws IOException;
}
