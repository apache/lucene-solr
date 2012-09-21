package org.apache.lucene.facet.search;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

import org.apache.lucene.util.UnsafeByteArrayInputStream;
import org.apache.lucene.util.encoding.IntDecoder;

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

/**
 * A payload deserializer comes with its own working space (buffer). One need to
 * define the {@link IndexReader} and {@link Term} in which the payload resides.
 * The iterator then consumes the payload information of each document and
 * decodes it into categories. A typical use case of this class is:
 * 
 * <pre class="prettyprint">
 * IndexReader reader = [open your reader];
 * Term t = new Term(&quot;field&quot;, &quot;where-payload-exists&quot;);
 * CategoryListIterator cli = new PayloadIntDecodingIterator(reader, t);
 * if (!cli.init()) {
 *   // it means there are no payloads / documents associated with that term.
 *   // Usually a sanity check. However, init() must be called.
 * }
 * DocIdSetIterator disi = [you usually iterate on something else, such as a Scorer];
 * int doc;
 * while ((doc = disi.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
 *   cli.setdoc(doc);
 *   long category;
 *   while ((category = cli.nextCategory()) &lt; Integer.MAX_VALUE) {
 *   }
 * }
 * </pre>
 * 
 * @lucene.experimental
 */
public class PayloadIntDecodingIterator implements CategoryListIterator {

  private final UnsafeByteArrayInputStream ubais;
  private final IntDecoder decoder;

  private final IndexReader indexReader;
  private final Term term;
  private final PayloadIterator pi;
  private final int hashCode;
  
  public PayloadIntDecodingIterator(IndexReader indexReader, Term term, IntDecoder decoder)
      throws IOException {
    this(indexReader, term, decoder, new byte[1024]);
  }

  public PayloadIntDecodingIterator(IndexReader indexReader, Term term, IntDecoder decoder,
                                    byte[] buffer) throws IOException {
    pi = new PayloadIterator(indexReader, term, buffer);
    ubais = new UnsafeByteArrayInputStream();
    this.decoder = decoder;
    hashCode = indexReader.hashCode() ^ term.hashCode();
    this.term = term;
    this.indexReader = indexReader;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PayloadIntDecodingIterator)) {
      return false;
    }
    PayloadIntDecodingIterator that = (PayloadIntDecodingIterator) other;
    if (hashCode != that.hashCode) {
      return false;
    }
    
    // Hash codes are the same, check equals() to avoid cases of hash-collisions.
    return indexReader.equals(that.indexReader) && term.equals(that.term);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  public boolean init() throws IOException {
    return pi.init();
  }
  
  public long nextCategory() throws IOException {
    return decoder.decode();
  }

  public boolean skipTo(int docId) throws IOException {
    if (!pi.setdoc(docId)) {
      return false;
    }

    // Initializing the decoding mechanism with the new payload data
    ubais.reInit(pi.getBuffer(), 0, pi.getPayloadLength());
    decoder.reInit(ubais);
    return true;
  }

}
