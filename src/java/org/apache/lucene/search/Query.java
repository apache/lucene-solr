package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import java.io.IOException;
import java.util.Hashtable;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

/** The abstract base class for queries.
  <p>Instantiable subclasses are:
  <ul>
  <li> {@link TermQuery}
  <li> {@link PhraseQuery}
  <li> {@link BooleanQuery}
  </ul>
  <p>A parser for queries is contained in:
  <ul>
  <li><a href="doc/lucene.queryParser.QueryParser.html">QueryParser</a>
  </ul>
  */
abstract public class Query {

  // query weighting
  abstract float sumOfSquaredWeights(Searcher searcher) throws IOException;
  abstract void normalize(float norm);

  // query evaluation
  abstract Scorer scorer(IndexReader reader) throws IOException;

  void prepare(IndexReader reader) {}

  static Scorer scorer(Query query, Searcher searcher, IndexReader reader)
    throws IOException {
    query.prepare(reader);
    float sum = query.sumOfSquaredWeights(searcher);
    float norm = 1.0f / (float)Math.sqrt(sum);
    query.normalize(norm);
    return query.scorer(reader);
  }

  /** Prints a query to a string, with <code>field</code> as the default field
    for terms.
    <p>The representation used is one that is readable by
    <a href="doc/lucene.queryParser.QueryParser.html">QueryParser</a>
    (although, if the query was created by the parser, the printed
    representation may not be exactly what was parsed). */
  abstract public String toString(String field);
}
