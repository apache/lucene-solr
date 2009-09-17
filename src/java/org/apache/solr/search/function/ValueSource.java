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

package org.apache.solr.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.solr.search.function.DocValues;

import java.io.IOException;
import java.io.Serializable;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Instantiates {@link org.apache.solr.search.function.DocValues} for a particular reader.
 * <br>
 * Often used when creating a {@link FunctionQuery}.
 *
 * @version $Id$
 */
public abstract class ValueSource implements Serializable {

  @Deprecated
  public DocValues getValues(IndexReader reader) throws IOException {
    return getValues(null, reader);
  }

  /** Gets the values for this reader and the context that was previously
   * passed to createWeight()
   */
  public DocValues getValues(Map context, IndexReader reader) throws IOException {
    return null;
  }

  public abstract boolean equals(Object o);

  public abstract int hashCode();

  /** description of field, used in explain() */
  public abstract String description();

  public String toString() {
    return description();
  }

  /** Implementations should propagate createWeight to sub-ValueSources which can optionally store
   * weight info in the context. The context object will be passed to getValues()
   * where this info can be retrieved. */
  public void createWeight(Map context, Searcher searcher) throws IOException {
  }

  /** Returns a new non-threadsafe context map. */
  public static Map newContext() {
    return new IdentityHashMap();
  }
}


class ValueSourceScorer extends Scorer {
  protected IndexReader reader;
  private int doc = -1;
  protected final int maxDoc;
  protected final DocValues values;
  protected boolean checkDeletes;

  protected ValueSourceScorer(IndexReader reader, DocValues values) {
    super(null);
    this.reader = reader;
    this.maxDoc = reader.maxDoc();
    this.values = values;
    setCheckDeletes(true);
  }

  public IndexReader getReader() { return reader; }

  public void setCheckDeletes(boolean checkDeletes) {
    this.checkDeletes = checkDeletes && reader.hasDeletions();
  }

  public boolean matches(int doc) {
    return (!checkDeletes || !reader.isDeleted(doc)) && matchesValue(doc);
  }

  public boolean matchesValue(int doc) {
    return true;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() throws IOException {
    for(;;) {
      doc++;
      if (doc >= maxDoc) return doc=NO_MORE_DOCS;
      if (matches(doc)) return doc;
    }
  }

  @Override
  public int advance(int target) throws IOException {
    // also works fine when target==NO_MORE_DOCS
    doc = target-1;
    return nextDoc();
  }

  public int doc() {
    return doc;
  }

  public boolean next() {
    for(;;) {
      doc++;
      if (doc >= maxDoc) return false;
      if (matches(doc)) return true;
    }
  }

  public boolean skipTo(int target) {
    doc = target-1;
    return next();
  }


  public float score() throws IOException {
    return values.floatVal(doc);
  }

  public Explanation explain(int doc) throws IOException {
    return values.explain(doc);
  }
}


