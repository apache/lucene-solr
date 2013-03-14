package org.apache.lucene.queries.function;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * {@link Scorer} which returns the result of {@link FunctionValues#floatVal(int)} as
 * the score for a document.
 */
public class ValueSourceScorer extends Scorer {
  protected final IndexReader reader;
  private int doc = -1;
  protected final int maxDoc;
  protected final FunctionValues values;
  protected boolean checkDeletes;
  private final Bits liveDocs;

  protected ValueSourceScorer(IndexReader reader, FunctionValues values) {
    super(null);
    this.reader = reader;
    this.maxDoc = reader.maxDoc();
    this.values = values;
    setCheckDeletes(true);
    this.liveDocs = MultiFields.getLiveDocs(reader);
  }

  public IndexReader getReader() {
    return reader;
  }

  public void setCheckDeletes(boolean checkDeletes) {
    this.checkDeletes = checkDeletes && reader.hasDeletions();
  }

  public boolean matches(int doc) {
    return (!checkDeletes || liveDocs.get(doc)) && matchesValue(doc);
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
    for (; ;) {
      doc++;
      if (doc >= maxDoc) return doc = NO_MORE_DOCS;
      if (matches(doc)) return doc;
    }
  }

  @Override
  public int advance(int target) throws IOException {
    // also works fine when target==NO_MORE_DOCS
    doc = target - 1;
    return nextDoc();
  }

  @Override
  public float score() throws IOException {
    return values.floatVal(doc);
  }

  @Override
  public int freq() throws IOException {
    return 1;
  }

  @Override
  public long cost() {
    return maxDoc;
  }
}
