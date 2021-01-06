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
package org.apache.lucene.search.spell;

import java.io.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.suggest.InputIterator;

/** Lucene Dictionary: terms taken from the given field of a Lucene index. */
public class LuceneDictionary implements Dictionary {
  private IndexReader reader;
  private String field;

  /**
   * Creates a new Dictionary, pulling source terms from the specified <code>field</code> in the
   * provided <code>reader</code>
   */
  public LuceneDictionary(IndexReader reader, String field) {
    this.reader = reader;
    this.field = field;
  }

  @Override
  public final InputIterator getEntryIterator() throws IOException {
    final Terms terms = MultiTerms.getTerms(reader, field);
    if (terms != null) {
      return new InputIterator.InputIteratorWrapper(terms.iterator());
    } else {
      return InputIterator.EMPTY;
    }
  }
}
