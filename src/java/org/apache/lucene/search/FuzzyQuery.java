package org.apache.lucene.search;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.lucene.index.Term;
import java.io.IOException;

/** Implements the fuzzy search query */
public final class FuzzyQuery extends MultiTermQuery {
  public FuzzyQuery(Term term) {
    super(term);
  }
    
  protected FilteredTermEnum getEnum(IndexReader reader) throws IOException {
    return new FuzzyTermEnum(reader, getTerm());
  }
    
  public String toString(String field) {
    return super.toString(field) + '~';
  }
}
