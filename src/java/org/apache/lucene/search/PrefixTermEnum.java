package org.apache.lucene.search;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;

/**
 * Subclass of FilteredTermEnum for enumerating all terms that match the
 * specified prefix filter term.
 * <p>
 * Term enumerations are always ordered by Term.compareTo().  Each term in
 * the enumeration is greater than all that precede it.
 *
 */
public class PrefixTermEnum extends FilteredTermEnum {

  private final Term prefix;
  private boolean endEnum = false;

  public PrefixTermEnum(IndexReader reader, Term prefix) throws IOException {
    this.prefix = prefix;

    setEnum(reader.terms(new Term(prefix.field(), prefix.text())));
  }

  public float difference() {
    return 1.0f;
  }

  protected boolean endEnum() {
    return endEnum;
  }
  
  protected Term getPrefixTerm() {
      return prefix;
  }

  protected boolean termCompare(Term term) {
    if (term.field() == prefix.field() && term.text().startsWith(prefix.text())) {                                                                              
      return true;
    }
    endEnum = true;
    return false;
  }
}
