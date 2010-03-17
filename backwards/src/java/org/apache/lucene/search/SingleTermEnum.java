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
 * Subclass of FilteredTermEnum for enumerating a single term.
 * <p>
 * This can be used by {@link MultiTermQuery}s that need only visit one term,
 * but want to preserve MultiTermQuery semantics such as
 * {@link MultiTermQuery#rewriteMethod}.
 */
public class SingleTermEnum extends FilteredTermEnum {
  private Term singleTerm;
  private boolean endEnum = false;
  
  /**
   * Creates a new <code>SingleTermEnum</code>.
   * <p>
   * After calling the constructor the enumeration is already pointing to the term,
   * if it exists.
   */
  public SingleTermEnum(IndexReader reader, Term singleTerm) throws IOException {
    super();
    this.singleTerm = singleTerm;
    setEnum(reader.terms(singleTerm));
  }

  @Override
  public float difference() {
    return 1.0F;
  }

  @Override
  protected boolean endEnum() {
    return endEnum;
  }

  @Override
  protected boolean termCompare(Term term) {
    if (term.equals(singleTerm)) {
      return true;
    } else {
      endEnum = true;
      return false;
    }
  }
}
