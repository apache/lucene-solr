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
package org.apache.lucene.queries;

import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;

/**
 * @deprecated Use {@link org.apache.lucene.search.TermInSetQuery}
 */
@Deprecated
public class TermsQuery extends TermInSetQuery {

  /**
   * Creates a new {@link TermsQuery} from the given collection. It
   * can contain duplicate terms and multiple fields.
   */
  public TermsQuery(Collection<Term> terms) {
    super(terms);
  }

  /**
   * Creates a new {@link TermsQuery} from the given collection for
   * a single field. It can contain duplicate terms.
   */
  public TermsQuery(String field, Collection<BytesRef> terms) {
    super(field, terms);
  }

  /**
   * Creates a new {@link TermsQuery} from the given {@link BytesRef} array for
   * a single field.
   */
  public TermsQuery(String field, BytesRef...terms) {
    this(field, Arrays.asList(terms));
  }

  /**
   * Creates a new {@link TermsQuery} from the given array. The array can
   * contain duplicate terms and multiple fields.
   */
  public TermsQuery(final Term... terms) {
    this(Arrays.asList(terms));
  }


}
