package org.apache.lucene.index;

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
import java.util.Comparator;

import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.BytesRef;

/** Implements flex API (FieldsEnum/TermsEnum) on top of
 *  pre-flex API.  Used only for IndexReader impls outside
 *  Lucene's core. */
class LegacyTerms extends Terms {

  private final IndexReader r;
  private final String field;

  LegacyTerms(IndexReader r, String field) {
    this.r = r;
    this.field = StringHelper.intern(field);
  }

  @Override
  public TermsEnum iterator() throws IOException {
    return new LegacyFieldsEnum.LegacyTermsEnum(r, field);
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    // Pre-flex indexes always sorted in UTF16 order
    return BytesRef.getUTF8SortedAsUTF16Comparator();
  }
}

  
    
