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

package org.apache.lucene.luke.models.documents;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.luke.util.LoggerFactory;

/**
 * An utility class to access to the term vectors.
 */
final class TermVectorsAdapter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private IndexReader reader;

  TermVectorsAdapter(IndexReader reader) {
    this.reader = Objects.requireNonNull(reader);
  }

  /**
   * Returns the term vectors for the specified field in the specified document.
   * If no term vector is available for the field, empty list is returned.
   *
   * @param docid - document id
   * @param field - field name
   * @return list of term vector elements
   * @throws IOException - if there is a low level IO error.
   */
  List<TermVectorEntry> getTermVector(int docid, String field) throws IOException {
    Terms termVector = reader.getTermVector(docid, field);
    if (termVector == null) {
      // no term vector available
      log.warn("No term vector indexed for doc: #{} and field: {}", docid, field);
      return Collections.emptyList();
    }

    List<TermVectorEntry> res = new ArrayList<>();
    TermsEnum te = termVector.iterator();
    while (te.next() != null) {
      res.add(TermVectorEntry.of(te));
    }
    return res;
  }

}
