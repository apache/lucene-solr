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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An utility class to access to the term vectors.
 */
final class TermVectorsAdapter {

  private static Logger logger = LoggerFactory.getLogger(TermVectorsAdapter.class);

  private IndexReader reader;

  TermVectorsAdapter(@Nonnull IndexReader reader) {
    this.reader = reader;
  }

  /**
   * Returns the term vectors for the specified field in the specified document.
   * If no term vector is available for the field, empty list is returned.
   *
   * @param docid - document id
   * @param field - field name
   * @return list of term vector elements
   * @throws IOException
   */
  List<TermVectorEntry> getTermVector(int docid, String field) throws IOException {
    Terms termVector = reader.getTermVector(docid, field);
    if (termVector == null) {
      // no term vector available
      logger.warn("No term vector indexed for doc: #{} and field: {}", docid, field);
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
