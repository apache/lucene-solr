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

import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexReader.AtomicReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Map;

/** @lucene.internal */
public class IDFValueSource extends DocFreqValueSource {
  public IDFValueSource(String field, String val, String indexedField, BytesRef indexedBytes) {
    super(field, val, indexedField, indexedBytes);
  }

  @Override
  public String name() {
    return "idf";
  }

  @Override
  public DocValues getValues(Map context, AtomicReaderContext readerContext) throws IOException {
    IndexSearcher searcher = (IndexSearcher)context.get("searcher");
    Similarity sim = searcher.getSimilarityProvider().get(field);
    // todo: we need docFreq that takes a BytesRef
    int docfreq = searcher.docFreq(new Term(indexedField, indexedBytes.utf8ToString()));
    float idf = sim.idf(docfreq, searcher.maxDoc());
    return new ConstDoubleDocValues(idf, this);
  }
}

