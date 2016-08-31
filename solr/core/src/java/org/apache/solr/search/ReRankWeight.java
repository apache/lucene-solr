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
package org.apache.solr.search;

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.Weight;

/**
 * A {@code Weight} used by reranking queries.
 */
public class ReRankWeight extends FilterWeight {

  final private IndexSearcher searcher;
  final private Rescorer reRankQueryRescorer;

  public ReRankWeight(Query mainQuery, Rescorer reRankQueryRescorer, IndexSearcher searcher, Weight mainWeight) throws IOException {
    super(mainQuery, mainWeight);
    this.searcher = searcher;
    this.reRankQueryRescorer = reRankQueryRescorer;
  }

  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    final Explanation mainExplain = in.explain(context, doc);
    return reRankQueryRescorer.explain(searcher, mainExplain, context.docBase+doc);
  }

}
