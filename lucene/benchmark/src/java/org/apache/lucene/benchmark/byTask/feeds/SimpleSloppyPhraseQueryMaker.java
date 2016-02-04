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
package org.apache.lucene.benchmark.byTask.feeds;


import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;

/**
 * Create sloppy phrase queries for performance test, in an index created using simple doc maker.
 */
public class SimpleSloppyPhraseQueryMaker extends SimpleQueryMaker {

  /* (non-Javadoc)
   * @see org.apache.lucene.benchmark.byTask.feeds.SimpleQueryMaker#prepareQueries()
   */
  @Override
  protected Query[] prepareQueries() throws Exception {
    // extract some 100 words from doc text to an array
    String words[];
    ArrayList<String> w = new ArrayList<>();
    StringTokenizer st = new StringTokenizer(SingleDocSource.DOC_TEXT);
    while (st.hasMoreTokens() && w.size()<100) {
      w.add(st.nextToken());
    }
    words = w.toArray(new String[0]);

    // create queries (that would find stuff) with varying slops
    ArrayList<Query> queries = new ArrayList<>();
    for (int slop=0; slop<8; slop++) {
      for (int qlen=2; qlen<6; qlen++) {
        for (int wd=0; wd<words.length-qlen-slop; wd++) {
          // ordered
          int remainedSlop = slop;
          int wind = wd;
          PhraseQuery.Builder builder = new PhraseQuery.Builder();
          for (int i=0; i<qlen; i++) {
            builder.add(new Term(DocMaker.BODY_FIELD, words[wind++]), i);
            if (remainedSlop>0) {
              remainedSlop--;
              wind++;
            }
          }
          builder.setSlop(slop);
          PhraseQuery q = builder.build();
          queries.add(q);
          // reversed
          remainedSlop = slop;
          wind = wd+qlen+remainedSlop-1;
          builder = new PhraseQuery.Builder();
          for (int i=0; i<qlen; i++) {
            builder.add(new Term(DocMaker.BODY_FIELD, words[wind--]), i);
            if (remainedSlop>0) {
              remainedSlop--;
              wind--;
            }
          }
          builder.setSlop(slop + 2 * qlen);
          q = builder.build();
          queries.add(q);
        }
      }
    }
    return queries.toArray(new Query[0]);
  }

}
