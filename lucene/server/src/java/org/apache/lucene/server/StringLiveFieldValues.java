package org.apache.lucene.server;

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

import java.io.IOException;
import java.util.Collections;

import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.index.StoredDocument;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.LiveFieldValues;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

// TODO: if there are more than one live field we could
// "bundle" them so we only do one stored doc lookup
// TODO: also be able to lookup from doc values

/** Implements live field values, for string values. */
public class StringLiveFieldValues extends LiveFieldValues<SearcherAndTaxonomy,String> {
  private final String idFieldName;
  private final String liveFieldName;

  /** Sole constructor. */
  public StringLiveFieldValues(SearcherTaxonomyManager mgr, String idFieldName, String liveFieldName) {
    super(mgr, "");
    this.idFieldName = idFieldName;
    this.liveFieldName = liveFieldName;
  }

  @Override
  protected String lookupFromSearcher(SearcherAndTaxonomy s, String id) throws IOException {

    TermQuery q = new TermQuery(new Term(idFieldName, id));
    TopDocs hits = s.searcher.search(q, 1);
    if (hits.totalHits == 0) {
      return null;
    } else if (hits.totalHits > 1) {
      throw new IllegalStateException("field \"" + idFieldName + "\"=\"" + id + "\" matches more than one document");
    } else {
      StoredDocument doc = s.searcher.doc(hits.scoreDocs[0].doc, Collections.singleton(liveFieldName));
      return doc.get(liveFieldName);
    }
  }
}
