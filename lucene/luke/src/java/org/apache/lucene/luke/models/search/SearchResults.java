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

package org.apache.lucene.luke.models.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TotalHits;

/** Holder for a search result page. */
public final class SearchResults {

  private TotalHits totalHits;

  private int offset = 0;

  private List<Doc> hits = new ArrayList<>();

  /**
   * Creates a search result page for the given raw Lucene hits.
   *
   * @param totalHits - total number of hits for this query
   * @param docs - array of hits
   * @param offset - offset of the current page
   * @param searcher - index searcher
   * @param fieldsToLoad - fields to load
   * @return the search result page
   * @throws IOException - if there is a low level IO error.
   */
  static SearchResults of(
      TotalHits totalHits,
      ScoreDoc[] docs,
      int offset,
      IndexSearcher searcher,
      Set<String> fieldsToLoad)
      throws IOException {
    SearchResults res = new SearchResults();

    res.totalHits = Objects.requireNonNull(totalHits);
    Objects.requireNonNull(docs);
    Objects.requireNonNull(searcher);

    for (ScoreDoc sd : docs) {
      Document luceneDoc =
          (fieldsToLoad == null) ? searcher.doc(sd.doc) : searcher.doc(sd.doc, fieldsToLoad);
      res.hits.add(Doc.of(sd.doc, sd.score, luceneDoc));
      res.offset = offset;
    }

    return res;
  }

  /** Returns the total number of hits for this query. */
  public TotalHits getTotalHits() {
    return totalHits;
  }

  /** Returns the offset of the current page. */
  public int getOffset() {
    return offset;
  }

  /** Returns the documents of the current page. */
  public List<Doc> getHits() {
    return List.copyOf(hits);
  }

  /** Returns the size of the current page. */
  public int size() {
    return hits.size();
  }

  private SearchResults() {}

  /** Holder for a hit. */
  public static class Doc {
    private int docId;
    private float score;
    private Map<String, String[]> fieldValues = new HashMap<>();

    /**
     * Creates a hit.
     *
     * @param docId - document id
     * @param score - score of this document for the query
     * @param luceneDoc - raw Lucene document
     * @return the hit
     */
    static Doc of(int docId, float score, Document luceneDoc) {
      Objects.requireNonNull(luceneDoc);

      Doc doc = new Doc();
      doc.docId = docId;
      doc.score = score;
      Set<String> fields =
          luceneDoc.getFields().stream().map(IndexableField::name).collect(Collectors.toSet());
      for (String f : fields) {
        doc.fieldValues.put(f, luceneDoc.getValues(f));
      }
      return doc;
    }

    /** Returns the document id. */
    public int getDocId() {
      return docId;
    }

    /** Returns the score of this document for the current query. */
    public float getScore() {
      return score;
    }

    /** Returns the field data of this document. */
    public Map<String, String[]> getFieldValues() {
      return Map.copyOf(fieldValues);
    }

    private Doc() {}
  }
}
