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

package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;

public class MatchHighlighter {

  private final IndexSearcher searcher;
  private final Analyzer analyzer;

  public MatchHighlighter(IndexSearcher searcher, Analyzer analyzer) {
    this.searcher = searcher;
    this.analyzer = analyzer;
  }

  public <T> TopHighlights<T> highlight(Query query, TopDocs docs, SnippetGenerator<T> generator) throws IOException {
    List<HighlightDoc<T>> highlights = new ArrayList<>();
    Weight weight = searcher.createNormalizedWeight(query, ScoreMode.COMPLETE);
    for (ScoreDoc doc : docs.scoreDocs) {
      int contextOrd = ReaderUtil.subIndex(doc.doc, searcher.getIndexReader().leaves());
      LeafReaderContext ctx = searcher.getIndexReader().leaves().get(contextOrd);
      HighlightingFieldVisitor<T> visitor = new HighlightingFieldVisitor<>(weight, ctx, doc.doc - ctx.docBase, generator);
      ctx.reader().document(doc.doc, visitor);
      highlights.add(visitor.getHighights());
    }
    return new TopHighlights<>(highlights);
  }

  private class HighlightingFieldVisitor<T> extends StoredFieldVisitor {

    final LeafReaderContext context;
    final int doc;
    final SnippetGenerator<T> generator;
    final Weight weight;

    final HighlightDoc<T> highlights;

    private HighlightingFieldVisitor(Weight weight, LeafReaderContext context, int doc, SnippetGenerator<T> generator) {
      this.context = context;
      this.doc = doc;
      this.generator = generator;
      this.weight = weight;
      this.highlights = new HighlightDoc<>(doc, fields);
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return generator.needsField(fieldInfo.name);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
      String field = fieldInfo.name;
      MatchesIterator matches = weight.matches(context, doc, field);
      if (matches != null) {
        this.highlights.highlights.put(field, generator.getSnippets(field, value, matches));
      }
    }
  }

}
