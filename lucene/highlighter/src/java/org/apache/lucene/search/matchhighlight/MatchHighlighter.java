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
import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
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

  public TopHighlights highlight(Query query, TopDocs docs, Supplier<SnippetCollector> collectorSupplier) throws IOException {
    HighlightDoc[] highlights = new HighlightDoc[docs.scoreDocs.length];
    Weight weight = searcher.createNormalizedWeight(query, ScoreMode.COMPLETE_NO_SCORES);
    int i = 0;
    for (ScoreDoc doc : docs.scoreDocs) {
      int contextOrd = ReaderUtil.subIndex(doc.doc, searcher.getIndexReader().leaves());
      LeafReaderContext ctx = searcher.getIndexReader().leaves().get(contextOrd);
      Matches matches = weight.matches(ctx, doc.doc - ctx.docBase);
      HighlightingFieldVisitor visitor = new HighlightingFieldVisitor(new SourceAwareMatches(matches, analyzer), collectorSupplier.get());
      ctx.reader().document(doc.doc, visitor);
      highlights[i++] = new HighlightDoc(doc.doc, visitor.getHighlights());
    }
    return new TopHighlights(highlights);
  }

  private class HighlightingFieldVisitor extends StoredFieldVisitor {

    final SourceAwareMatches matches;
    final SnippetCollector collector;

    private HighlightingFieldVisitor(SourceAwareMatches matches, SnippetCollector collector) {
      this.matches = matches;
      this.collector = collector;
    }

    Document getHighlights() {
      return collector.getHighlights();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      return collector.needsField(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
      collector.collectSnippets(matches, fieldInfo.name, value);
    }
  }

}
