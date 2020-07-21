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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Terms;
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

  public TopHighlights highlight(Query query, TopDocs docs, Supplier<PassageCollector> collectorSupplier) throws IOException {
    HighlightDoc[] highlights = new HighlightDoc[docs.scoreDocs.length];
    Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1);
    int i = 0;
    for (ScoreDoc doc : docs.scoreDocs) {
      PassageCollector collector = collectorSupplier.get();
      LeafReaderContext ctx = getReaderContext(doc.doc, collector);
      Matches matches = weight.matches(ctx, doc.doc - ctx.docBase);
      collector.setMatches(matches);
      HighlightingFieldVisitor visitor = new HighlightingFieldVisitor(collector);
      ctx.reader().document(doc.doc, visitor);
      highlights[i++] = new HighlightDoc(doc.doc, visitor.getHighlights());
    }
    return new TopHighlights(highlights);
  }

  private LeafReaderContext getReaderContext(int doc, PassageCollector collector) throws IOException {

    // If we have offsets stored in the index for the relevant fields, we can just use the
    // default reader context to get Matches from.  Otherwise, we need to replace the
    // Terms from the default context with one built either from term vectors, or from
    // the re-analyzed source

    int contextOrd = ReaderUtil.subIndex(doc, searcher.getIndexReader().leaves());
    LeafReaderContext defaultContext = searcher.getIndexReader().leaves().get(contextOrd);
    FieldInfos fis = defaultContext.reader().getFieldInfos();

    Map<String, Terms> fromTermVectors = new HashMap<>();
    Set<String> fromAnalysis = new HashSet<>();

    for (String field : collector.requiredFields()) {
      FieldInfo fi = fis.fieldInfo(field);
      if (fi != null && fi.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) < 0) {
        if (fi.hasVectors()) {
          Terms terms = defaultContext.reader().getTermVector(doc, field);
          if (terms != null && terms.hasOffsets()) {
            fromTermVectors.put(field, terms);
            continue;
          }
        }
        fromAnalysis.add(field);
      }
    }

    if (fromTermVectors.size() == 0 && fromAnalysis.size() == 0) {
      return defaultContext;
    }

    LeafReader reader = new OffsetsReader(defaultContext.reader(), doc - defaultContext.docBase, fromTermVectors, fromAnalysis, analyzer);
    return new LeafReaderContext(defaultContext.parent, reader, 0, 0, 0, 0);
  }

  private class HighlightingFieldVisitor extends StoredFieldVisitor {

    final PassageCollector collector;
    final Map<String, Integer> offsets = new HashMap<>();

    private HighlightingFieldVisitor(PassageCollector collector) {
      this.collector = collector;
    }

    Document getHighlights() {
      return collector.getHighlights();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
      return collector.requiredFields().contains(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
      String text = new String(value);
      collector.collectHighlights(fieldInfo.name, text, offsets.getOrDefault(fieldInfo.name, 0));
      offsets.compute(fieldInfo.name, (n, i) -> {
        if (i == null) {
          i = 0;
        }
        return i + text.length() + analyzer.getOffsetGap(n);
      });
    }
  }

}
