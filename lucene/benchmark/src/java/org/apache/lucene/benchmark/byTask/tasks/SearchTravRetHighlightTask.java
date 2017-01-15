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

package org.apache.lucene.benchmark.byTask.tasks;

import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.benchmark.byTask.PerfRunData;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.search.postingshighlight.PostingsHighlighter;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.BreakIteratorBoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.WeightedFragListBuilder;
import org.apache.lucene.util.ArrayUtil;

/**
 * Search and Traverse and Retrieve docs task.  Highlight the fields in the retrieved documents.
 *
 * <p>Note: This task reuses the reader if it is already open.
 * Otherwise a reader is opened at start and closed at the end.
 * </p>
 *
 * <p>Takes optional multivalued, comma separated param string as: type[&lt;enum&gt;],maxFrags[&lt;int&gt;],fields[name1;name2;...]</p>
 * <ul>
 * <li>type - the highlighter implementation, e.g. "UH"</li>
 * <li>maxFrags - The maximum number of fragments to score by the highlighter</li>
 * <li>fields - The fields to highlight.  If not specified all fields will be highlighted (or at least attempted)</li>
 * </ul>
 * Example:
 * <pre>"SearchHlgtSameRdr" SearchTravRetHighlight(type[UH],maxFrags[3],fields[body]) &gt; : 1000
 * </pre>
 *
 * Documents must be stored in order for this task to work.  Additionally, term vector positions can be used as well,
 * and offsets in postings is another option.
 *
 * <p>Other side effects: counts additional 1 (record) for each traversed hit,
 * and 1 more for each retrieved (non null) document and 1 for each fragment returned.</p>
 */
public class SearchTravRetHighlightTask extends SearchTravTask {
  private int maxDocCharsToAnalyze; // max leading content chars to highlight
  private int maxFrags = 1; // aka passages
  private Set<String> hlFields = Collections.singleton("body");
  private String type;
  private HLImpl hlImpl;
  private Analyzer analyzer;

  public SearchTravRetHighlightTask(PerfRunData runData) {
    super(runData);
  }

  @Override
  public void setParams(String params) {
    // can't call super because super doesn't understand our params syntax
    this.params = params;
    // TODO consider instead using data.getConfig().get("highlighter.*")?
    String[] splits = params.split(",");
    for (String split : splits) {
      if (split.startsWith("type[") == true) {
        type = split.substring("type[".length(), split.length() - 1);
      } else if (split.startsWith("maxFrags[") == true) {
        maxFrags = (int) Float.parseFloat(split.substring("maxFrags[".length(), split.length() - 1));
      } else if (split.startsWith("fields[") == true) {
        String fieldNames = split.substring("fields[".length(), split.length() - 1);
        String[] fieldSplits = fieldNames.split(";");
        hlFields = new HashSet<>(Arrays.asList(fieldSplits));
      }
    }
  }

  @Override
  public void setup() throws Exception {
    super.setup();
    //check to make sure either the doc is being stored
    PerfRunData data = getRunData();
    if (data.getConfig().get("doc.stored", false) == false){
      throw new Exception("doc.stored must be set to true");
    }
    maxDocCharsToAnalyze = data.getConfig().get("highlighter.maxDocCharsToAnalyze", Highlighter.DEFAULT_MAX_CHARS_TO_ANALYZE);
    analyzer = data.getAnalyzer();
    String type = this.type;
    if (type == null) {
      type = data.getConfig().get("highlighter", null);
    }
    switch (type) {
      case "NONE": hlImpl = new NoHLImpl(); break;
      case "SH_A": hlImpl = new StandardHLImpl(false); break;
      case "SH_V": hlImpl = new StandardHLImpl(true); break;

      case "FVH_V": hlImpl = new FastVectorHLImpl(); break;

      case "UH": hlImpl = new UnifiedHLImpl(null); break;
      case "UH_A": hlImpl = new UnifiedHLImpl(UnifiedHighlighter.OffsetSource.ANALYSIS); break;
      case "UH_V": hlImpl = new UnifiedHLImpl(UnifiedHighlighter.OffsetSource.TERM_VECTORS); break;
      case "UH_P": hlImpl = new UnifiedHLImpl(UnifiedHighlighter.OffsetSource.POSTINGS); break;
      case "UH_PV": hlImpl = new UnifiedHLImpl(UnifiedHighlighter.OffsetSource.POSTINGS_WITH_TERM_VECTORS); break;

      case "PH_P": hlImpl = new PostingsHLImpl(); break;

      default: throw new Exception("unrecognized highlighter type: " + type + " (try 'UH')");
    }
  }

  // here is where we intercept ReadTask's logic to do the highlighting, and nothing else (no retrieval of all field vals)
  @Override
  protected int withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
    hlImpl.withTopDocs(searcher, q, hits);
    // note: it'd be nice if we knew the sum kilobytes of text across these hits so we could return that. It'd be a more
    //  useful number to gauge the amount of work. But given "average" document sizes and lots of queries, returning the
    //  number of docs is reasonable.
    return hits.scoreDocs.length; // always return # scored docs.
  }

  private interface HLImpl {
    void withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception;
  }

  private volatile int preventOptimizeAway = 0;

  private class StandardHLImpl implements HLImpl {
    SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<em>", "</em>");
    DefaultEncoder encoder = new DefaultEncoder();
    Highlighter highlighter = new Highlighter(formatter, encoder, null);
    boolean termVecs;

    StandardHLImpl(boolean termVecs) {
      highlighter.setEncoder(new DefaultEncoder());
      highlighter.setMaxDocCharsToAnalyze(maxDocCharsToAnalyze);
      this.termVecs = termVecs;
    }

    @Override
    public void withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
      IndexReader reader = searcher.getIndexReader();
      highlighter.setFragmentScorer(new QueryScorer(q));
      // highlighter.setTextFragmenter();  unfortunately no sentence mechanism, not even regex. Default here is trivial
      for (ScoreDoc scoreDoc : docIdOrder(hits.scoreDocs)) {
        Document document = reader.document(scoreDoc.doc, hlFields);
        Fields tvFields = termVecs ? reader.getTermVectors(scoreDoc.doc) : null;
        for (IndexableField indexableField : document) {
          TokenStream tokenStream;
          if (termVecs) {
            tokenStream = TokenSources.getTokenStream(indexableField.name(), tvFields,
                indexableField.stringValue(), analyzer, maxDocCharsToAnalyze);
          } else {
            tokenStream = analyzer.tokenStream(indexableField.name(), indexableField.stringValue());
          }
          // will close TokenStream:
          String[] fragments = highlighter.getBestFragments(tokenStream, indexableField.stringValue(), maxFrags);
          preventOptimizeAway = fragments.length;
        }
      }
    }
  }

  private class FastVectorHLImpl implements HLImpl {
    int fragSize = 100;
    WeightedFragListBuilder fragListBuilder = new WeightedFragListBuilder();
    BoundaryScanner bs = new BreakIteratorBoundaryScanner(BreakIterator.getSentenceInstance(Locale.ENGLISH));
    ScoreOrderFragmentsBuilder fragmentsBuilder = new ScoreOrderFragmentsBuilder(bs);
    String[] preTags = {"<em>"};
    String[] postTags = {"</em>"};
    Encoder encoder = new DefaultEncoder();// new SimpleHTMLEncoder();
    FastVectorHighlighter highlighter = new FastVectorHighlighter(
        true,   // phraseHighlight
        false); // requireFieldMatch -- not pertinent to our benchmark

    @Override
    public void withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
      IndexReader reader = searcher.getIndexReader();
      final FieldQuery fq = highlighter.getFieldQuery( q, reader);
      for (ScoreDoc scoreDoc : docIdOrder(hits.scoreDocs)) {
        for (String hlField : hlFields) {
          String[] fragments = highlighter.getBestFragments(fq, reader, scoreDoc.doc, hlField, fragSize, maxFrags,
              fragListBuilder, fragmentsBuilder, preTags, postTags, encoder);
          preventOptimizeAway = fragments.length;
        }
      }
    }
  }

  private ScoreDoc[] docIdOrder(ScoreDoc[] scoreDocs) {
    ScoreDoc[] clone = new ScoreDoc[scoreDocs.length];
    System.arraycopy(scoreDocs, 0, clone, 0, scoreDocs.length);
    ArrayUtil.introSort(clone, (a, b) -> Integer.compare(a.doc, b.doc));
    return clone;
  }

  private class PostingsHLImpl implements HLImpl {
    PostingsHighlighter highlighter;
    String[] fields = hlFields.toArray(new String[hlFields.size()]);
    int[] maxPassages;
    PostingsHLImpl() {
      highlighter = new PostingsHighlighter(maxDocCharsToAnalyze) {
        @Override
        protected Analyzer getIndexAnalyzer(String field) { // thus support wildcards
          return analyzer;
        }

        @Override
        protected BreakIterator getBreakIterator(String field) {
          return BreakIterator.getSentenceInstance(Locale.ENGLISH);
        }
      };
      maxPassages = new int[hlFields.size()];
      Arrays.fill(maxPassages, maxFrags);
    }

    @Override
    public void withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
      Map<String, String[]> result = highlighter.highlightFields(fields, q, searcher, hits, maxPassages);
      preventOptimizeAway = result.size();
    }
  }

  private class UnifiedHLImpl implements HLImpl {
    UnifiedHighlighter highlighter;
    IndexSearcher lastSearcher;
    UnifiedHighlighter.OffsetSource offsetSource; // null means auto select
    String[] fields = hlFields.toArray(new String[hlFields.size()]);
    int[] maxPassages;

    UnifiedHLImpl(final UnifiedHighlighter.OffsetSource offsetSource) {
      this.offsetSource = offsetSource;
      maxPassages = new int[hlFields.size()];
      Arrays.fill(maxPassages, maxFrags);
    }

    private void reset(IndexSearcher searcher) {
      if (lastSearcher == searcher) {
        return;
      }
      lastSearcher = searcher;
      highlighter = new UnifiedHighlighter(searcher, analyzer) {
        @Override
        protected OffsetSource getOffsetSource(String field) {
          return offsetSource != null ? offsetSource : super.getOffsetSource(field);
        }
      };
      highlighter.setBreakIterator(() -> BreakIterator.getSentenceInstance(Locale.ENGLISH));
      highlighter.setMaxLength(maxDocCharsToAnalyze);
      highlighter.setHighlightPhrasesStrictly(true);
      highlighter.setHandleMultiTermQuery(true);
    }

    @Override
    public void withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
      reset(searcher);
      Map<String, String[]> result = highlighter.highlightFields(fields, q, hits, maxPassages);
      preventOptimizeAway = result.size();
    }
  }

  private class NoHLImpl implements HLImpl {

    @Override
    public void withTopDocs(IndexSearcher searcher, Query q, TopDocs hits) throws Exception {
      //just retrieve the HL fields
      for (ScoreDoc scoreDoc : docIdOrder(hits.scoreDocs)) {
        preventOptimizeAway += searcher.doc(scoreDoc.doc, hlFields).iterator().hasNext() ? 2 : 1;
      }
    }
  }
}
