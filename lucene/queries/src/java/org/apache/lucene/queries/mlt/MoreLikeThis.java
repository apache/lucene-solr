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
package org.apache.lucene.queries.mlt;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.PriorityQueue;

/**
 * Generate "more like this" similarity queries.
 * Based on this mail:
 * <pre><code>
 * Lucene does let you access the document frequency of terms, with IndexReader.docFreq().
 * Term frequencies can be computed by re-tokenizing the text, which, for a single document,
 * is usually fast enough.  But looking up the docFreq() of every term in the document is
 * probably too slow.
 * 
 * You can use some heuristics to prune the set of terms, to avoid calling docFreq() too much,
 * or at all.  Since you're trying to maximize a tf*idf score, you're probably most interested
 * in terms with a high tf. Choosing a tf threshold even as low as two or three will radically
 * reduce the number of terms under consideration.  Another heuristic is that terms with a
 * high idf (i.e., a low df) tend to be longer.  So you could threshold the terms by the
 * number of characters, not selecting anything less than, e.g., six or seven characters.
 * With these sorts of heuristics you can usually find small set of, e.g., ten or fewer terms
 * that do a pretty good job of characterizing a document.
 * 
 * It all depends on what you're trying to do.  If you're trying to eek out that last percent
 * of precision and recall regardless of computational difficulty so that you can win a TREC
 * competition, then the techniques I mention above are useless.  But if you're trying to
 * provide a "more like this" button on a search results page that does a decent job and has
 * good performance, such techniques might be useful.
 * 
 * An efficient, effective "more-like-this" query generator would be a great contribution, if
 * anyone's interested.  I'd imagine that it would take a Reader or a String (the document's
 * text), analyzer Analyzer, and return a set of representative terms using heuristics like those
 * above.  The frequency and length thresholds could be parameters, etc.
 * 
 * Doug
 * </code></pre>
 * <h3>Initial Usage</h3>
 * <p>
 * This class has lots of options to try to make it efficient and flexible.
 * The simplest possible usage is as follows. The bold
 * fragment is specific to this class.
 * <br>
 * <pre class="prettyprint">
 * IndexReader ir = ...
 * IndexSearcher is = ...
 *
 * MoreLikeThis mlt = new MoreLikeThis(ir);
 * Reader target = ... // orig source of doc you want to find similarities to
 * Query query = mlt.like( target);
 * 
 * Hits hits = is.search(query);
 * // now the usual iteration thru 'hits' - the only thing to watch for is to make sure
 * //you ignore the doc if it matches your 'target' document, as it should be similar to itself
 *
 * </pre>
 * <p>
 * Thus you:
 * <ol>
 * <li> do your normal, Lucene setup for searching,
 * <li> create a MoreLikeThis,
 * <li> get the text of the doc you want to find similarities to
 * <li> then call one of the like() calls to generate a similarity query
 * <li> call the searcher to find the similar docs
 * </ol>
 * <br>
 * <h3>More Advanced Usage</h3>
 * <p>
 * You may want to see the parameters class
 * <br>
 * <hr>
 * <pre>
 * Changes: Mark Harwood 29/02/04
 * Some bugfixing, some refactoring, some optimisation.
 * - bugfix: retrieveTerms(int docNum) was not working for indexes without a termvector -added missing code
 * - bugfix: No significant terms being created for fields with a termvector - because
 * was only counting one occurrence per term/field pair in calculations(ie not including frequency info from TermVector)
 * - refactor: moved common code into isNoiseWord()
 * - optimise: when no termvector support available - used maxNumTermsParsed to limit amount of tokenization
 * </pre>
 */
public final class MoreLikeThis {
  /**
   * All tunable parameters that regulates the query building
   */
  private MoreLikeThisParameters parameters;

  /**
   * For idf() calculations.
   */
  private TFIDFSimilarity similarity;// = new DefaultSimilarity();

  /**
   * IndexReader to use
   */
  private final IndexReader ir;

  /**
   * Constructor requiring an IndexReader.
   */
  public MoreLikeThis(IndexReader ir) {
    this(ir, new ClassicSimilarity());
  }

  public MoreLikeThis(IndexReader ir, TFIDFSimilarity sim) {
    this.parameters = new MoreLikeThisParameters();
    this.ir = ir;
    this.similarity = sim;
  }

  public MoreLikeThisParameters getParameters() {
    return parameters;
  }

  public TFIDFSimilarity getSimilarity() {
    return similarity;
  }

  public void setSimilarity(TFIDFSimilarity similarity) {
    this.similarity = similarity;
  }

  /**
   * Return a query that will return docs like the passed lucene document ID.
   *
   * @param docNum the documentID of the lucene doc to generate the 'More Like This" query for.
   * @return a query that will return docs like the passed lucene document ID.
   */
  public Query like(int docNum) throws IOException {
    return createQuery(retrieveTerms(docNum));
  }

  /**
   * 
   * @param filteredDocument Document with field values extracted for selected fields.
   * @return More Like This query for the passed document.
   */
  public Query like(Map<String, Collection<Object>> filteredDocument) throws IOException {
    return createQuery(retrieveTerms(filteredDocument));
  }

  /**
   * Return a query that will return docs like the passed Readers.
   * This was added in order to treat multi-value fields.
   *
   * @return a query that will return docs like the passed Readers.
   */
  public Query like(String fieldName, Reader... readers) throws IOException {
    Map<String, Map<String, Int>> perFieldTermFrequencies = new HashMap<>();
    for (Reader r : readers) {
      addTermFrequencies(r, perFieldTermFrequencies, fieldName);
    }
    return createQuery(createQueue(perFieldTermFrequencies));
  }

  /**
   * Create the More like query from a PriorityQueue
   */
  private Query createQuery(PriorityQueue<ScoreTerm> q) {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    MoreLikeThisParameters.BoostProperties boostConfiguration = parameters.getBoostConfiguration();
    ScoreTerm scoreTerm;
    float bestScore = -1;

    while ((scoreTerm = q.pop()) != null) {
      Query tq = new TermQuery(new Term(scoreTerm.topField, scoreTerm.word));

      if (boostConfiguration.isBoostByTermScore()) {
        if (bestScore == -1) {
          bestScore = (scoreTerm.score);
        }
        float myScore = (scoreTerm.score);
        float fieldBoost = boostConfiguration.getFieldBoost(scoreTerm.topField);
        tq = new BoostQuery(tq, fieldBoost * myScore / bestScore);
      }

      try {
        query.add(tq, BooleanClause.Occur.SHOULD);
      }
      catch (BooleanQuery.TooManyClauses ignore) {
        break;
      }
    }
    return query.build();
  }

  /**
   * Create a PriorityQueue from a word-&gt;tf map.
   *
   * @param perFieldTermFrequencies a per field map of words keyed on the word(String) with Int objects as the values.
   */
  private PriorityQueue<ScoreTerm> createQueue(Map<String, Map<String, Int>> perFieldTermFrequencies) throws IOException {
    // have collected all words in doc and their freqs
    int numDocs = ir.numDocs();
    int maxQueryTerms = parameters.getMaxQueryTerms();
    int minTermFreq = parameters.getMinTermFreq();
    int minDocFreq = parameters.getMinDocFreq();
    int maxDocFreq = parameters.getMaxDocFreq();
    final int limit = Math.min(maxQueryTerms, this.getTermsCount(perFieldTermFrequencies));
    FreqQ queue = new FreqQ(limit); // will order words by score
    
    for (Map.Entry<String, Map<String, Int>> entry : perFieldTermFrequencies.entrySet()) {
      Map<String, Int> perWordTermFrequencies = entry.getValue();
      String fieldName = entry.getKey();

      for (Map.Entry<String, Int> tfEntry : perWordTermFrequencies.entrySet()) { // for every word
        String word = tfEntry.getKey();
        int tf = tfEntry.getValue().x; // term freq in the source doc
        if (minTermFreq > 0 && tf < minTermFreq) {
          continue; // filter out words that don't occur enough times in the source
        }

        int docFreq = ir.docFreq(new Term(fieldName, word));

        if (minDocFreq > 0 && docFreq < minDocFreq) {
          continue; // filter out words that don't occur in enough docs
        }

        if (docFreq > maxDocFreq) {
          continue; // filter out words that occur in too many docs
        }

        if (docFreq == 0) {
          continue; // index update problem?
        }

        float idf = similarity.idf(docFreq, numDocs);
        float score = tf * idf;

        if (queue.size() < limit) {
          // there is still space in the queue
          queue.add(new ScoreTerm(word, fieldName, score, idf, docFreq, tf));
        } else {
          ScoreTerm term = queue.top();
          if (term.score < score) { // update the smallest in the queue in place and update the queue.
            term.update(word, fieldName, score, idf, docFreq, tf);
            queue.updateTop();
          }
        }
      }
    }
    return queue;
  }

  private int getTermsCount(Map<String, Map<String, Int>> perFieldTermFrequencies) {
    int totalTermsCount = 0;
    Collection<Map<String, Int>> values = perFieldTermFrequencies.values();
    for (Map<String, Int> perWordTermFrequencies : values) {
      totalTermsCount += perWordTermFrequencies.size();
    }
    return totalTermsCount;
  }

  /**
   * Find words for a more-like-this query former.
   *
   * @param docNum the id of the lucene document from which to find terms
   */
  private PriorityQueue<ScoreTerm> retrieveTerms(int docNum) throws IOException {
    Map<String, Map<String, Int>> field2termFreqMap = new HashMap<>();
    for (String fieldName : parameters.getFieldNamesOrInit(ir)) {
      final Fields vectors = ir.getTermVectors(docNum);
      final Terms vector;
      if (vectors != null) {
        vector = vectors.terms(fieldName);
      } else {
        vector = null;
      }

      // field does not store term vector info
      if (vector == null) {
        Document d = ir.document(docNum);
        IndexableField[] fields = d.getFields(fieldName);
        for (IndexableField field : fields) {
          final String stringValue = field.stringValue();
          if (stringValue != null) {
            addTermFrequencies(new StringReader(stringValue), field2termFreqMap, fieldName);
          }
        }
      } else {
        addTermFrequencies(field2termFreqMap, vector, fieldName);
      }
    }

    return createQueue(field2termFreqMap);
  }


  private PriorityQueue<ScoreTerm> retrieveTerms(Map<String, Collection<Object>> field2fieldValues) throws
      IOException {
    Map<String, Map<String, Int>> field2termFreqMap = new HashMap<>();
    for (String fieldName : parameters.getFieldNamesOrInit(ir)) {
      for (String field : field2fieldValues.keySet()) {
        Collection<Object> fieldValues = field2fieldValues.get(field);
        if(fieldValues == null)
          continue;
        for(Object fieldValue:fieldValues) {
          if (fieldValue != null) {
            addTermFrequencies(new StringReader(String.valueOf(fieldValue)), field2termFreqMap,
                fieldName);
          }
        }
      }
    }
    return createQueue(field2termFreqMap);
  }
  /**
   * Adds terms and frequencies found in vector into the Map termFreqMap
   *
   * @param field2termFreqMap a Map of terms and their frequencies per field
   * @param vector List of terms and their frequencies for a doc/field
   */
  private void addTermFrequencies(Map<String, Map<String, Int>> field2termFreqMap, Terms vector, String fieldName) throws IOException {
    Map<String, Int> termFreqMap = field2termFreqMap.computeIfAbsent(fieldName, k -> new HashMap<>());
    final TermsEnum termsEnum = vector.iterator();
    final CharsRefBuilder spare = new CharsRefBuilder();
    BytesRef text;
    while((text = termsEnum.next()) != null) {
      spare.copyUTF8Bytes(text);
      final String term = spare.toString();
      if (isNoiseWord(term)) {
        continue;
      }
      final int freq = (int) termsEnum.totalTermFreq();

      // increment frequency
      Int cnt = termFreqMap.get(term);
      if (cnt == null) {
        cnt = new Int();
        termFreqMap.put(term, cnt);
        cnt.x = freq;
      } else {
        cnt.x += freq;
      }
    }
  }

  /**
   * Adds term frequencies found by tokenizing text from reader into the Map words
   *
   * @param r a source of text to be tokenized
   * @param perFieldTermFrequencies a Map of terms and their frequencies per field
   * @param fieldName Used by analyzer for any special per-field analysis
   */
  private void addTermFrequencies(Reader r, Map<String, Map<String, Int>> perFieldTermFrequencies, String fieldName)
      throws IOException {
    Analyzer analyzer = parameters.getAnalyzer();
    if (analyzer == null) {
      throw new UnsupportedOperationException("To use MoreLikeThis without " +
          "term vectors, you must provide an Analyzer");
    }
    Map<String, Int> termFreqMap = perFieldTermFrequencies.computeIfAbsent(fieldName, k -> new HashMap<>());
    try (TokenStream ts = analyzer.tokenStream(fieldName, r)) {
      int tokenCount = 0;
      // for every token
      CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
      ts.reset();
      while (ts.incrementToken()) {
        String word = termAtt.toString();
        tokenCount++;
        if (tokenCount > parameters.getMaxNumTokensParsed()) {
          break;
        }
        if (isNoiseWord(word)) {
          continue;
        }

        // increment frequency
        Int cnt = termFreqMap.get(word);
        if (cnt == null) {
          termFreqMap.put(word, new Int());
        } else {
          cnt.x++;
        }
      }
      ts.end();
    }
  }


  /**
   * determines if the passed term is likely to be of interest in "more like" comparisons
   *
   * @param term The word being considered
   * @return true if should be ignored, false if should be used in further analysis
   */
  private boolean isNoiseWord(String term) {
    int len = term.length();
    if (parameters.getMinWordLen() > 0 && len < parameters.getMinWordLen()) {
      return true;
    }
    if (parameters.getMaxWordLen() > 0 && len > parameters.getMaxWordLen()) {
      return true;
    }
    return parameters.getStopWords() != null && parameters.getStopWords().contains(term);
  }


  /**
   * Find words for a more-like-this query former.
   * The result is a priority queue of arrays with one entry for <b>every word</b> in the document.
   * Each array has 6 elements.
   * The elements are:
   * <ol>
   * <li> The word (String)
   * <li> The top field that this word comes from (String)
   * <li> The score for this word (Float)
   * <li> The IDF value (Float)
   * <li> The frequency of this word in the index (Integer)
   * <li> The frequency of this word in the source document (Integer)
   * </ol>
   * This is a somewhat "advanced" routine, and in general only the 1st entry in the array is of interest.
   * This method is exposed so that you can identify the "interesting words" in a document.
   * For an easier method to call see {@link #retrieveInterestingTerms retrieveInterestingTerms()}.
   *
   * @param r the reader that has the content of the document
   * @param fieldName field passed to the analyzer to use when analyzing the content
   * @return the most interesting words in the document ordered by score, with the highest scoring, or best entry, first
   * @see #retrieveInterestingTerms
   */
  private PriorityQueue<ScoreTerm> retrieveTerms(Reader r, String fieldName) throws IOException {
    Map<String, Map<String, Int>> field2termFreqMap = new HashMap<>();
    addTermFrequencies(r, field2termFreqMap, fieldName);
    return createQueue(field2termFreqMap);
  }

  /**
   * @see #retrieveInterestingTerms(java.io.Reader, String)
   */
  public String[] retrieveInterestingTerms(int docNum) throws IOException {
    int maxQueryTerms = parameters.getMaxQueryTerms();
    ArrayList<String> al = new ArrayList<>(maxQueryTerms);
    PriorityQueue<ScoreTerm> pq = retrieveTerms(docNum);
    ScoreTerm scoreTerm;
    int lim = maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
    // we just want to return the top words
    while (((scoreTerm = pq.pop()) != null) && lim-- > 0) {
      al.add(scoreTerm.word); // the 1st entry is the interesting word
    }
    String[] res = new String[al.size()];
    return al.toArray(res);
  }

  /**
   * Convenience routine to make it easy to return the most interesting words in a document.
   * More advanced users will call {@link #retrieveTerms(Reader, String) retrieveTerms()} directly.
   *
   * @param r the source document
   * @param fieldName field passed to analyzer to use when analyzing the content
   * @return the most interesting words in the document
   * @see #retrieveTerms(java.io.Reader, String)
   */
  public String[] retrieveInterestingTerms(Reader r, String fieldName) throws IOException {
    int maxQueryTerms = parameters.getMaxQueryTerms();
    ArrayList<String> al = new ArrayList<>(maxQueryTerms);
    PriorityQueue<ScoreTerm> pq = retrieveTerms(r, fieldName);
    ScoreTerm scoreTerm;
    int lim = maxQueryTerms; // have to be careful, retrieveTerms returns all words but that's probably not useful to our caller...
    // we just want to return the top words
    while (((scoreTerm = pq.pop()) != null) && lim-- > 0) {
      al.add(scoreTerm.word); // the 1st entry is the interesting word
    }
    String[] res = new String[al.size()];
    return al.toArray(res);
  }

  /**
   * PriorityQueue that orders words by score.
   */
  private static class FreqQ extends PriorityQueue<ScoreTerm> {
    FreqQ(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(ScoreTerm a, ScoreTerm b) {
      return a.score < b.score;
    }
  }

  private static class ScoreTerm {
    // only really need 1st 3 entries, other ones are for troubleshooting
    String word;
    String topField;
    float score;
    float idf;
    int docFreq;
    int tf;

    ScoreTerm(String word, String topField, float score, float idf, int docFreq, int tf) {
      this.word = word;
      this.topField = topField;
      this.score = score;
      this.idf = idf;
      this.docFreq = docFreq;
      this.tf = tf;
    }

    void update(String word, String topField, float score, float idf, int docFreq, int tf) {
      this.word = word;
      this.topField = topField;
      this.score = score;
      this.idf = idf;
      this.docFreq = docFreq;
      this.tf = tf;
    }
  }

  /**
   * Use for frequencies and to avoid renewing Integers.
   */
  private static class Int {
    int x;

    Int() {
      x = 1;
    }
  }
}
