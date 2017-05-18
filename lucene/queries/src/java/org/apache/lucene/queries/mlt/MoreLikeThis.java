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
import java.util.Collection;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.queries.mlt.query.MoreLikeThisQueryBuilder;
import org.apache.lucene.queries.mlt.terms.LuceneDocumentTermsRetriever;
import org.apache.lucene.queries.mlt.terms.LocalDocumentTermsRetriever;
import org.apache.lucene.queries.mlt.terms.scorer.ScoredTerm;
import org.apache.lucene.search.Query;
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
 * <p>
 * MoreLikeThis mlt = new MoreLikeThis(ir);
 * Reader target = ... // orig source of doc you want to find similarities to
 * Query query = mlt.like( target);
 * 
 * Hits hits = is.search(query);
 * // now the usual iteration thru 'hits' - the only thing to watch for is to make sure
 * //you ignore the doc if it matches your 'target' document, as it should be similar to itself
 * <p>
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
   * Parameter and default
   */
  private MoreLikeThisParameters params;

  private LocalDocumentTermsRetriever localDocumentTermsRetriever;

  private LuceneDocumentTermsRetriever luceneDocumentTermsRetriever;

  private MoreLikeThisQueryBuilder queryBuilder;

  /**
   * IndexReader to use
   */
  private final IndexReader ir;

  /**
   * Constructor requiring an IndexReader.
   */
  public MoreLikeThis(IndexReader ir) {
    this(ir, new MoreLikeThisParameters());
  }

  /**
   * Constructor requiring an IndexReader.
   */
  public MoreLikeThis(IndexReader ir, MoreLikeThisParameters params) {
    this.params = params;
    this.ir = ir;

    this.localDocumentTermsRetriever = new LocalDocumentTermsRetriever(ir, params);
    this.luceneDocumentTermsRetriever = new LuceneDocumentTermsRetriever(ir, params);

    this.queryBuilder = new MoreLikeThisQueryBuilder(params);
  }

  public MoreLikeThisParameters getParameters() {
    return this.params;
  }

  public void setParameters(MoreLikeThisParameters params) {
    this.params = params;

    this.localDocumentTermsRetriever.setParameters(params);
    this.luceneDocumentTermsRetriever.setParameters(params);

    this.queryBuilder.setParameters(params);
  }

  /**
   * Return a query that will return docs like the passed lucene document ID.
   *
   * @param docNum the documentID of the lucene doc to generate the 'More Like This" query for.
   * @return a query that will return docs like the passed lucene document ID.
   */
  public Query like(int docNum) throws IOException {
    String[] fieldNames = params.getFieldNames();
    initMoreLikeThisQueryFields(fieldNames);
    PriorityQueue<ScoredTerm> scoredTerms = localDocumentTermsRetriever.retrieveTermsFromLocalDocument(docNum);

    return queryBuilder.createQuery(scoredTerms);
  }

  public Query like(Document luceneDocument) throws IOException {
    initMoreLikeThisQueryFields(params.getFieldNames());
    PriorityQueue<ScoredTerm> scoredTerms = luceneDocumentTermsRetriever.retrieveTermsFromDocument(luceneDocument);
    return queryBuilder.createQuery(scoredTerms);
  }

  public Query like(String fieldName, String... seedText) throws IOException {
    initMoreLikeThisQueryFields(params.getFieldNames());

    Document luceneDocument = new Document();
    for (String seedTextValue : seedText) {
      luceneDocument.add(new TextField(fieldName, seedTextValue, Field.Store.YES));
    }

    return this.like(luceneDocument);
  }

  private void initMoreLikeThisQueryFields(String[] fieldNames) {
    if (fieldNames == null) {
      // gather list of valid fields from lucene
      Collection<String> fields = MultiFields.getIndexedFields(ir);
      params.setFieldNames(fields.toArray(new String[fields.size()]));
    }
  }

  /**
   * Describe the parameters that control how the "more like this" query is formed.
   */
  public String describeParameters() {
    StringBuilder sb = new StringBuilder();
    sb.append("\t").append("maxQueryTerms  : ").append(params.getMaxQueryTerms()).append("\n");
    sb.append("\t").append("minWordLen     : ").append(params.getMinWordLen()).append("\n");
    sb.append("\t").append("maxWordLen     : ").append(params.getMaxWordLen()).append("\n");
    sb.append("\t").append("fieldNames     : ");
    String delim = "";
    for (String fieldName : params.getFieldNames()) {
      sb.append(delim).append(fieldName);
      delim = ", ";
    }
    sb.append("\n");
    sb.append("\t").append("boost          : ").append(params.isBoostEnabled()).append("\n");
    sb.append("\t").append("minTermFreq    : ").append(params.getMinTermFreq()).append("\n");
    sb.append("\t").append("minDocFreq     : ").append(params.getMinDocFreq()).append("\n");
    return sb.toString();
  }


}
