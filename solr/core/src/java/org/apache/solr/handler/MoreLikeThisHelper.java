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

package org.apache.solr.handler;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

/**
 * Helper class for MoreLikeThis that can be called from other request handlers
 */
public class MoreLikeThisHelper
{
  final SolrIndexSearcher searcher;
  final MoreLikeThis mlt;
  final IndexReader reader;
  final SchemaField uniqueKeyField;
  final boolean needDocSet;
  Map<String,Float> boostFields;
  private Query rawMLTQuery;
  private Query boostedMLTQuery;
  private BooleanQuery realMLTQuery;

  public MoreLikeThisHelper(SolrParams params, SolrIndexSearcher searcher )
  {
    this.searcher = searcher;
    this.reader = searcher.getIndexReader();
    this.uniqueKeyField = searcher.getSchema().getUniqueKeyField();
    this.needDocSet = params.getBool(FacetParams.FACET,false);

    // Pattern is thread safe -- TODO? share this with general 'fl' param
    final Pattern splitList = Pattern.compile(",| ");
    SolrParams required = params.required();
    String[] fl = required.getParams(MoreLikeThisParams.SIMILARITY_FIELDS);
    List<String> list = new ArrayList<>();
    for (String f : fl) {
      if (!StringUtils.isEmpty(f))  {
        String[] strings = splitList.split(f);
        for (String string : strings) {
          if (!StringUtils.isEmpty(string)) {
            list.add(string);
          }
        }
      }
    }
    String[] fields = list.toArray(new String[list.size()]);
    if( fields.length < 1 ) {
      throw new SolrException( SolrException.ErrorCode.BAD_REQUEST,
          "MoreLikeThis requires at least one similarity field: "+MoreLikeThisParams.SIMILARITY_FIELDS );
    }

    this.mlt = new MoreLikeThis( reader ); // TODO -- after LUCENE-896, we can use , searcher.getSimilarity() );
    mlt.setFieldNames(fields);
    mlt.setAnalyzer( searcher.getSchema().getIndexAnalyzer() );

    // configurable params
    mlt.setMinTermFreq(       params.getInt(MoreLikeThisParams.MIN_TERM_FREQ,         MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
    mlt.setMinDocFreq(        params.getInt(MoreLikeThisParams.MIN_DOC_FREQ,          MoreLikeThis.DEFAULT_MIN_DOC_FREQ));
    mlt.setMaxDocFreq(        params.getInt(MoreLikeThisParams.MAX_DOC_FREQ,          MoreLikeThis.DEFAULT_MAX_DOC_FREQ));
    mlt.setMinWordLen(        params.getInt(MoreLikeThisParams.MIN_WORD_LEN,          MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
    mlt.setMaxWordLen(        params.getInt(MoreLikeThisParams.MAX_WORD_LEN,          MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
    mlt.setMaxQueryTerms(     params.getInt(MoreLikeThisParams.MAX_QUERY_TERMS,       MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
    mlt.setMaxNumTokensParsed(params.getInt(MoreLikeThisParams.MAX_NUM_TOKENS_PARSED, MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
    mlt.setBoost(            params.getBool(MoreLikeThisParams.BOOST, false ) );

    // There is no default for maxDocFreqPct. Also, it's a bit oddly expressed as an integer value
    // (percentage of the collection's documents count). We keep Lucene's convention here.
    if (params.getInt(MoreLikeThisParams.MAX_DOC_FREQ_PCT) != null) {
      mlt.setMaxDocFreqPct(params.getInt(MoreLikeThisParams.MAX_DOC_FREQ_PCT));
    }

    boostFields = SolrPluginUtils.parseFieldBoosts(params.getParams(MoreLikeThisParams.QF));
  }

  public Query getRawMLTQuery(){
    return rawMLTQuery;
  }

  public Query getBoostedMLTQuery(){
    return boostedMLTQuery;
  }

  public Query getRealMLTQuery(){
    return realMLTQuery;
  }

  private Query getBoostedQuery(Query mltquery) {
    BooleanQuery boostedQuery = (BooleanQuery)mltquery;
    if (boostFields.size() > 0) {
      BooleanQuery.Builder newQ = new BooleanQuery.Builder();
      newQ.setMinimumNumberShouldMatch(boostedQuery.getMinimumNumberShouldMatch());
      for (BooleanClause clause : boostedQuery) {
        Query q = clause.getQuery();
        float originalBoost = 1f;
        if (q instanceof BoostQuery) {
          BoostQuery bq = (BoostQuery) q;
          q = bq.getQuery();
          originalBoost = bq.getBoost();
        }
        Float fieldBoost = boostFields.get(((TermQuery) q).getTerm().field());
        q = ((fieldBoost != null) ? new BoostQuery(q, fieldBoost * originalBoost) : clause.getQuery());
        newQ.add(q, clause.getOccur());
      }
      boostedQuery = newQ.build();
    }
    return boostedQuery;
  }

  private DocListAndSet getQueryResults(Query query, List<Query> filters, int start, int rows, int flags) throws IOException
  {
    DocListAndSet results = new DocListAndSet();
    if (this.needDocSet) {
      results = searcher.getDocListAndSet(query, filters, null, start, rows, flags);
    } else {
      results.docList = searcher.getDocList(query, filters, null, start, rows, flags);
    }
    return results;
  }

  public DocListAndSet getMoreLikeThis( int id, int start, int rows, List<Query> filters, List<InterestingTerm> terms, int flags ) throws IOException
  {
    Document doc = reader.document(id);
    rawMLTQuery = mlt.like(id);
    boostedMLTQuery = getBoostedQuery( rawMLTQuery );
    if( terms != null ) {
      fillInterestingTermsFromMLTQuery( boostedMLTQuery, terms );
    }

    // exclude current document from results
    BooleanQuery.Builder realMLTQuery = new BooleanQuery.Builder();
    realMLTQuery.add(boostedMLTQuery, BooleanClause.Occur.MUST);
    realMLTQuery.add(
        new TermQuery(new Term(uniqueKeyField.getName(), uniqueKeyField.getType().storedToIndexed(doc.getField(uniqueKeyField.getName())))),
        BooleanClause.Occur.MUST_NOT);
    this.realMLTQuery = realMLTQuery.build();
    return getQueryResults(this.realMLTQuery, filters, start, rows, flags);
  }

  public DocListAndSet getMoreLikeThis(Reader reader, int start, int rows, List<Query> filters, List<InterestingTerm> terms, int flags ) throws IOException
  {
    // SOLR-5351: if only check against a single field, use the reader directly. Otherwise we
    // repeat the stream's content for multiple fields so that query terms can be pulled from any
    // of those fields.
    String [] fields = mlt.getFieldNames();
    if (fields.length == 1) {
      rawMLTQuery = mlt.like(fields[0], reader);
    } else {
      CharsRefBuilder buffered = new CharsRefBuilder();
      char [] chunk = new char [1024];
      int len;
      while ((len = reader.read(chunk)) >= 0) {
        buffered.append(chunk, 0, len);
      }

      Collection<Object> streamValue = Collections.singleton(buffered.get().toString());
      Map<String, Collection<Object>> multifieldDoc = new HashMap<>(fields.length);
      for (String field : fields) {
        multifieldDoc.put(field, streamValue);
      }

      rawMLTQuery = mlt.like(multifieldDoc);
    }

    boostedMLTQuery = getBoostedQuery( rawMLTQuery );
    if (terms != null) {
      fillInterestingTermsFromMLTQuery( boostedMLTQuery, terms );
    }
    return getQueryResults(boostedMLTQuery, filters, start,rows, flags);
  }

  public NamedList<BooleanQuery> getMoreLikeTheseQuery(DocList docs) throws IOException
  {
    IndexSchema schema = searcher.getSchema();
    NamedList<BooleanQuery> result = new NamedList<>();
    DocIterator iterator = docs.iterator();
    while (iterator.hasNext()) {
      int id = iterator.nextDoc();
      String uniqueId = schema.printableUniqueKey(reader.document(id));

      BooleanQuery mltquery = (BooleanQuery) mlt.like(id);
      if (mltquery.clauses().size() == 0) {
        return result;
      }
      mltquery = (BooleanQuery) getBoostedQuery(mltquery);

      // exclude current document from results
      BooleanQuery.Builder mltQuery = new BooleanQuery.Builder();
      mltQuery.add(mltquery, BooleanClause.Occur.MUST);

      mltQuery.add(
          new TermQuery(new Term(uniqueKeyField.getName(), uniqueId)), BooleanClause.Occur.MUST_NOT);
      result.add(uniqueId, mltQuery.build());
    }

    return result;
  }

  private void fillInterestingTermsFromMLTQuery( Query query, List<InterestingTerm> terms )
  {
    Collection<BooleanClause> clauses = ((BooleanQuery)query).clauses();
    for( BooleanClause o : clauses ) {
      Query q = o.getQuery();
      float boost = 1f;
      if (q instanceof BoostQuery) {
        BoostQuery bq = (BoostQuery) q;
        q = bq.getQuery();
        boost = bq.getBoost();
      }
      InterestingTerm it = new InterestingTerm();
      it.boost = boost;
      it.term = ((TermQuery) q).getTerm();
      terms.add( it );
    }
    // alternatively we could use
    // mltquery.extractTerms( terms );
  }

  public MoreLikeThis getMoreLikeThis()
  {
    return mlt;
  }

}
