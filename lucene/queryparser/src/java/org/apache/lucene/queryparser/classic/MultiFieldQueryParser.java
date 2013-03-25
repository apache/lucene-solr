package org.apache.lucene.queryparser.classic;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

/**
 * A QueryParser which constructs queries to search multiple fields.
 *
 */
public class MultiFieldQueryParser extends QueryParser
{
  protected String[] fields;
  protected Map<String,Float> boosts;

  /**
   * Creates a MultiFieldQueryParser. 
   * Allows passing of a map with term to Boost, and the boost to apply to each term.
   *
   * <p>It will, when parse(String query)
   * is called, construct a query like this (assuming the query consists of
   * two terms and you specify the two fields <code>title</code> and <code>body</code>):</p>
   * 
   * <code>
   * (title:term1 body:term1) (title:term2 body:term2)
   * </code>
   *
   * <p>When setDefaultOperator(AND_OPERATOR) is set, the result will be:</p>
   *  
   * <code>
   * +(title:term1 body:term1) +(title:term2 body:term2)
   * </code>
   * 
   * <p>When you pass a boost (title=>5 body=>10) you can get </p>
   * 
   * <code>
   * +(title:term1^5.0 body:term1^10.0) +(title:term2^5.0 body:term2^10.0)
   * </code>
   *
   * <p>In other words, all the query's terms must appear, but it doesn't matter in
   * what fields they appear.</p>
   */
  public MultiFieldQueryParser(Version matchVersion, String[] fields, Analyzer analyzer, Map<String,Float> boosts) {
    this(matchVersion, fields, analyzer);
    this.boosts = boosts;
  }
  
  /**
   * Creates a MultiFieldQueryParser.
   *
   * <p>It will, when parse(String query)
   * is called, construct a query like this (assuming the query consists of
   * two terms and you specify the two fields <code>title</code> and <code>body</code>):</p>
   * 
   * <code>
   * (title:term1 body:term1) (title:term2 body:term2)
   * </code>
   *
   * <p>When setDefaultOperator(AND_OPERATOR) is set, the result will be:</p>
   *  
   * <code>
   * +(title:term1 body:term1) +(title:term2 body:term2)
   * </code>
   * 
   * <p>In other words, all the query's terms must appear, but it doesn't matter in
   * what fields they appear.</p>
   */
  public MultiFieldQueryParser(Version matchVersion, String[] fields, Analyzer analyzer) {
    super(matchVersion, null, analyzer);
    this.fields = fields;
  }
  
  @Override
  protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        Query q = super.getFieldQuery(fields[i], queryText, true);
        if (q != null) {
          //If the user passes a map of boosts
          if (boosts != null) {
            //Get the boost from the map and apply them
            Float boost = boosts.get(fields[i]);
            if (boost != null) {
              q.setBoost(boost.floatValue());
            }
          }
          applySlop(q,slop);
          clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
        }
      }
      if (clauses.size() == 0)  // happens for stopwords
        return null;
      return getBooleanQuery(clauses, true);
    }
    Query q = super.getFieldQuery(field, queryText, true);
    applySlop(q,slop);
    return q;
  }

  private void applySlop(Query q, int slop) {
    if (q instanceof PhraseQuery) {
      ((PhraseQuery) q).setSlop(slop);
    } else if (q instanceof MultiPhraseQuery) {
      ((MultiPhraseQuery) q).setSlop(slop);
    }
  }
  

  @Override
  protected Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        Query q = super.getFieldQuery(fields[i], queryText, quoted);
        if (q != null) {
          //If the user passes a map of boosts
          if (boosts != null) {
            //Get the boost from the map and apply them
            Float boost = boosts.get(fields[i]);
            if (boost != null) {
              q.setBoost(boost.floatValue());
            }
          }
          clauses.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
        }
      }
      if (clauses.size() == 0)  // happens for stopwords
        return null;
      return getBooleanQuery(clauses, true);
    }
    Query q = super.getFieldQuery(field, queryText, quoted);
    return q;
  }


  @Override
  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException
  {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(getFuzzyQuery(fields[i], termStr, minSimilarity),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses, true);
    }
    return super.getFuzzyQuery(field, termStr, minSimilarity);
  }

  @Override
  protected Query getPrefixQuery(String field, String termStr) throws ParseException
  {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(getPrefixQuery(fields[i], termStr),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses, true);
    }
    return super.getPrefixQuery(field, termStr);
  }

  @Override
  protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(getWildcardQuery(fields[i], termStr),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses, true);
    }
    return super.getWildcardQuery(field, termStr);
  }

 
  @Override
  protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(getRangeQuery(fields[i], part1, part2, startInclusive, endInclusive),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses, true);
    }
    return super.getRangeQuery(field, part1, part2, startInclusive, endInclusive);
  }
  
  

  @Override
  protected Query getRegexpQuery(String field, String termStr)
      throws ParseException {
    if (field == null) {
      List<BooleanClause> clauses = new ArrayList<BooleanClause>();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(getRegexpQuery(fields[i], termStr),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses, true);
    }
    return super.getRegexpQuery(field, termStr);
  }

  /**
   * Parses a query which searches on the fields specified.
   * <p>
   * If x fields are specified, this effectively constructs:
   * <pre>
   * <code>
   * (field1:query1) (field2:query2) (field3:query3)...(fieldx:queryx)
   * </code>
   * </pre>
   * @param matchVersion Lucene version to match; this is passed through to QueryParser.
   * @param queries Queries strings to parse
   * @param fields Fields to search on
   * @param analyzer Analyzer to use
   * @throws ParseException if query parsing fails
   * @throws IllegalArgumentException if the length of the queries array differs
   *  from the length of the fields array
   */
  public static Query parse(Version matchVersion, String[] queries, String[] fields,
      Analyzer analyzer) throws ParseException
  {
    if (queries.length != fields.length)
      throw new IllegalArgumentException("queries.length != fields.length");
    BooleanQuery bQuery = new BooleanQuery();
    for (int i = 0; i < fields.length; i++)
    {
      QueryParser qp = new QueryParser(matchVersion, fields[i], analyzer);
      Query q = qp.parse(queries[i]);
      if (q!=null && // q never null, just being defensive
          (!(q instanceof BooleanQuery) || ((BooleanQuery)q).getClauses().length>0)) {
        bQuery.add(q, BooleanClause.Occur.SHOULD);
      }
    }
    return bQuery;
  }

  /**
   * Parses a query, searching on the fields specified.
   * Use this if you need to specify certain fields as required,
   * and others as prohibited.
   * <p>
   * Usage:
   * <pre class="prettyprint">
   * <code>
   * String[] fields = {"filename", "contents", "description"};
   * BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD,
   *                BooleanClause.Occur.MUST,
   *                BooleanClause.Occur.MUST_NOT};
   * MultiFieldQueryParser.parse("query", fields, flags, analyzer);
   * </code>
   * </pre>
   *<p>
   * The code above would construct a query:
   * <pre>
   * <code>
   * (filename:query) +(contents:query) -(description:query)
   * </code>
   * </pre>
   *
   * @param matchVersion Lucene version to match; this is passed through to QueryParser.
   * @param query Query string to parse
   * @param fields Fields to search on
   * @param flags Flags describing the fields
   * @param analyzer Analyzer to use
   * @throws ParseException if query parsing fails
   * @throws IllegalArgumentException if the length of the fields array differs
   *  from the length of the flags array
   */
  public static Query parse(Version matchVersion, String query, String[] fields,
      BooleanClause.Occur[] flags, Analyzer analyzer) throws ParseException {
    if (fields.length != flags.length)
      throw new IllegalArgumentException("fields.length != flags.length");
    BooleanQuery bQuery = new BooleanQuery();
    for (int i = 0; i < fields.length; i++) {
      QueryParser qp = new QueryParser(matchVersion, fields[i], analyzer);
      Query q = qp.parse(query);
      if (q!=null && // q never null, just being defensive 
          (!(q instanceof BooleanQuery) || ((BooleanQuery)q).getClauses().length>0)) {
        bQuery.add(q, flags[i]);
      }
    }
    return bQuery;
  }

  /**
   * Parses a query, searching on the fields specified.
   * Use this if you need to specify certain fields as required,
   * and others as prohibited.
   * <p>
   * Usage:
   * <pre class="prettyprint">
   * <code>
   * String[] query = {"query1", "query2", "query3"};
   * String[] fields = {"filename", "contents", "description"};
   * BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD,
   *                BooleanClause.Occur.MUST,
   *                BooleanClause.Occur.MUST_NOT};
   * MultiFieldQueryParser.parse(query, fields, flags, analyzer);
   * </code>
   * </pre>
   *<p>
   * The code above would construct a query:
   * <pre>
   * <code>
   * (filename:query1) +(contents:query2) -(description:query3)
   * </code>
   * </pre>
   *
   * @param matchVersion Lucene version to match; this is passed through to QueryParser.
   * @param queries Queries string to parse
   * @param fields Fields to search on
   * @param flags Flags describing the fields
   * @param analyzer Analyzer to use
   * @throws ParseException if query parsing fails
   * @throws IllegalArgumentException if the length of the queries, fields,
   *  and flags array differ
   */
  public static Query parse(Version matchVersion, String[] queries, String[] fields, BooleanClause.Occur[] flags,
      Analyzer analyzer) throws ParseException
  {
    if (!(queries.length == fields.length && queries.length == flags.length))
      throw new IllegalArgumentException("queries, fields, and flags array have have different length");
    BooleanQuery bQuery = new BooleanQuery();
    for (int i = 0; i < fields.length; i++)
    {
      QueryParser qp = new QueryParser(matchVersion, fields[i], analyzer);
      Query q = qp.parse(queries[i]);
      if (q!=null && // q never null, just being defensive
          (!(q instanceof BooleanQuery) || ((BooleanQuery)q).getClauses().length>0)) {
        bQuery.add(q, flags[i]);
      }
    }
    return bQuery;
  }

}
