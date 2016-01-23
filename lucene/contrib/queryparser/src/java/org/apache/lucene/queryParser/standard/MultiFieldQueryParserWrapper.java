package org.apache.lucene.queryParser.standard;

/**
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

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * This class behaves as the as the lucene 2.4 MultiFieldQueryParser class, but uses the new
 * query parser interface instead of the old one. <br/>
 * <br/>
 * This class should be used when the new query parser features are needed and
 * also keep at the same time the old query parser interface. <br/>
 * 
 * @deprecated this class will be removed soon, it's a temporary class to be
 *             used along the transition from the old query parser to the new
 *             one
 */
@Deprecated
public class MultiFieldQueryParserWrapper extends QueryParserWrapper {

  /**
   * Creates a MultiFieldQueryParser. Allows passing of a map with term to
   * Boost, and the boost to apply to each term.
   * 
   * <p>
   * It will, when parse(String query) is called, construct a query like this
   * (assuming the query consists of two terms and you specify the two fields
   * <code>title</code> and <code>body</code>):
   * </p>
   * 
   * <code>
     * (title:term1 body:term1) (title:term2 body:term2)
     * </code>
   * 
   * <p>
   * When setDefaultOperator(AND_OPERATOR) is set, the result will be:
   * </p>
   * 
   * <code>
     * +(title:term1 body:term1) +(title:term2 body:term2)
     * </code>
   * 
   * <p>
   * When you pass a boost (title=>5 body=>10) you can get
   * </p>
   * 
   * <code>
     * +(title:term1^5.0 body:term1^10.0) +(title:term2^5.0 body:term2^10.0)
     * </code>
   * 
   * <p>
   * In other words, all the query's terms must appear, but it doesn't matter in
   * what fields they appear.
   * </p>
   */
  @SuppressWarnings("unchecked")
public MultiFieldQueryParserWrapper(String[] fields, Analyzer analyzer, Map boosts) {
    this(fields, analyzer);
    StandardQueryParser qpHelper = getQueryParserHelper();

    qpHelper.setMultiFields(fields);
    qpHelper.setFieldsBoost(boosts);

  }

  /**
   * Creates a MultiFieldQueryParser.
   * 
   * <p>
   * It will, when parse(String query) is called, construct a query like this
   * (assuming the query consists of two terms and you specify the two fields
   * <code>title</code> and <code>body</code>):
   * </p>
   * 
   * <code>
     * (title:term1 body:term1) (title:term2 body:term2)
     * </code>
   * 
   * <p>
   * When setDefaultOperator(AND_OPERATOR) is set, the result will be:
   * </p>
   * 
   * <code>
     * +(title:term1 body:term1) +(title:term2 body:term2)
     * </code>
   * 
   * <p>
   * In other words, all the query's terms must appear, but it doesn't matter in
   * what fields they appear.
   * </p>
   */
  public MultiFieldQueryParserWrapper(String[] fields, Analyzer analyzer) {
    super(null, analyzer);

    StandardQueryParser qpHelper = getQueryParserHelper();
    qpHelper.setAnalyzer(analyzer);

    qpHelper.setMultiFields(fields);
  }

  /**
   * Parses a query which searches on the fields specified.
   * <p>
   * If x fields are specified, this effectively constructs:
   * 
   * <pre>
   * &lt;code&gt;
   * (field1:query1) (field2:query2) (field3:query3)...(fieldx:queryx)
   * &lt;/code&gt;
   * </pre>
   * 
   * @param queries
   *          Queries strings to parse
   * @param fields
   *          Fields to search on
   * @param analyzer
   *          Analyzer to use
   * @throws ParseException
   *           if query parsing fails
   * @throws IllegalArgumentException
   *           if the length of the queries array differs from the length of the
   *           fields array
   */
  public static Query parse(String[] queries, String[] fields, Analyzer analyzer)
      throws ParseException {
    if (queries.length != fields.length)
      throw new IllegalArgumentException("queries.length != fields.length");
    BooleanQuery bQuery = new BooleanQuery();
    for (int i = 0; i < fields.length; i++) {
      QueryParserWrapper qp = new QueryParserWrapper(fields[i], analyzer);
      Query q = qp.parse(queries[i]);
      if (q != null && // q never null, just being defensive
          (!(q instanceof BooleanQuery) || ((BooleanQuery) q).getClauses().length > 0)) {
        bQuery.add(q, BooleanClause.Occur.SHOULD);
      }
    }
    return bQuery;
  }

  /**
   * Parses a query, searching on the fields specified. Use this if you need to
   * specify certain fields as required, and others as prohibited.
   * <p>
   * 
   * <pre>
   * Usage:
   * &lt;code&gt;
   * String[] fields = {&quot;filename&quot;, &quot;contents&quot;, &quot;description&quot;};
   * BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD,
   *                BooleanClause.Occur.MUST,
   *                BooleanClause.Occur.MUST_NOT};
   * MultiFieldQueryParser.parse(&quot;query&quot;, fields, flags, analyzer);
   * &lt;/code&gt;
   * </pre>
   *<p>
   * The code above would construct a query:
   * 
   * <pre>
   * &lt;code&gt;
   * (filename:query) +(contents:query) -(description:query)
   * &lt;/code&gt;
   * </pre>
   * 
   * @param query
   *          Query string to parse
   * @param fields
   *          Fields to search on
   * @param flags
   *          Flags describing the fields
   * @param analyzer
   *          Analyzer to use
   * @throws ParseException
   *           if query parsing fails
   * @throws IllegalArgumentException
   *           if the length of the fields array differs from the length of the
   *           flags array
   */
  public static Query parse(String query, String[] fields,
      BooleanClause.Occur[] flags, Analyzer analyzer) throws ParseException {
    if (fields.length != flags.length)
      throw new IllegalArgumentException("fields.length != flags.length");
    BooleanQuery bQuery = new BooleanQuery();
    for (int i = 0; i < fields.length; i++) {
      QueryParserWrapper qp = new QueryParserWrapper(fields[i], analyzer);
      Query q = qp.parse(query);
      if (q != null && // q never null, just being defensive
          (!(q instanceof BooleanQuery) || ((BooleanQuery) q).getClauses().length > 0)) {
        bQuery.add(q, flags[i]);
      }
    }
    return bQuery;
  }

  /**
   * Parses a query, searching on the fields specified. Use this if you need to
   * specify certain fields as required, and others as prohibited.
   * <p>
   * 
   * <pre>
   * Usage:
   * &lt;code&gt;
   * String[] query = {&quot;query1&quot;, &quot;query2&quot;, &quot;query3&quot;};
   * String[] fields = {&quot;filename&quot;, &quot;contents&quot;, &quot;description&quot;};
   * BooleanClause.Occur[] flags = {BooleanClause.Occur.SHOULD,
   *                BooleanClause.Occur.MUST,
   *                BooleanClause.Occur.MUST_NOT};
   * MultiFieldQueryParser.parse(query, fields, flags, analyzer);
   * &lt;/code&gt;
   * </pre>
   *<p>
   * The code above would construct a query:
   * 
   * <pre>
   * &lt;code&gt;
   * (filename:query1) +(contents:query2) -(description:query3)
   * &lt;/code&gt;
   * </pre>
   * 
   * @param queries
   *          Queries string to parse
   * @param fields
   *          Fields to search on
   * @param flags
   *          Flags describing the fields
   * @param analyzer
   *          Analyzer to use
   * @throws ParseException
   *           if query parsing fails
   * @throws IllegalArgumentException
   *           if the length of the queries, fields, and flags array differ
   */
  public static Query parse(String[] queries, String[] fields,
      BooleanClause.Occur[] flags, Analyzer analyzer) throws ParseException {
    if (!(queries.length == fields.length && queries.length == flags.length))
      throw new IllegalArgumentException(
          "queries, fields, and flags array have have different length");
    BooleanQuery bQuery = new BooleanQuery();
    for (int i = 0; i < fields.length; i++) {
      QueryParserWrapper qp = new QueryParserWrapper(fields[i], analyzer);
      Query q = qp.parse(queries[i]);
      if (q != null && // q never null, just being defensive
          (!(q instanceof BooleanQuery) || ((BooleanQuery) q).getClauses().length > 0)) {
        bQuery.add(q, flags[i]);
      }
    }
    return bQuery;
  }

}
