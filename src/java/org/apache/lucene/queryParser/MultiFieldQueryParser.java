package org.apache.lucene.queryParser;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.Vector;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * A QueryParser which constructs queries to search multiple fields.
 *
 * @author <a href="mailto:kelvin@relevanz.com">Kelvin Tan</a>
 * @version $Revision$
 */
public class MultiFieldQueryParser extends QueryParser
{
  
  private String[] fields;

  /**
   * <p>Creates a MultiFieldQueryParser that will, when parse(String query)
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
  public MultiFieldQueryParser(String[] fields, Analyzer analyzer) {
    super(null, analyzer);
    this.fields = fields;
  }
  
  protected Query getFieldQuery(String field, Analyzer analyzer, String queryText)
      throws ParseException {
    if (field == null) {
      Vector clauses = new Vector();
      for (int i = 0; i < fields.length; i++)
        clauses.add(new BooleanClause(super.getFieldQuery(fields[i], queryText),
            BooleanClause.Occur.SHOULD));
      return getBooleanQuery(clauses);
    }
    return super.getFieldQuery(field, queryText);
  }
  
  /**
   * @deprecated use {@link #getFuzzyQuery(String, String, float)}
   */
  protected Query getFuzzyQuery(String field, String termStr) throws ParseException {
    return getFuzzyQuery(field, termStr, fuzzyMinSim);
  }

  protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException
  {
    if (field == null) {
      Vector clauses = new Vector();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(super.getFuzzyQuery(fields[i], termStr, minSimilarity),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses);
    }
    return super.getFuzzyQuery(field, termStr, minSimilarity);
  }

  protected Query getPrefixQuery(String field, String termStr) throws ParseException
  {
    if (field == null) {
      Vector clauses = new Vector();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(super.getPrefixQuery(fields[i], termStr),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses);
    }
    return super.getPrefixQuery(field, termStr);
  }

  /** @throws ParseException
   * @deprecated use {@link #getRangeQuery(String, String, String, boolean)}
  */
  protected Query getRangeQuery(String field, Analyzer analyzer, 
      String part1, String part2, boolean inclusive) throws ParseException {
   return getRangeQuery(field, part1, part2, inclusive);
  }
 
  protected Query getRangeQuery(String field, String part1, String part2, boolean inclusive) throws ParseException {
    if (field == null) {
      Vector clauses = new Vector();
      for (int i = 0; i < fields.length; i++) {
        clauses.add(new BooleanClause(super.getRangeQuery(fields[i], part1, part2, inclusive),
            BooleanClause.Occur.SHOULD));
      }
      return getBooleanQuery(clauses);
    }
    return super.getRangeQuery(field, part1, part2, inclusive);
  }
 

    public static final int NORMAL_FIELD     = 0;
    public static final int REQUIRED_FIELD   = 1;
    public static final int PROHIBITED_FIELD = 2;

    /**
     * @deprecated use {@link #MultiFieldQueryParser(String[], Analyzer)} instead
     */
    public MultiFieldQueryParser(QueryParserTokenManager tm)
    {
        super(tm);
    }

    /**
     * @deprecated use {@link #MultiFieldQueryParser(String[], Analyzer)} instead
     */
    public MultiFieldQueryParser(CharStream stream)
    {
        super(stream);
    }

    /**
     * @deprecated use {@link #MultiFieldQueryParser(String[], Analyzer)} instead
     */
    public MultiFieldQueryParser(String f, Analyzer a)
    {
        super(f, a);
    }

    /**
     * <p>Parses a query which searches on the fields specified.
     * If x fields are specified, this effectively constructs:</p>
     *
     * <code>
     * (field1:query) (field2:query) (field3:query)...(fieldx:query)
     * </code>
     *
     * @param query Query string to parse
     * @param fields Fields to search on
     * @param analyzer Analyzer to use
     * @throws ParseException if query parsing fails
     * @throws TokenMgrError if query parsing fails
     * @deprecated use {@link #parse(String)} instead but note that it
     *  returns a different query for queries where all terms are required:
     *  its query excepts all terms, no matter in what field they occur whereas
     *  the query built by this (deprecated) method expected all terms in all fields 
     *  at the same time.
     */
    public static Query parse(String query, String[] fields, Analyzer analyzer)
	throws ParseException
    {
        BooleanQuery bQuery = new BooleanQuery();
        for (int i = 0; i < fields.length; i++)
        {
            Query q = parse(query, fields[i], analyzer);
            bQuery.add(q, BooleanClause.Occur.SHOULD);
        }
        return bQuery;
    }

    /**
     * <p>
     * Parses a query which searches on the fields specified.
     * <p>
     * If x fields are specified, this effectively constructs:
     * <pre>
     * <code>
     * (field1:query1) (field2:query2) (field3:query3)...(fieldx:queryx)
     * </code>
     * </pre>
     * @param queries Queries strings to parse
     * @param fields Fields to search on
     * @param analyzer Analyzer to use
     * @throws ParseException if query parsing fails
     * @throws TokenMgrError if query parsing fails
     * @throws IllegalArgumentException if the length of the queries array differs
     *  from the length of the fields array
     */
    public static Query parse(String[] queries, String[] fields,
        Analyzer analyzer) throws ParseException
    {
        if (queries.length != fields.length)
            throw new IllegalArgumentException("queries.length != fields.length");
        BooleanQuery bQuery = new BooleanQuery();
        for (int i = 0; i < fields.length; i++)
        {
            Query q = parse(queries[i], fields[i], analyzer);
            bQuery.add(q, BooleanClause.Occur.SHOULD);
        }
        return bQuery;
    }

    /**
     * <p>
     * Parses a query, searching on the fields specified.
     * Use this if you need to specify certain fields as required,
     * and others as prohibited.
     * <p><pre>
     * Usage:
     * <code>
     * String[] fields = {"filename", "contents", "description"};
     * int[] flags = {MultiFieldQueryParser.NORMAL_FIELD,
     *                MultiFieldQueryParser.REQUIRED_FIELD,
     *                MultiFieldQueryParser.PROHIBITED_FIELD,};
     * parse(query, fields, flags, analyzer);
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
     * @param query Query string to parse
     * @param fields Fields to search on
     * @param flags Flags describing the fields
     * @param analyzer Analyzer to use
     * @throws ParseException if query parsing fails
     * @throws TokenMgrError if query parsing fails
     * @throws IllegalArgumentException if the length of the fields array differs
     *  from the length of the flags array
     */
    public static Query parse(String query, String[] fields, int[] flags,
	Analyzer analyzer) throws ParseException
    {
        if (fields.length != flags.length)
          throw new IllegalArgumentException("fields.length != flags.length");
        BooleanQuery bQuery = new BooleanQuery();
        for (int i = 0; i < fields.length; i++)
        {
            Query q = parse(query, fields[i], analyzer);
            int flag = flags[i];
            switch (flag)
            {
                case REQUIRED_FIELD:
                    bQuery.add(q, BooleanClause.Occur.MUST);
                    break;
                case PROHIBITED_FIELD:
                    bQuery.add(q, BooleanClause.Occur.MUST_NOT);
                    break;
                default:
                    bQuery.add(q, BooleanClause.Occur.SHOULD);
                    break;
            }
        }
        return bQuery;
    }

    /**
     * <p>
     * Parses a query, searching on the fields specified.
     * Use this if you need to specify certain fields as required,
     * and others as prohibited.
     * <p><pre>
     * Usage:
     * <code>
     * String[] fields = {"filename", "contents", "description"};
     * int[] flags = {MultiFieldQueryParser.NORMAL_FIELD,
     *                MultiFieldQueryParser.REQUIRED_FIELD,
     *                MultiFieldQueryParser.PROHIBITED_FIELD,};
     * parse(query, fields, flags, analyzer);
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
     * @param queries Queries string to parse
     * @param fields Fields to search on
     * @param flags Flags describing the fields
     * @param analyzer Analyzer to use
     * @throws ParseException if query parsing fails
     * @throws TokenMgrError if query parsing fails
     * @throws IllegalArgumentException if the length of the queries, fields,
     *  and flags array differ
     */
    public static Query parse(String[] queries, String[] fields, int[] flags,
        Analyzer analyzer) throws ParseException
    {
        if (!(queries.length == fields.length && queries.length == flags.length))
            throw new IllegalArgumentException("queries, fields, and flags array have have different length");
        BooleanQuery bQuery = new BooleanQuery();
        for (int i = 0; i < fields.length; i++)
        {
            Query q = parse(queries[i], fields[i], analyzer);
            int flag = flags[i];
            switch (flag)
            {
                case REQUIRED_FIELD:
                    bQuery.add(q, BooleanClause.Occur.MUST);
                    break;
                case PROHIBITED_FIELD:
                    bQuery.add(q, BooleanClause.Occur.MUST_NOT);
                    break;
                default:
                    bQuery.add(q, BooleanClause.Occur.SHOULD);
                    break;
            }
        }
        return bQuery;
    }
}
