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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.CharStream;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParserTokenManager;
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
    public static final int NORMAL_FIELD     = 0;
    public static final int REQUIRED_FIELD   = 1;
    public static final int PROHIBITED_FIELD = 2;

    public MultiFieldQueryParser(QueryParserTokenManager tm)
    {
        super(tm);
    }

    public MultiFieldQueryParser(CharStream stream)
    {
        super(stream);
    }

    public MultiFieldQueryParser(String f, Analyzer a)
    {
        super(f, a);
    }

    /**
     * <p>
     * Parses a query which searches on the fields specified.
     * <p>
     * If x fields are specified, this effectively constructs:
     * <pre>
     * <code>
     * (field1:query) (field2:query) (field3:query)...(fieldx:query)
     * </code>
     * </pre>
     *
     * @param query Query string to parse
     * @param fields Fields to search on
     * @param analyzer Analyzer to use
     * @throws ParseException if query parsing fails
     * @throws TokenMgrError if query parsing fails
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
     */
    public static Query parse(String[] queries, String[] fields,
        Analyzer analyzer) throws ParseException
    {
        if (queries.length != fields.length)
            // TODO Exception handling
            throw new ParseException("queries.length != fields.length");
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
     * int[] flags = {MultiFieldQueryParser.NORMAL FIELD,
     *                MultiFieldQueryParser.REQUIRED FIELD,
     *                MultiFieldQueryParser.PROHIBITED FIELD,};
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
     */
    public static Query parse(String query, String[] fields, int[] flags,
	Analyzer analyzer)
	throws ParseException
    {
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
     * int[] flags = {MultiFieldQueryParser.NORMAL FIELD,
     *                MultiFieldQueryParser.REQUIRED FIELD,
     *                MultiFieldQueryParser.PROHIBITED FIELD,};
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
     */
    public static Query parse(String[] queries, String[] fields, int[] flags,
        Analyzer analyzer) throws ParseException
    {
        if (queries.length != fields.length)
            // TODO Exception handling
            throw new ParseException("queries.length != fields.length");
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
