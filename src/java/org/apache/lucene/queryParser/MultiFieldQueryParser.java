package org.apache.lucene.queryParser;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation"
 *    must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.CharStream;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.QueryParserTokenManager;
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
     * @throws ParserException if query parsing fails
     * @throws TokenMgrError if query parsing fails
     */
    public static Query parse(String query, String[] fields, Analyzer analyzer)
	throws ParseException
    {
        BooleanQuery bQuery = new BooleanQuery();
        for (int i = 0; i < fields.length; i++)
        {
            Query q = parse(query, fields[i], analyzer);
            bQuery.add(q, false, false);
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
     * @throws ParserException if query parsing fails
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
                    bQuery.add(q, true, false);
                    break;
                case PROHIBITED_FIELD:
                    bQuery.add(q, false, true);
                    break;
                default:
                    bQuery.add(q, false, false);
                    break;
            }
        }
        return bQuery;
    }
}
