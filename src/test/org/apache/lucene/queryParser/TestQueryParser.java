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
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
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

import java.io.*;
import java.text.*;
import java.util.*;
import junit.framework.*;

import org.apache.lucene.*;
import org.apache.lucene.queryParser.*;
import org.apache.lucene.search.*;
import org.apache.lucene.document.DateField;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.*;
import org.apache.lucene.analysis.Token;

public class TestQueryParser extends TestCase {

    public TestQueryParser(String name) {
	super(name);
    }

    public static Analyzer qpAnalyzer = new QPTestAnalyzer();

    public static class QPTestFilter extends TokenFilter {

	/**
	 * Filter which discards the token 'stop' and which expands the
	 * token 'phrase' into 'phrase1 phrase2'
	 */
	public QPTestFilter(TokenStream in) {
            super(in);
	}

	boolean inPhrase = false;
	int savedStart=0, savedEnd=0;

	public Token next() throws IOException {
	    if (inPhrase) {
		inPhrase = false;
		return new Token("phrase2", savedStart, savedEnd);
	    }
	    else
		for (Token token = input.next(); token != null; token = input.next()) {
		    if (token.termText().equals("phrase")) {
			inPhrase = true;
			savedStart = token.startOffset();
			savedEnd = token.endOffset();
			return new Token("phrase1", savedStart, savedEnd);
		    }
		    else if (!token.termText().equals("stop"))
			return token;
		}
	    return null;
	}
    }

    public static class QPTestAnalyzer extends Analyzer {

	public QPTestAnalyzer() {
	}

	/** Filters LowerCaseTokenizer with StopFilter. */
	public final TokenStream tokenStream(String fieldName, Reader reader) {
	    return new QPTestFilter(new LowerCaseTokenizer(reader));
	}
    }

    public Query getQuery(String query, Analyzer a) throws Exception {
	if (a == null)
	    a = new SimpleAnalyzer();
	QueryParser qp = new QueryParser("field", a);
	qp.setOperator(QueryParser.DEFAULT_OPERATOR_OR);
	return qp.parse(query);
    }

    public void assertQueryEquals(String query, Analyzer a, String result)
	throws Exception {
	Query q = getQuery(query, a);
	String s = q.toString("field");
	if (!s.equals(result)) {
	    fail("Query /" + query + "/ yielded /" + s
		+ "/, expecting /" + result + "/");
	}
    }

    public Query getQueryDOA(String query, Analyzer a)
	throws Exception
    {
	if (a == null)
	    a = new SimpleAnalyzer();
	QueryParser qp = new QueryParser("field", a);
	qp.setOperator(QueryParser.DEFAULT_OPERATOR_AND);
	return qp.parse(query);
    }

    public void assertQueryEqualsDOA(String query, Analyzer a, String result)
	throws Exception
    {
	Query q = getQueryDOA(query, a);
	String s = q.toString("field");
	if (!s.equals(result))
	{
	    fail("Query /" + query + "/ yielded /" + s
		+ "/, expecting /" + result + "/");
	}
    }

    public void testSimple() throws Exception {
	assertQueryEquals("term term term", null, "term term term");
	assertQueryEquals("türm term term", null, "türm term term");
	assertQueryEquals("ümlaut", null, "ümlaut");

	assertQueryEquals("a AND b", null, "+a +b");
	assertQueryEquals("(a AND b)", null, "+a +b");
	assertQueryEquals("c OR (a AND b)", null, "c (+a +b)");
	assertQueryEquals("a AND NOT b", null, "+a -b");
	assertQueryEquals("a AND -b", null, "+a -b");
	assertQueryEquals("a AND !b", null, "+a -b");
	assertQueryEquals("a && b", null, "+a +b");
	assertQueryEquals("a && ! b", null, "+a -b");

	assertQueryEquals("a OR b", null, "a b");
	assertQueryEquals("a || b", null, "a b");
	assertQueryEquals("a OR !b", null, "a -b");
	assertQueryEquals("a OR ! b", null, "a -b");
	assertQueryEquals("a OR -b", null, "a -b");

	assertQueryEquals("+term -term term", null, "+term -term term");
	assertQueryEquals("foo:term AND field:anotherTerm", null,
	    "+foo:term +anotherterm");
	assertQueryEquals("term AND \"phrase phrase\"", null,
	    "+term +\"phrase phrase\"");
	assertQueryEquals("\"hello there\"", null, "\"hello there\"");
	assertTrue(getQuery("a AND b", null) instanceof BooleanQuery);
	assertTrue(getQuery("hello", null) instanceof TermQuery);
	assertTrue(getQuery("\"hello there\"", null) instanceof PhraseQuery);

	assertQueryEquals("germ term^2.0", null, "germ term^2.0");
	assertQueryEquals("(term)^2.0", null, "term^2.0");
	assertQueryEquals("(germ term)^2.0", null, "(germ term)^2.0");
	assertQueryEquals("term^2.0", null, "term^2.0");
	assertQueryEquals("term^2", null, "term^2.0");
	assertQueryEquals("\"germ term\"^2.0", null, "\"germ term\"^2.0");
	assertQueryEquals("\"term germ\"^2", null, "\"term germ\"^2.0");

	assertQueryEquals("(foo OR bar) AND (baz OR boo)", null,
	    "+(foo bar) +(baz boo)");
	assertQueryEquals("((a OR b) AND NOT c) OR d", null,
	    "(+(a b) -c) d");
	assertQueryEquals("+(apple \"steve jobs\") -(foo bar baz)", null,
	    "+(apple \"steve jobs\") -(foo bar baz)");
	assertQueryEquals("+title:(dog OR cat) -author:\"bob dole\"", null,
	    "+(title:dog title:cat) -author:\"bob dole\"");
    }

    public void testPunct() throws Exception {
	Analyzer a = new WhitespaceAnalyzer();
	assertQueryEquals("a&b", a, "a&b");
	assertQueryEquals("a&&b", a, "a&&b");
	assertQueryEquals(".NET", a, ".NET");
    }

    public void testSlop() throws Exception {
	assertQueryEquals("\"term germ\"~2", null, "\"term germ\"~2");
	assertQueryEquals("\"term germ\"~2 flork", null, "\"term germ\"~2 flork");
	assertQueryEquals("\"term\"~2", null, "term");
	assertQueryEquals("\" \"~2 germ", null, "germ");
	assertQueryEquals("\"term germ\"~2^2", null, "\"term germ\"~2^2.0");
    }

    public void testNumber() throws Exception {
	// The numbers go away because SimpleAnalzyer ignores them
	assertQueryEquals("3", null, "");
	assertQueryEquals("term 1.0 1 2", null, "term");
	assertQueryEquals("term term1 term2", null, "term term term");

	Analyzer a = new StandardAnalyzer();
	assertQueryEquals("3", a, "3");
	assertQueryEquals("term 1.0 1 2", a, "term 1.0 1 2");
	assertQueryEquals("term term1 term2", a, "term term1 term2");
    }

    public void testWildcard() throws Exception {
	assertQueryEquals("term*", null, "term*");
	assertQueryEquals("term*^2", null, "term*^2.0");
	assertQueryEquals("term~", null, "term~");
	assertQueryEquals("term~^2", null, "term^2.0~");
	assertQueryEquals("term^2~", null, "term^2.0~");
	assertQueryEquals("term*germ", null, "term*germ");
	assertQueryEquals("term*germ^3", null, "term*germ^3.0");

	assertTrue(getQuery("term*", null) instanceof PrefixQuery);
	assertTrue(getQuery("term*^2", null) instanceof PrefixQuery);
	assertTrue(getQuery("term~", null) instanceof FuzzyQuery);
	assertTrue(getQuery("term*germ", null) instanceof WildcardQuery);
    }

    public void testQPA() throws Exception {
	assertQueryEquals("term term term", qpAnalyzer, "term term term");
	assertQueryEquals("term +stop term", qpAnalyzer, "term term");
	assertQueryEquals("term -stop term", qpAnalyzer, "term term");
	assertQueryEquals("drop AND stop AND roll", qpAnalyzer, "+drop +roll");
	assertQueryEquals("term phrase term", qpAnalyzer,
	    "term \"phrase1 phrase2\" term");
	assertQueryEquals("term AND NOT phrase term", qpAnalyzer,
	    "+term -\"phrase1 phrase2\" term");
	assertQueryEquals("stop", qpAnalyzer, "");
	assertTrue(getQuery("term term term", qpAnalyzer) instanceof BooleanQuery);
	assertTrue(getQuery("term +stop", qpAnalyzer) instanceof TermQuery);
    }

    public void testRange() throws Exception {
	assertQueryEquals("[ a TO z]", null, "[a-z]");
	assertTrue(getQuery("[ a TO z]", null) instanceof RangeQuery);
	assertQueryEquals("[ a TO z ]", null, "[a-z]");
	assertQueryEquals("{ a TO z}", null, "{a-z}");
	assertQueryEquals("{ a TO z }", null, "{a-z}");
	assertQueryEquals("{ a TO z }^2.0", null, "{a-z}^2.0");
	assertQueryEquals("[ a TO z] OR bar", null, "[a-z] bar");
	assertQueryEquals("[ a TO z] AND bar", null, "+[a-z] +bar");
	assertQueryEquals("( bar blar { a TO z}) ", null, "bar blar {a-z}");
	assertQueryEquals("gack ( bar blar { a TO z}) ", null, "gack (bar blar {a-z})");
    }

    public String getDate(String s) throws Exception {
	DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
	return DateField.dateToString(df.parse(s));
    }

    public void testDateRange() throws Exception {
	assertQueryEquals("[ 1/1/02 TO 1/4/02]", null,
	    "[" + getDate("1/1/02") + "-" + getDate("1/4/02") + "]");
	assertQueryEquals("{  1/1/02    1/4/02   }", null,
	    "{" + getDate("1/1/02") + "-" + getDate("1/4/02") + "}");
    }

    public void testEscaped() throws Exception {
	Analyzer a = new WhitespaceAnalyzer();
	assertQueryEquals("\\[brackets", a, "\\[brackets");
	assertQueryEquals("\\[brackets", null, "brackets");
	assertQueryEquals("\\\\", a, "\\\\");
	assertQueryEquals("\\+blah", a, "\\+blah");
	assertQueryEquals("\\(blah", a, "\\(blah");
    }

    public void testSimpleDAO()
	throws Exception
    {
	assertQueryEqualsDOA("term term term", null, "+term +term +term");
	assertQueryEqualsDOA("term +term term", null, "+term +term +term");
	assertQueryEqualsDOA("term term +term", null, "+term +term +term");
	assertQueryEqualsDOA("term +term +term", null, "+term +term +term");
	assertQueryEqualsDOA("-term term term", null, "-term +term +term");
    }
}
