package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultipleTermPositions;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Query;

/**
 * PhrasePrefixQuery is a generalized version of PhraseQuery, with an added
 * method {@link #add(Term[])}.
 * To use this class, to search for the phrase "Microsoft app*" first use
 * add(Term) on the term "Microsoft", then find all terms that has "app" as
 * prefix using IndexReader.terms(Term), and use PhrasePrefixQuery.add(Term[]
 * terms) to add them to the query.
 *
 * @author Anders Nielsen
 * @version 1.0
 */
public class PhrasePrefixQuery
    extends Query
{
    private String _field;
    private ArrayList _termArrays = new ArrayList();

    private float _idf = 0.0f;
    private float _weight = 0.0f;

    private int _slop = 0;

    /**
     * Creates a new <code>PhrasePrefixQuery</code> instance.
     *
     */
    public PhrasePrefixQuery()
    {
    }

    /**
     * Describe <code>setSlop</code> method here.
     *
     * @param s an <code>int</code> value
     */
    public void setSlop(int s)
    {
	_slop = s;
    }

    /**
     * Describe <code>getSlop</code> method here.
     *
     * @return an <code>int</code> value
     */
    public int getSlop()
    {
	return _slop;
    }

    /**
     * Describe <code>add</code> method here.
     *
     * @param term a <code>Term</code> value
     */
    public void add(Term term)
    {
	add(new Term[]{term});
    }

    /**
     * Describe <code>add</code> method here.
     *
     * @param terms a <code>Term[]</code> value
     */
    public void add(Term[] terms)
    {
	if (_termArrays.size() == 0)
	    _field = terms[0].field();

      	for (int i=0; i<terms.length; i++)
	{
	    if (terms[i].field() != _field)
	    {
		throw new IllegalArgumentException(
		    "All phrase terms must be in the same field (" + _field + "): "
		    + terms[i]);
	    }
	}

	_termArrays.add(terms);
    }

    Scorer scorer(IndexReader reader)
	throws IOException
    {
    	if (_termArrays.size() == 0)  // optimize zero-term case
	    return null;

	if (_termArrays.size() == 1)  // optimize one-term case
	{
	    Term[] terms = (Term[])_termArrays.get(0);

	    BooleanQuery boq = new BooleanQuery();
	    for (int i=0; i<terms.length; i++)
		boq.add(new TermQuery(terms[i]), false, false);

	    return boq.scorer(reader);
    	}

    	TermPositions[] tps = new TermPositions[_termArrays.size()];
	for (int i=0; i<tps.length; i++)
	{
	    Term[] terms = (Term[])_termArrays.get(i);

	    TermPositions p;
	    if (terms.length > 1)
		p = new MultipleTermPositions(reader, terms);
	    else
		p = reader.termPositions(terms[0]);

	    if (p == null)
		return null;

	    tps[i] = p;
	}

	if (_slop == 0)
	    return new ExactPhraseScorer(tps, reader.norms(_field), _weight);
	else
	    return new SloppyPhraseScorer(tps, _slop, reader.norms(_field), _weight);
    }

    float sumOfSquaredWeights(Searcher searcher)
	throws IOException
    {
	Iterator i = _termArrays.iterator();
	while (i.hasNext())
	{
	    Term[] terms = (Term[])i.next();
	    for (int j=0; j<terms.length; j++)
		_idf += Similarity.idf(terms[j], searcher);
	}

	_weight = _idf * boost;
	return _weight * _weight;
    }

    void normalize(float norm)
    {
	_weight *= norm;
	_weight *= _idf;
    }

    /**
     * Describe <code>toString</code> method here.
     *
     * This method assumes that the first term in a array of terms is the
     * prefix for the whole array. That might not necessarily be so.
     *
     * @param f a <code>String</code> value
     * @return a <code>String</code> value
     */
    public final String toString(String f)
    {
	StringBuffer buffer = new StringBuffer();
	if (!_field.equals(f))
	{
	    buffer.append(_field);
	    buffer.append(":");
	}

	buffer.append("\"");
	Iterator i = _termArrays.iterator();
	while (i.hasNext())
	{
	    Term[] terms = (Term[])i.next();
	    buffer.append(terms[0].text() + (terms.length > 0 ? "*" : ""));
	}
	buffer.append("\"");

	if (_slop != 0)
	{
	    buffer.append("~");
	    buffer.append(_slop);
	}

	if (boost != 1.0f)
	{
	    buffer.append("^");
	    buffer.append(Float.toString(boost));
	}

	return buffer.toString();
    }
}
