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
import java.util.Vector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

/** A Query that matches documents containing a subset of terms provided by a 
 FilteredTermEnum enumeration. MultiTermQuery is not designed to be used by 
 itself. The reason being that it is not intialized with a FilteredTermEnum 
 enumeration. A FilteredTermEnum enumeration needs to be provided. For example,
 WildcardQuery and FuzzyQuery extends MultiTermQuery to provide WildcardTermEnum
 and FuzzyTermEnum respectively. */
public class MultiTermQuery extends Query {
    private Term term;
    private FilteredTermEnum enum;
    private IndexReader reader;
    private float boost = 1.0f;
    private BooleanQuery query;
    
    /** Enable or disable lucene style toString(field) format */
    private static boolean LUCENE_STYLE_TOSTRING = false;
    
    /** Constructs a query for terms matching <code>term</code>. */
    public MultiTermQuery(Term term) {
        this.term = term;
        this.query = query;
    }
    
    /** Set the TermEnum to be used */
    protected void setEnum(FilteredTermEnum enum) {
        this.enum = enum;
    }
    
    /** Sets the boost for this term to <code>b</code>.  Documents containing
     * this term will (in addition to the normal weightings) have their score
     * multiplied by <code>boost</code>. */
    final public void setBoost(float boost) {
        this.boost = boost;
    }
    
    /** Returns the boost for this term. */
    final public float getBoost() {
        return boost;
    }
    
    final float sumOfSquaredWeights(Searcher searcher) throws IOException {
        return getQuery().sumOfSquaredWeights(searcher);
    }
    
    final void normalize(float norm) {
        try {
            getQuery().normalize(norm);
        } catch (IOException e) {
            throw new RuntimeException(e.toString());
        }
    }
    
    final Scorer scorer(IndexReader reader) throws IOException {
        return getQuery().scorer(reader);
    }
    
    final private BooleanQuery getQuery() throws IOException {
        if (query == null) {
            BooleanQuery q = new BooleanQuery();
            try {
                do {
                    Term t = enum.term();
                    if (t != null) {
                        TermQuery tq = new TermQuery(t);	// found a match
                        tq.setBoost(boost * enum.difference()); // set the boost
                        q.add(tq, false, false);		// add to q
                    }
                } while (enum.next());
            } finally {
                enum.close();
            }
            query = q;
        }
        return query;
    }
    
    /** Prints a user-readable version of this query. */
    public String toString(String field) {
        if (!LUCENE_STYLE_TOSTRING) {
            Query q = null;
            try {
                q = getQuery();
            } catch (Exception e) {}
            if (q != null) {
                return "(" + q.toString(field) + ")";
            }
        }
        StringBuffer buffer = new StringBuffer();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        if (boost != 1.0f) {
            buffer.append("^");
            buffer.append(Float.toString(boost));
        }
        return buffer.toString();
    }
}
