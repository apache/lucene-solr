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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;

/** A Query that matches documents within an exclusive range. */
public class RangeQuery extends Query
{
    private Term lowerTerm;
    private Term upperTerm;
    private boolean inclusive;

    /** Constructs a query selecting all terms greater than
     * <code>lowerTerm</code> but less than <code>upperTerm</code>.
     * There must be at least one term and either term may be null--
     * in which case there is no bound on that side, but if there are
     * two term, both terms <b>must</b> be for the same field.
     */
    public RangeQuery(Term lowerTerm, Term upperTerm, boolean inclusive)
    {
        if (lowerTerm == null && upperTerm == null)
        {
            throw new IllegalArgumentException("At least one term must be non-null");
        }
        if (lowerTerm != null && upperTerm != null && lowerTerm.field() != upperTerm.field())
        {
            throw new IllegalArgumentException("Both terms must be for the same field");
        }
        this.lowerTerm = lowerTerm;
        this.upperTerm = upperTerm;
        this.inclusive = inclusive;
    }

    public Query rewrite(IndexReader reader) throws IOException {
      BooleanQuery query = new BooleanQuery();
      // if we have a lowerTerm, start there. otherwise, start at beginning
      if (lowerTerm == null) lowerTerm = new Term(getField(), "");
      TermEnum enumerator = reader.terms(lowerTerm);
      try {
        String lowerText = null;
        String field;
        boolean checkLower = false;
          if (!inclusive) {             // make adjustments to set to exclusive
            if (lowerTerm != null) {
              lowerText = lowerTerm.text();
              checkLower = true;
            }
            if (upperTerm != null) {
              // set upperTerm to an actual term in the index
              TermEnum uppEnum = reader.terms(upperTerm);
              upperTerm = uppEnum.term();
            }
          }
          String testField = getField();
          do {
            Term term = enumerator.term();
            if (term != null && term.field() == testField) {
              if (!checkLower || term.text().compareTo(lowerText) > 0) {
                checkLower = false;
                if (upperTerm != null) {
                  int compare = upperTerm.compareTo(term);
                  /* if beyond the upper term, or is exclusive and
                   * this is equal to the upper term, break out */
                  if ((compare < 0) || (!inclusive && compare == 0)) break;
                }
                TermQuery tq = new TermQuery(term); // found a match
                tq.setBoost(getBoost());          // set the boost
                query.add(tq, false, false); // add to query
              }
            }
            else {
              break;
            }
          }
          while (enumerator.next());
      } finally {
        enumerator.close();
      }
      return query;
    }

    public Query combine(Query[] queries) {
      return Query.mergeBooleanQueries(queries);
    }

    private String getField()
    {
        return (lowerTerm != null ? lowerTerm.field() : upperTerm.field());
    }

    /** Prints a user-readable version of this query. */
    public String toString(String field)
    {
        StringBuffer buffer = new StringBuffer();
        if (!getField().equals(field))
        {
            buffer.append(getField());
            buffer.append(":");
        }
        buffer.append(inclusive ? "[" : "{");
        buffer.append(lowerTerm != null ? lowerTerm.text() : "null");
        buffer.append(" TO ");
        buffer.append(upperTerm != null ? upperTerm.text() : "null");
        buffer.append(inclusive ? "]" : "}");
        if (getBoost() != 1.0f)
        {
            buffer.append("^");
            buffer.append(Float.toString(getBoost()));
        }
        return buffer.toString();
    }
}
