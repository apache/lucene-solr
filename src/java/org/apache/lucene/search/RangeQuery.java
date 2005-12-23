package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.ToStringUtils;

/**
 * A Query that matches documents within an exclusive range. A RangeQuery
 * is built by QueryParser for input like <code>[010 TO 120]</code>.
 *
 * @version $Id$
 */
public class RangeQuery extends Query
{
    private Term lowerTerm;
    private Term upperTerm;
    private boolean inclusive;

    /** Constructs a query selecting all terms greater than
     * <code>lowerTerm</code> but less than <code>upperTerm</code>.
     * There must be at least one term and either term may be null,
     * in which case there is no bound on that side, but if there are
     * two terms, both terms <b>must</b> be for the same field.
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

        // if we have a lowerTerm, start there. otherwise, start at beginning
        if (lowerTerm != null) {
            this.lowerTerm = lowerTerm;
        }
        else {
            this.lowerTerm = new Term(upperTerm.field(), "");
        }

        this.upperTerm = upperTerm;
        this.inclusive = inclusive;
    }

    public Query rewrite(IndexReader reader) throws IOException {

        BooleanQuery query = new BooleanQuery(true);
        TermEnum enumerator = reader.terms(lowerTerm);

        try {

            boolean checkLower = false;
            if (!inclusive) // make adjustments to set to exclusive
                checkLower = true;

            String testField = getField();

            do {
                Term term = enumerator.term();
                if (term != null && term.field() == testField) {
                    if (!checkLower || term.text().compareTo(lowerTerm.text()) > 0) {
                        checkLower = false;
                        if (upperTerm != null) {
                            int compare = upperTerm.text().compareTo(term.text());
                            /* if beyond the upper term, or is exclusive and
                             * this is equal to the upper term, break out */
                            if ((compare < 0) || (!inclusive && compare == 0))
                                break;
                        }
                        TermQuery tq = new TermQuery(term); // found a match
                        tq.setBoost(getBoost()); // set the boost
                        query.add(tq, BooleanClause.Occur.SHOULD); // add to query
                    }
                }
                else {
                    break;
                }
            }
            while (enumerator.next());
        }
        finally {
            enumerator.close();
        }
        return query;
    }

    /** Returns the field name for this query */
    public String getField() {
      return (lowerTerm != null ? lowerTerm.field() : upperTerm.field());
    }

    /** Returns the lower term of this range query */
    public Term getLowerTerm() { return lowerTerm; }

    /** Returns the upper term of this range query */
    public Term getUpperTerm() { return upperTerm; }

    /** Returns <code>true</code> if the range query is inclusive */
    public boolean isInclusive() { return inclusive; }


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
        buffer.append(ToStringUtils.boost(getBoost()));
        return buffer.toString();
    }

    /** Returns true iff <code>o</code> is equal to this. */
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RangeQuery)) return false;

        final RangeQuery other = (RangeQuery) o;
        if (this.getBoost() != other.getBoost()) return false;
        if (this.inclusive != other.inclusive) return false;
        // one of lowerTerm and upperTerm can be null
        if (this.lowerTerm != null ? !this.lowerTerm.equals(other.lowerTerm) : other.lowerTerm != null) return false;
        if (this.upperTerm != null ? !this.upperTerm.equals(other.upperTerm) : other.upperTerm != null) return false;
        return true;
    }

    /** Returns a hash code value for this object.*/
    public int hashCode() {
      int h = Float.floatToIntBits(getBoost());
      h ^= lowerTerm != null ? lowerTerm.hashCode() : 0;
      // reversible mix to make lower and upper position dependent and
      // to prevent them from cancelling out.
      h ^= (h << 25) | (h >>> 8);
      h ^= upperTerm != null ? upperTerm.hashCode() : 0;
      h ^= this.inclusive ? 0x2742E74A : 0;
      return h;
    }
}
