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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;

import java.io.IOException;
import java.util.BitSet;

/**
 * A Filter that restricts search results to a range of values in a given
 * field.
 * 
 * <p>
 * This code borrows heavily from {@link RangeQuery}, but is implemented as a Filter
 * 
 * </p>
 */
public class RangeFilter extends Filter {
    
    private String fieldName;
    private String lowerTerm;
    private String upperTerm;
    private boolean includeLower;
    private boolean includeUpper;

    /**
     * @param fieldName The field this range applies to
     * @param lowerTerm The lower bound on this range
     * @param upperTerm The upper bound on this range
     * @param includeLower Does this range include the lower bound?
     * @param includeUpper Does this range include the upper bound?
     * @throws IllegalArgumentException if both terms are null or if
     *  lowerTerm is null and includeLower is true (similar for upperTerm
     *  and includeUpper)
     */
    public RangeFilter(String fieldName, String lowerTerm, String upperTerm,
                       boolean includeLower, boolean includeUpper) {
        this.fieldName = fieldName;
        this.lowerTerm = lowerTerm;
        this.upperTerm = upperTerm;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
        
        if (null == lowerTerm && null == upperTerm) {
            throw new IllegalArgumentException
                ("At least one value must be non-null");
        }
        if (includeLower && null == lowerTerm) {
            throw new IllegalArgumentException
                ("The lower bound must be non-null to be inclusive");
        }
        if (includeUpper && null == upperTerm) {
            throw new IllegalArgumentException
                ("The upper bound must be non-null to be inclusive");
        }
    }
    
    /**
     * Constructs a filter for field <code>fieldName</code> matching
     * less than or equal to <code>upperTerm</code>.
     */
    public static RangeFilter Less(String fieldName, String upperTerm) {
        return new RangeFilter(fieldName, null, upperTerm, false, true);
    }

    /**
     * Constructs a filter for field <code>fieldName</code> matching
     * greater than or equal to <code>lowerTerm</code>.
     */
    public static RangeFilter More(String fieldName, String lowerTerm) {
        return new RangeFilter(fieldName, lowerTerm, null, true, false);
    }
    
    /**
     * Returns a BitSet with true for documents which should be
     * permitted in search results, and false for those that should
     * not.
     */
    public BitSet bits(IndexReader reader) throws IOException {
        BitSet bits = new BitSet(reader.maxDoc());
        TermEnum enumerator =
            (null != lowerTerm
             ? reader.terms(new Term(fieldName, lowerTerm))
             : reader.terms(new Term(fieldName,"")));
        
        try {
            
            if (enumerator.term() == null) {
                return bits;
            }
            
            boolean checkLower = false;
            if (!includeLower) // make adjustments to set to exclusive
                checkLower = true;
        
            TermDocs termDocs = reader.termDocs();
            try {
                
                do {
                    Term term = enumerator.term();
                    if (term != null && term.field().equals(fieldName)) {
                        if (!checkLower || null==lowerTerm || term.text().compareTo(lowerTerm) > 0) {
                            checkLower = false;
                            if (upperTerm != null) {
                                int compare = upperTerm.compareTo(term.text());
                                /* if beyond the upper term, or is exclusive and
                                 * this is equal to the upper term, break out */
                                if ((compare < 0) ||
                                    (!includeUpper && compare==0)) {
                                    break;
                                }
                            }
                            /* we have a good term, find the docs */
                            
                            termDocs.seek(enumerator.term());
                            while (termDocs.next()) {
                                bits.set(termDocs.doc());
                            }
                        }
                    } else {
                        break;
                    }
                }
                while (enumerator.next());
                
            } finally {
                termDocs.close();
            }
        } finally {
            enumerator.close();
        }

        return bits;
    }
    
    public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append(fieldName);
        buffer.append(":");
        buffer.append(includeLower ? "[" : "{");
        if (null != lowerTerm) {
            buffer.append(lowerTerm);
        }
        buffer.append("-");
        if (null != upperTerm) {
            buffer.append(upperTerm);
        }
        buffer.append(includeUpper ? "]" : "}");
        return buffer.toString();
    }

    /** Returns true if <code>o</code> is equal to this. */
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RangeFilter)) return false;
        RangeFilter other = (RangeFilter) o;

        if (!this.fieldName.equals(other.fieldName)
            || this.includeLower != other.includeLower
            || this.includeUpper != other.includeUpper
           ) { return false; }
        if (this.lowerTerm != null ? !this.lowerTerm.equals(other.lowerTerm) : other.lowerTerm != null) return false;
        if (this.upperTerm != null ? !this.upperTerm.equals(other.upperTerm) : other.upperTerm != null) return false;
        return true;
    }

    /** Returns a hash code value for this object.*/
    public int hashCode() {
      int h = fieldName.hashCode();
      h ^= lowerTerm != null ? lowerTerm.hashCode() : 0xB6ECE882;
      h = (h << 1) | (h >>> 31);  // rotate to distinguish lower from upper
      h ^= (upperTerm != null ? (upperTerm.hashCode()) : 0x91BEC2C2);
      h ^= (includeLower ? 0xD484B933 : 0)
         ^ (includeUpper ? 0x6AE423AC : 0);
      return h;
    }
}
