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

package org.apache.lucene.spatial.tier;

import java.io.IOException;
import java.util.BitSet;
import java.util.logging.Logger;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.Filter;
import org.apache.lucene.spatial.NumberUtils;



/**
 * An implementation of org.apache.lucene.search.RangeFilter that
 * caches values extracted from the index.
 * @deprecated
 * @see CartesianShapeFilter
 */
public class BoundaryBoxFilter extends Filter {

  private static final long serialVersionUID = 1L;
  private String fieldName;
  private String lowerTerm;
  private String upperTerm;
  private boolean includeLower;
  private boolean includeUpper;
  private Logger log = Logger.getLogger(getClass().getName());
  
  // cache of values extracted from the index 
  // TODO: add generics 
  //private Map coords;
  
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
  public BoundaryBoxFilter(String fieldName, String lowerTerm, String upperTerm,
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
   * Returns a BitSet with true for documents which should be
   * permitted in search results, and false for those that should
   * not.
   */
  @Override
  public BitSet bits(IndexReader reader) throws IOException {
    long start = System.currentTimeMillis();
    
    BitSet bits = new BitSet(reader.maxDoc());
    TermEnum enumerator =
        (null != lowerTerm
         ? reader.terms(new Term(fieldName, lowerTerm))
         : reader.terms(new Term(fieldName,"")));
    
    //coords = new HashMap(enumerator.docFreq());
    
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
                // if beyond the upper term, or is exclusive and
                // this is equal to the upper term, break out 
                if ((compare < 0) ||
                  (!includeUpper && compare==0)) {
                  break;
                }
              }
              // we have a good term, find the docs 
              termDocs.seek(enumerator.term());
              while (termDocs.next()) {
                bits.set(termDocs.doc());
              }
            }
          } 
          else {
            break;
          }
        }
        while (enumerator.next());    
      } 
      finally {
        termDocs.close();
      }
    } 
    finally {
      enumerator.close();
    }

    long end = System.currentTimeMillis();
    log.info("BoundaryBox Time Taken: "+ (end - start));
    return bits;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append(fieldName);
    buffer.append(":");
    buffer.append(includeLower ? "[" : "{");
    if (null != lowerTerm) {
      buffer.append(NumberUtils.SortableStr2double(lowerTerm));
    }
    buffer.append("-");
    if (null != upperTerm) {
      buffer.append(NumberUtils.SortableStr2double(upperTerm));
    }
    buffer.append(includeUpper ? "]" : "}");
    return buffer.toString();
  }

  public String getFieldName() {
    return fieldName;
  }

  
  /** 
   * Returns true if <code>o</code> is equal to this.
   * 
   * @see org.apache.lucene.search.RangeFilter#equals
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BoundaryBoxFilter)) return false;
    BoundaryBoxFilter other = (BoundaryBoxFilter) o;

    if (!this.fieldName.equals(other.fieldName)
        || this.includeLower != other.includeLower
        || this.includeUpper != other.includeUpper
       ) { return false; }
    if (this.lowerTerm != null ? !this.lowerTerm.equals(other.lowerTerm) : other.lowerTerm != null) return false;
    if (this.upperTerm != null ? !this.upperTerm.equals(other.upperTerm) : other.upperTerm != null) return false;
    return true;
  }

  /** 
   * Returns a hash code value for this object.
   * 
   * @see org.apache.lucene.search.RangeFilter#hashCode
   */
  @Override
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
