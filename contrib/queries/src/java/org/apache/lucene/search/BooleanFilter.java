package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.DocIdBitSet;
import org.apache.lucene.util.OpenBitSet;
import org.apache.lucene.util.OpenBitSetDISI;
import org.apache.lucene.util.SortedVIntList;

/**
 * A container Filter that allows Boolean composition of Filters.
 * Filters are allocated into one of three logical constructs;
 * SHOULD, MUST NOT, MUST
 * The results Filter BitSet is constructed as follows:
 * SHOULD Filters are OR'd together
 * The resulting Filter is NOT'd with the NOT Filters
 * The resulting Filter is AND'd with the MUST Filters
 */

public class BooleanFilter extends Filter
{
  ArrayList shouldFilters = null;
  ArrayList notFilters = null;
  ArrayList mustFilters = null;
  
  private DocIdSetIterator getDISI(ArrayList filters, int index, IndexReader reader)
  throws IOException
  {
    return ((Filter)filters.get(index)).getDocIdSet(reader).iterator();
  }

  /**
   * Returns the a DocIdSetIterator representing the Boolean composition
   * of the filters that have been added.
   */
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException
  {
    OpenBitSetDISI res = null;
  
    if (shouldFilters != null) {
      for (int i = 0; i < shouldFilters.size(); i++) {
        if (res == null) {
          res = new OpenBitSetDISI(getDISI(shouldFilters, i, reader), reader.maxDoc());
        } else { 
          DocIdSet dis = ((Filter)shouldFilters.get(i)).getDocIdSet(reader);
          if(dis instanceof OpenBitSet) {
            // optimized case for OpenBitSets
            res.or((OpenBitSet) dis);
          } else {
            res.inPlaceOr(getDISI(shouldFilters, i, reader));
          }
        }
      }
    }
    
    if (notFilters!=null) {
      for (int i = 0; i < notFilters.size(); i++) {
        if (res == null) {
          res = new OpenBitSetDISI(getDISI(notFilters, i, reader), reader.maxDoc());
          res.flip(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
        } else {
          DocIdSet dis = ((Filter)notFilters.get(i)).getDocIdSet(reader);
          if(dis instanceof OpenBitSet) {
            // optimized case for OpenBitSets
            res.andNot((OpenBitSet) dis);
          } else {
            res.inPlaceNot(getDISI(notFilters, i, reader));
          }
        }
      }
    }
    
    if (mustFilters!=null) {
      for (int i = 0; i < mustFilters.size(); i++) {
        if (res == null) {
          res = new OpenBitSetDISI(getDISI(mustFilters, i, reader), reader.maxDoc());
        } else {
          DocIdSet dis = ((Filter)mustFilters.get(i)).getDocIdSet(reader);
          if(dis instanceof OpenBitSet) {
            // optimized case for OpenBitSets
            res.and((OpenBitSet) dis);
          } else {
            res.inPlaceAnd(getDISI(mustFilters, i, reader));
          }
        }
      }
    }
    
    if (res !=null)
      return finalResult(res, reader.maxDoc());

    if (emptyDocIdSet == null)
      emptyDocIdSet = new OpenBitSetDISI(1);

    return emptyDocIdSet;
  }

  /** Provide a SortedVIntList when it is definitely smaller than an OpenBitSet */
  protected DocIdSet finalResult(OpenBitSetDISI result, int maxDocs) {
    return (result.cardinality() < (maxDocs / 9))
      ? (DocIdSet) new SortedVIntList(result)
      : (DocIdSet) result;
  }

  private static DocIdSet emptyDocIdSet = null;

  /**
  * Adds a new FilterClause to the Boolean Filter container
  * @param filterClause A FilterClause object containing a Filter and an Occur parameter
  */
  
  public void add(FilterClause filterClause)
  {
    if (filterClause.getOccur().equals(Occur.MUST)) {
      if (mustFilters==null) {
        mustFilters=new ArrayList();
      }
      mustFilters.add(filterClause.getFilter());
    }
    if (filterClause.getOccur().equals(Occur.SHOULD)) {
      if (shouldFilters==null) {
        shouldFilters=new ArrayList();
      }
      shouldFilters.add(filterClause.getFilter());
    }
    if (filterClause.getOccur().equals(Occur.MUST_NOT)) {
      if (notFilters==null) {
        notFilters=new ArrayList();
      }
      notFilters.add(filterClause.getFilter());
    }
  }

  private boolean equalFilters(ArrayList filters1, ArrayList filters2)
  {
     return (filters1 == filters2) ||
              ((filters1 != null) && filters1.equals(filters2));
  }
  
  public boolean equals(Object obj)
  {
    if (this == obj)
      return true;

    if ((obj == null) || (obj.getClass() != this.getClass()))
      return false;

    BooleanFilter other = (BooleanFilter)obj;
    return equalFilters(notFilters, other.notFilters)
        && equalFilters(mustFilters, other.mustFilters)
        && equalFilters(shouldFilters, other.shouldFilters);
  }

  public int hashCode()
  {
    int hash=7;
    hash = 31 * hash + (null == mustFilters ? 0 : mustFilters.hashCode());
    hash = 31 * hash + (null == notFilters ? 0 : notFilters.hashCode());
    hash = 31 * hash + (null == shouldFilters ? 0 : shouldFilters.hashCode());
    return hash;
  }
  
  /** Prints a user-readable version of this query. */
  public String toString()
  {
    StringBuffer buffer = new StringBuffer();
    buffer.append("BooleanFilter(");
    appendFilters(shouldFilters, "", buffer);
    appendFilters(mustFilters, "+", buffer);
    appendFilters(notFilters, "-", buffer);
    buffer.append(")");
    return buffer.toString();
  }
  
  private void appendFilters(ArrayList filters, String occurString, StringBuffer buffer)
  {
    if (filters != null) {
      for (int i = 0; i < filters.size(); i++) {
        buffer.append(' ');
        buffer.append(occurString);
        buffer.append(filters.get(i).toString());
      }
    }
  }    
}
