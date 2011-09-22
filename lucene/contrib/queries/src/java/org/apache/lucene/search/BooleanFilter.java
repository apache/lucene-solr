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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.FixedBitSet;

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
  ArrayList<Filter> shouldFilters = null;
  ArrayList<Filter> notFilters = null;
  ArrayList<Filter> mustFilters = null;
  
  private DocIdSetIterator getDISI(ArrayList<Filter> filters, int index, IndexReader reader)
      throws IOException {
    final DocIdSet set = filters.get(index).getDocIdSet(reader);
    return (set == null) ? null : set.iterator();
  }

  /**
   * Returns the a DocIdSetIterator representing the Boolean composition
   * of the filters that have been added.
   */
  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    FixedBitSet res = null;
    if (shouldFilters != null) {
      for (int i = 0; i < shouldFilters.size(); i++) {
        final DocIdSetIterator disi = getDISI(shouldFilters, i, reader);
        if (disi == null) continue;
        if (res == null) {
          res = new FixedBitSet(reader.maxDoc());
        }
        res.or(disi);
      }
    }
    
    if (notFilters != null) {
      for (int i = 0; i < notFilters.size(); i++) {
        if (res == null) {
          res = new FixedBitSet(reader.maxDoc());
          res.set(0, reader.maxDoc()); // NOTE: may set bits on deleted docs
        }
        final DocIdSetIterator disi = getDISI(notFilters, i, reader);
        if (disi != null) {
          res.andNot(disi);
        }
      }
    }
    
    if (mustFilters != null) {
      for (int i = 0; i < mustFilters.size(); i++) {
        final DocIdSetIterator disi = getDISI(mustFilters, i, reader);
        if (disi == null) {
          return DocIdSet.EMPTY_DOCIDSET; // no documents can match
        }
        if (res == null) {
          res = new FixedBitSet(reader.maxDoc());
          res.or(disi);
        } else {
          res.and(disi);
        }
      }
    }

    return res != null ? res : DocIdSet.EMPTY_DOCIDSET;
  }

  /**
  * Adds a new FilterClause to the Boolean Filter container
  * @param filterClause A FilterClause object containing a Filter and an Occur parameter
  */
  public void add(FilterClause filterClause)
  {
    if (filterClause.getOccur().equals(Occur.MUST)) {
      if (mustFilters==null) {
        mustFilters=new ArrayList<Filter>();
      }
      mustFilters.add(filterClause.getFilter());
    }
    if (filterClause.getOccur().equals(Occur.SHOULD)) {
      if (shouldFilters==null) {
        shouldFilters=new ArrayList<Filter>();
      }
      shouldFilters.add(filterClause.getFilter());
    }
    if (filterClause.getOccur().equals(Occur.MUST_NOT)) {
      if (notFilters==null) {
        notFilters=new ArrayList<Filter>();
      }
      notFilters.add(filterClause.getFilter());
    }
  }

  private boolean equalFilters(ArrayList<Filter> filters1, ArrayList<Filter> filters2)
  {
     return (filters1 == filters2) ||
              ((filters1 != null) && filters1.equals(filters2));
  }
  
  @Override
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

  @Override
  public int hashCode()
  {
    int hash=7;
    hash = 31 * hash + (null == mustFilters ? 0 : mustFilters.hashCode());
    hash = 31 * hash + (null == notFilters ? 0 : notFilters.hashCode());
    hash = 31 * hash + (null == shouldFilters ? 0 : shouldFilters.hashCode());
    return hash;
  }
  
  /** Prints a user-readable version of this query. */
  @Override
  public String toString()
  {
    StringBuilder buffer = new StringBuilder();
    buffer.append("BooleanFilter(");
    appendFilters(shouldFilters, "", buffer);
    appendFilters(mustFilters, "+", buffer);
    appendFilters(notFilters, "-", buffer);
    buffer.append(")");
    return buffer.toString();
  }
  
  private void appendFilters(ArrayList<Filter> filters, String occurString, StringBuilder buffer)
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
