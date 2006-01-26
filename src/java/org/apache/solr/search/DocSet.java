/**
 * Copyright 2006 The Apache Software Foundation
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

package org.apache.solr.search;

import org.apache.solr.core.SolrException;

import java.util.BitSet;

/**
 * <code>DocSet</code> represents an unordered set of Lucene Document Ids.
 * <p>
 * WARNING: Any DocSet returned from SolrIndexSearcher should <b>not</b> be modified as it may have been retrieved from
 * a cache and could be shared.
 * @author yonik
 * @version $Id: DocSet.java,v 1.6 2005/05/13 21:20:15 yonik Exp $
 * @since solr 0.9
 */
public interface DocSet /* extends Collection<Integer> */ {
  public void add(int doc);
  public void addUnique(int doc);

  /**
   * @return The number of document ids in the set.
   */
  public int size();

  /**
   *
   * @param docid
   * @return
   * true if the docid is in the set
   */
  public boolean exists(int docid);

  /**
   *
   * @return an interator that may be used to iterate over all of the documents in the set.
   */
  public DocIterator iterator();

  /**
   * Returns a BitSet view of the DocSet.  Any changes to this BitSet <b>may</b>
   * be reflected in the DocSet, hence if the DocSet is shared or was returned from
   * a SolrIndexSearcher method, it's not safe to modify the BitSet.
   *
   * @return
   * A BitSet with the bit number of every docid set in the set.
   */
  @Deprecated
  public BitSet getBits();

  /**
   * Returns the approximate amount of memory taken by this DocSet.
   * This is only an approximation and doesn't take into account java object overhead.
   *
   * @return
   * the approximate memory consumption in bytes
   */
  public long memSize();

  /**
   * Returns the intersection of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @param other
   * @return a DocSet representing the intersection
   */
  public DocSet intersection(DocSet other);

  /**
   * Returns the number of documents of the intersection of this set with another set.
   * May be more efficient than actually creating the intersection and then getting it's size.
   */
  public int intersectionSize(DocSet other);

  /**
   * Returns the union of this set with another set.  Neither set is modified - a new DocSet is
   * created and returned.
   * @param other
   * @return a DocSet representing the union
   */
  public DocSet union(DocSet other);

  /**
   * Returns the number of documents of the union of this set with another set.
   * May be more efficient than actually creating the union and then getting it's size.
   */
  public int unionSize(DocSet other);

}


abstract class DocSetBase implements DocSet {

  // Not implemented efficiently... for testing purposes only
  public boolean equals(Object obj) {
    if (!(obj instanceof DocSet)) return false;
    DocSet other = (DocSet)obj;
    if (this.size() != other.size()) return false;

    if (this instanceof DocList && other instanceof DocList) {
      // compare ordering
      DocIterator i1=this.iterator();
      DocIterator i2=this.iterator();
      while(i1.hasNext() && i2.hasNext()) {
        if (i1.nextDoc() != i2.nextDoc()) return false;
      }
      return true;
      // don't compare matches
    }

    // if (this.size() != other.size()) return false;
    return this.getBits().equals(other.getBits());
  }

  public void add(int doc) {
    throw new SolrException(500,"Unsupported Operation");
  }

  public void addUnique(int doc) {
    throw new SolrException(500,"Unsupported Operation");
  }

  // Only the inefficient base implementation.  DocSets based on
  // BitSets will return the actual BitSet without making a copy.
  public BitSet getBits() {
    BitSet bits = new BitSet();
    for (DocIterator iter = iterator(); iter.hasNext();) {
      bits.set(iter.nextDoc());
    }
    return bits;
  };

  public DocSet intersection(DocSet other) {
    // intersection is overloaded in HashDocSet to be more
    // efficient, so if "other" is a HashDocSet, dispatch off
    // of it instead.
    if (other instanceof HashDocSet) {
      return other.intersection(this);
    }

    // Default... handle with bitsets.
    BitSet newbits = (BitSet)(this.getBits().clone());
    newbits.and(other.getBits());
    return new BitDocSet(newbits);
  }

  public DocSet union(DocSet other) {
    BitSet newbits = (BitSet)(this.getBits().clone());
    newbits.or(other.getBits());
    return new BitDocSet(newbits);
  }

  // TODO: more efficient implementations
  public int intersectionSize(DocSet other) {
    return intersection(other).size();
  }

  // TODO: more efficient implementations
  public int unionSize(DocSet other) {
    return union(other).size();
  }


}




