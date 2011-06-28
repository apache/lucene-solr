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
package org.apache.solr.handler.component;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.MissingStringLastComparatorSource;

import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;
import java.util.List;
import java.util.ArrayList;

public class ShardDoc {
  public String shard;
  public String shardAddress;  // TODO
  
  int orderInShard;
    // the position of this doc within the shard... this can be used
    // to short-circuit comparisons if the shard is equal, and can
    // also be used to break ties within the same shard.

  Object id;
    // this is currently the uniqueKeyField but
    // may be replaced with internal docid in a future release.

  Float score;

  NamedList sortFieldValues;
  // sort field values for *all* docs in a particular shard.
  // this doc's values are in position orderInShard

  // TODO: store the SolrDocument here?
  // Store the order in the merged list for lookup when getting stored fields?
  // (other components need this ordering to store data in order, like highlighting)
  // but we shouldn't expose uniqueKey (have a map by it) until the stored-field
  // retrieval stage.

  int positionInResponse;
  // the ordinal position in the merged response arraylist  

  @Override
  public String toString(){
    return "id="+id
            +" ,score="+score
            +" ,shard="+shard
            +" ,orderInShard="+orderInShard
            +" ,positionInResponse="+positionInResponse
            +" ,sortFieldValues="+sortFieldValues;
  }
}



// used by distributed search to merge results.
class ShardFieldSortedHitQueue extends PriorityQueue {

  /** Stores a comparator corresponding to each field being sorted by */
  protected Comparator[] comparators;

  /** Stores the sort criteria being used. */
  protected SortField[] fields;

  /** The order of these fieldNames should correspond to the order of sort field values retrieved from the shard */
  protected List<String> fieldNames = new ArrayList<String>();

  public ShardFieldSortedHitQueue(SortField[] fields, int size) {
    super(size);
    final int n = fields.length;
    comparators = new Comparator[n];
    this.fields = new SortField[n];
    for (int i = 0; i < n; ++i) {

      // keep track of the named fields
      SortField.Type type = fields[i].getType();
      if (type!=SortField.Type.SCORE && type!=SortField.Type.DOC) {
        fieldNames.add(fields[i].getField());
      }

      String fieldname = fields[i].getField();
      comparators[i] = getCachedComparator(fieldname, fields[i]
          .getType(), fields[i].getComparatorSource());

     if (fields[i].getType() == SortField.Type.STRING) {
        this.fields[i] = new SortField(fieldname, SortField.Type.STRING,
            fields[i].getReverse());
      } else {
        this.fields[i] = new SortField(fieldname, fields[i].getType(),
            fields[i].getReverse());
      }

      //System.out.println("%%%%%%%%%%%%%%%%%% got "+fields[i].getType() +"   for "+ fieldname +"  fields[i].getReverse(): "+fields[i].getReverse());
    }
  }

  @Override
  protected boolean lessThan(Object objA, Object objB) {
    ShardDoc docA = (ShardDoc)objA;
    ShardDoc docB = (ShardDoc)objB;

    // If these docs are from the same shard, then the relative order
    // is how they appeared in the response from that shard.    
    if (docA.shard == docB.shard) {
      // if docA has a smaller position, it should be "larger" so it
      // comes before docB.
      // This will handle sorting by docid within the same shard

      // comment this out to test comparators.
      return !(docA.orderInShard < docB.orderInShard);
    }


    // run comparators
    final int n = comparators.length;
    int c = 0;
    for (int i = 0; i < n && c == 0; i++) {
      c = (fields[i].getReverse()) ? comparators[i].compare(docB, docA)
          : comparators[i].compare(docA, docB);
    }

    // solve tiebreaks by comparing shards (similar to using docid)
    // smaller docid's beat larger ids, so reverse the natural ordering
    if (c == 0) {
      c = -docA.shard.compareTo(docB.shard);
    }

    return c < 0;
  }

  Comparator getCachedComparator(String fieldname, SortField.Type type, FieldComparatorSource factory) {
    Comparator comparator = null;
    switch (type) {
    case SCORE:
      comparator = comparatorScore(fieldname);
      break;
    case STRING:
      comparator = comparatorNatural(fieldname);
      break;
    case CUSTOM:
      if (factory instanceof MissingStringLastComparatorSource){
        comparator = comparatorMissingStringLast(fieldname);
      } else {
        // TODO: support other types such as random... is there a way to
        // support generically?  Perhaps just comparing Object
        comparator = comparatorNatural(fieldname);
        // throw new RuntimeException("Custom sort not supported factory is "+factory.getClass());
      }
      break;
    case DOC:
      // TODO: we can support this!
      throw new RuntimeException("Doc sort not supported");
    default:
      comparator = comparatorNatural(fieldname);
      break;
    }
    return comparator;
  }

  class ShardComparator implements Comparator {
    String fieldName;
    int fieldNum;
    public ShardComparator(String fieldName) {
      this.fieldName = fieldName;
      this.fieldNum=0;
      for (int i=0; i<fieldNames.size(); i++) {
        if (fieldNames.get(i).equals(fieldName)) {
          this.fieldNum = i;
          break;
        }
      }
    }

    Object sortVal(ShardDoc shardDoc) {
      assert(shardDoc.sortFieldValues.getName(fieldNum).equals(fieldName));
      List lst = (List)shardDoc.sortFieldValues.getVal(fieldNum);
      return lst.get(shardDoc.orderInShard);
    }

    public int compare(Object o1, Object o2) {
      return 0;
    }
  }

  static Comparator comparatorScore(final String fieldName) {
    return new Comparator() {
      public final int compare(final Object o1, final Object o2) {
        ShardDoc e1 = (ShardDoc) o1;
        ShardDoc e2 = (ShardDoc) o2;

        final float f1 = e1.score;
        final float f2 = e2.score;
        if (f1 < f2)
          return -1;
        if (f1 > f2)
          return 1;
        return 0;
      }
    };
  }

  // The lucene natural sort ordering corresponds to numeric
  // and string natural sort orderings (ascending).  Since
  // the PriorityQueue keeps the biggest elements by default,
  // we need to reverse the natural compare ordering so that the
  // smallest elements are kept instead of the largest... hence
  // the negative sign on the final compareTo().
  Comparator comparatorNatural(String fieldName) {
    return new ShardComparator(fieldName) {
      @Override
      public final int compare(final Object o1, final Object o2) {
        ShardDoc sd1 = (ShardDoc) o1;
        ShardDoc sd2 = (ShardDoc) o2;
        Comparable v1 = (Comparable)sortVal(sd1);
        Comparable v2 = (Comparable)sortVal(sd2);
        if (v1==v2)
          return 0;
        if (v1==null)
          return 1;
        if(v2==null)
          return -1;
        return -v1.compareTo(v2);
      }
    };
  }


  Comparator comparatorStringLocale(final String fieldName,
      Locale locale) {
    final Collator collator = Collator.getInstance(locale);
    return new ShardComparator(fieldName) {
      @Override
      public final int compare(final Object o1, final Object o2) {
        ShardDoc sd1 = (ShardDoc) o1;
        ShardDoc sd2 = (ShardDoc) o2;
        Comparable v1 = (Comparable)sortVal(sd1);
        Comparable v2 = (Comparable)sortVal(sd2);
        if (v1==v2)
          return 0;
        if (v1==null)
          return 1;
        if(v2==null)
          return -1;
        return -collator.compare(v1,v2);
      }
    };
  }


  Comparator comparatorMissingStringLast(final String fieldName) {
     return new ShardComparator(fieldName) {
      @Override
      public final int compare(final Object o1, final Object o2) {
        ShardDoc sd1 = (ShardDoc) o1;
        ShardDoc sd2 = (ShardDoc) o2;
        Comparable v1 = (Comparable)sortVal(sd1);
        Comparable v2 = (Comparable)sortVal(sd2);
        if (v1==v2)
          return 0;
        if (v1==null)
          return -1;
        if(v2==null)
          return 1;
        return -v1.compareTo(v2);
      }
    };
  }

}
