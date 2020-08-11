/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.SolrException;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

// used by distributed search to merge results.
public class ShardFieldSortedHitQueue extends PriorityQueue<ShardDoc> {

  /** Stores a comparator corresponding to each field being sorted by */
  protected Comparator<ShardDoc>[] comparators;

  /** Stores the sort criteria being used. */
  protected SortField[] fields;

  /** The order of these fieldNames should correspond to the order of sort field values retrieved from the shard */
  protected List<String> fieldNames = new ArrayList<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public ShardFieldSortedHitQueue(SortField[] fields, int size, IndexSearcher searcher) {
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
      comparators[i] = getCachedComparator(fields[i], searcher);

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
  protected boolean lessThan(ShardDoc docA, ShardDoc docB) {
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

  Comparator<ShardDoc> getCachedComparator(SortField sortField, IndexSearcher searcher) {
    SortField.Type type = sortField.getType();
    if (type == SortField.Type.SCORE) {
      return (o1, o2) -> {
        final float f1 = o1.score;
        final float f2 = o2.score;
        if (f1 < f2)
          return -1;
        if (f1 > f2)
          return 1;
        return 0;
      };
    } else if (type == SortField.Type.REWRITEABLE) {
      try {
        sortField = sortField.rewrite(searcher);
      } catch (IOException e) {
        throw new SolrException(SERVER_ERROR, "Exception rewriting sort field " + sortField, e);
      }
    }
    return comparatorFieldComparator(sortField);
  }

  abstract class ShardComparator implements Comparator<ShardDoc> {
    final SortField sortField;
    final String fieldName;
    final int fieldNum;

    public ShardComparator(SortField sortField) {
      this.sortField = sortField;
      this.fieldName = sortField.getField();
      int fieldNum = 0;
      for (int i=0; i<fieldNames.size(); i++) {
        if (fieldNames.get(i).equals(fieldName)) {
          fieldNum = i;
          break;
        }
      }
      this.fieldNum = fieldNum;
    }

    Object sortVal(ShardDoc shardDoc) {
      assert(shardDoc.sortFieldValues.getName(fieldNum).equals(fieldName));
      @SuppressWarnings({"rawtypes"})
      List lst = (List)shardDoc.sortFieldValues.getVal(fieldNum);
      return lst.get(shardDoc.orderInShard);
    }
  }

  Comparator<ShardDoc> comparatorFieldComparator(SortField sortField) {
    @SuppressWarnings({"rawtypes"})
    final FieldComparator fieldComparator = sortField.getComparator(0, 0);
    return new ShardComparator(sortField) {
      // Since the PriorityQueue keeps the biggest elements by default,
      // we need to reverse the field compare ordering so that the
      // smallest elements are kept instead of the largest... hence
      // the negative sign.
      @Override
      @SuppressWarnings({"unchecked"})
      public int compare(final ShardDoc o1, final ShardDoc o2) {
        return -fieldComparator.compareValues(sortVal(o1), sortVal(o2));
      }
    };
  }
}