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
package org.apache.lucene.search.grouping;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.io.IOException;
import java.util.*;

/**
 * Represents a group that is found during the first pass search.
 *
 * @lucene.experimental
 */
public class SearchGroup<GROUP_VALUE_TYPE> {

  /** The value that defines this group  */
  public GROUP_VALUE_TYPE groupValue;

  /** The sort values used during sorting. These are the
   *  groupSort field values of the highest rank document
   *  (by the groupSort) within the group.  Can be
   * <code>null</code> if <code>fillFields=false</code> had
   * been passed to {@link AbstractFirstPassGroupingCollector#getTopGroups} */
  public Object[] sortValues;

  @Override
  public String toString() {
    return("SearchGroup(groupValue=" + groupValue + " sortValues=" + Arrays.toString(sortValues) + ")");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SearchGroup<?> that = (SearchGroup<?>) o;

    if (groupValue == null) {
      if (that.groupValue != null) {
        return false;
      }
    } else if (!groupValue.equals(that.groupValue)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return groupValue != null ? groupValue.hashCode() : 0;
  }

  private static class ShardIter<T> {
    public final Iterator<SearchGroup<T>> iter;
    public final int shardIndex;

    public ShardIter(Collection<SearchGroup<T>> shard, int shardIndex) {
      this.shardIndex = shardIndex;
      iter = shard.iterator();
      assert iter.hasNext();
    }

    public SearchGroup<T> next() {
      assert iter.hasNext();
      final SearchGroup<T> group = iter.next();
      if (group.sortValues == null) {
        throw new IllegalArgumentException("group.sortValues is null; you must pass fillFields=true to the first pass collector");
      }
      return group;
    }
    
    @Override
    public String toString() {
      return "ShardIter(shard=" + shardIndex + ")";
    }
  }

  // Holds all shards currently on the same group
  private static class MergedGroup<T> {

    // groupValue may be null!
    public final T groupValue;

    public Object[] topValues;
    public final List<ShardIter<T>> shards = new ArrayList<>();
    public int minShardIndex;
    public boolean processed;
    public boolean inQueue;

    public MergedGroup(T groupValue) {
      this.groupValue = groupValue;
    }

    // Only for assert
    private boolean neverEquals(Object _other) {
      if (_other instanceof MergedGroup) {
        MergedGroup<?> other = (MergedGroup<?>) _other;
        if (groupValue == null) {
          assert other.groupValue != null;
        } else {
          assert !groupValue.equals(other.groupValue);
        }
      }
      return true;
    }

    @Override
    public boolean equals(Object _other) {
      // We never have another MergedGroup instance with
      // same groupValue
      assert neverEquals(_other);

      if (_other instanceof MergedGroup) {
        MergedGroup<?> other = (MergedGroup<?>) _other;
        if (groupValue == null) {
          return other == null;
        } else {
          return groupValue.equals(other);
        }
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      if (groupValue == null) {
        return 0;
      } else {
        return groupValue.hashCode();
      }
    }
  }

  private static class GroupComparator<T> implements Comparator<MergedGroup<T>> {

    @SuppressWarnings("rawtypes")
    public final FieldComparator[] comparators;
    
    public final int[] reversed;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public GroupComparator(Sort groupSort) throws IOException {
      final SortField[] sortFields = groupSort.getSort();
      comparators = new FieldComparator[sortFields.length];
      reversed = new int[sortFields.length];
      for (int compIDX = 0; compIDX < sortFields.length; compIDX++) {
        final SortField sortField = sortFields[compIDX];
        comparators[compIDX] = sortField.getComparator(1, compIDX);
        reversed[compIDX] = sortField.getReverse() ? -1 : 1;
      }
    }

    @Override
    @SuppressWarnings({"unchecked","rawtypes"})
    public int compare(MergedGroup<T> group, MergedGroup<T> other) {
      if (group == other) {
        return 0;
      }
      //System.out.println("compare group=" + group + " other=" + other);
      final Object[] groupValues = group.topValues;
      final Object[] otherValues = other.topValues;
      //System.out.println("  groupValues=" + groupValues + " otherValues=" + otherValues);
      for (int compIDX = 0;compIDX < comparators.length; compIDX++) {
        final int c = reversed[compIDX] * comparators[compIDX].compareValues(groupValues[compIDX],
                                                                             otherValues[compIDX]);
        if (c != 0) {
          return c;
        }
      }

      // Tie break by min shard index:
      assert group.minShardIndex != other.minShardIndex;
      return group.minShardIndex - other.minShardIndex;
    }
  }

  private static class GroupMerger<T> {

    private final GroupComparator<T> groupComp;
    private final NavigableSet<MergedGroup<T>> queue;
    private final Map<T,MergedGroup<T>> groupsSeen;

    public GroupMerger(Sort groupSort) throws IOException {
      groupComp = new GroupComparator<>(groupSort);
      queue = new TreeSet<>(groupComp);
      groupsSeen = new HashMap<>();
    }

    @SuppressWarnings({"unchecked","rawtypes"})
    private void updateNextGroup(int topN, ShardIter<T> shard) {
      while(shard.iter.hasNext()) {
        final SearchGroup<T> group = shard.next();
        MergedGroup<T> mergedGroup = groupsSeen.get(group.groupValue);
        final boolean isNew = mergedGroup == null;
        //System.out.println("    next group=" + (group.groupValue == null ? "null" : ((BytesRef) group.groupValue).utf8ToString()) + " sort=" + Arrays.toString(group.sortValues));

        if (isNew) {
          // Start a new group:
          //System.out.println("      new");
          mergedGroup = new MergedGroup<>(group.groupValue);
          mergedGroup.minShardIndex = shard.shardIndex;
          assert group.sortValues != null;
          mergedGroup.topValues = group.sortValues;
          groupsSeen.put(group.groupValue, mergedGroup);
          mergedGroup.inQueue = true;
          queue.add(mergedGroup);
        } else if (mergedGroup.processed) {
          // This shard produced a group that we already
          // processed; move on to next group...
          continue;
        } else {
          //System.out.println("      old");
          boolean competes = false;
          for(int compIDX=0;compIDX<groupComp.comparators.length;compIDX++) {
            final int cmp = groupComp.reversed[compIDX] * groupComp.comparators[compIDX].compareValues(group.sortValues[compIDX],
                                                                                                       mergedGroup.topValues[compIDX]);
            if (cmp < 0) {
              // Definitely competes
              competes = true;
              break;
            } else if (cmp > 0) {
              // Definitely does not compete
              break;
            } else if (compIDX == groupComp.comparators.length-1) {
              if (shard.shardIndex < mergedGroup.minShardIndex) {
                competes = true;
              }
            }
          }

          //System.out.println("      competes=" + competes);

          if (competes) {
            // Group's sort changed -- remove & re-insert
            if (mergedGroup.inQueue) {
              queue.remove(mergedGroup);
            }
            mergedGroup.topValues = group.sortValues;
            mergedGroup.minShardIndex = shard.shardIndex;
            queue.add(mergedGroup);
            mergedGroup.inQueue = true;
          }
        }

        mergedGroup.shards.add(shard);
        break;
      }

      // Prune un-competitive groups:
      while(queue.size() > topN) {
        final MergedGroup<T> group = queue.pollLast();
        //System.out.println("PRUNE: " + group);
        group.inQueue = false;
      }
    }

    public Collection<SearchGroup<T>> merge(List<Collection<SearchGroup<T>>> shards, int offset, int topN) {

      final int maxQueueSize = offset + topN;

      //System.out.println("merge");
      // Init queue:
      for(int shardIDX=0;shardIDX<shards.size();shardIDX++) {
        final Collection<SearchGroup<T>> shard = shards.get(shardIDX);
        if (!shard.isEmpty()) {
          //System.out.println("  insert shard=" + shardIDX);
          updateNextGroup(maxQueueSize, new ShardIter<>(shard, shardIDX));
        }
      }

      // Pull merged topN groups:
      final List<SearchGroup<T>> newTopGroups = new ArrayList<>();

      int count = 0;

      while(queue.size() != 0) {
        final MergedGroup<T> group = queue.pollFirst();
        group.processed = true;
        //System.out.println("  pop: shards=" + group.shards + " group=" + (group.groupValue == null ? "null" : (((BytesRef) group.groupValue).utf8ToString())) + " sortValues=" + Arrays.toString(group.topValues));
        if (count++ >= offset) {
          final SearchGroup<T> newGroup = new SearchGroup<>();
          newGroup.groupValue = group.groupValue;
          newGroup.sortValues = group.topValues;
          newTopGroups.add(newGroup);
          if (newTopGroups.size() == topN) {
            break;
          }
        //} else {
        // System.out.println("    skip < offset");
        }

        // Advance all iters in this group:
        for(ShardIter<T> shardIter : group.shards) {
          updateNextGroup(maxQueueSize, shardIter);
        }
      }

      if (newTopGroups.size() == 0) {
        return null;
      } else {
        return newTopGroups;
      }
    }
  }

  /** Merges multiple collections of top groups, for example
   *  obtained from separate index shards.  The provided
   *  groupSort must match how the groups were sorted, and
   *  the provided SearchGroups must have been computed
   *  with fillFields=true passed to {@link
   *  AbstractFirstPassGroupingCollector#getTopGroups}.
   *
   * <p>NOTE: this returns null if the topGroups is empty.
   */
  public static <T> Collection<SearchGroup<T>> merge(List<Collection<SearchGroup<T>>> topGroups, int offset, int topN, Sort groupSort)
    throws IOException {
    if (topGroups.size() == 0) {
      return null;
    } else {
      return new GroupMerger<T>(groupSort).merge(topGroups, offset, topN);
    }
  }
}
