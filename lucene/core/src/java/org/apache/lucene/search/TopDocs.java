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

import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

/** Represents hits returned by {@link
 * IndexSearcher#search(Query,Filter,int)} and {@link
 * IndexSearcher#search(Query,int)}. */
public class TopDocs {

  /** The total number of hits for the query. */
  public int totalHits;

  /** The top hits for the query. */
  public ScoreDoc[] scoreDocs;

  /** Stores the maximum score value encountered, needed for normalizing. */
  private float maxScore;
  
  /**
   * Returns the maximum score value encountered. Note that in case
   * scores are not tracked, this returns {@link Float#NaN}.
   */
  public float getMaxScore() {
    return maxScore;
  }
  
  /** Sets the maximum score value encountered. */
  public void setMaxScore(float maxScore) {
    this.maxScore=maxScore;
  }

  /** Constructs a TopDocs with a default maxScore=Float.NaN. */
  TopDocs(int totalHits, ScoreDoc[] scoreDocs) {
    this(totalHits, scoreDocs, Float.NaN);
  }

  public TopDocs(int totalHits, ScoreDoc[] scoreDocs, float maxScore) {
    this.totalHits = totalHits;
    this.scoreDocs = scoreDocs;
    this.maxScore = maxScore;
  }

  // Refers to one hit:
  private static class ShardRef {
    // Which shard (index into shardHits[]):
    final int shardIndex;

    // Which hit within the shard:
    int hitIndex;

    public ShardRef(int shardIndex) {
      this.shardIndex = shardIndex;
    }

    @Override
    public String toString() {
      return "ShardRef(shardIndex=" + shardIndex + " hitIndex=" + hitIndex + ")";
    }
  };

  // Specialized MergeSortQueue that just merges by
  // relevance score, descending:
  private static class ScoreMergeSortQueue extends PriorityQueue<ShardRef> {
    final ScoreDoc[][] shardHits;

    public ScoreMergeSortQueue(TopDocs[] shardHits) {
      super(shardHits.length);
      this.shardHits = new ScoreDoc[shardHits.length][];
      for(int shardIDX=0;shardIDX<shardHits.length;shardIDX++) {
        this.shardHits[shardIDX] = shardHits[shardIDX].scoreDocs;
      }
    }

    // Returns true if first is < second
    public boolean lessThan(ShardRef first, ShardRef second) {
      assert first != second;
      final float firstScore = shardHits[first.shardIndex][first.hitIndex].score;
      final float secondScore = shardHits[second.shardIndex][second.hitIndex].score;

      if (firstScore < secondScore) {
        return false;
      } else if (firstScore > secondScore) {
        return true;
      } else {
        // Tie break: earlier shard wins
        if (first.shardIndex < second.shardIndex) {
          return true;
        } else if (first.shardIndex > second.shardIndex) {
          return false;
        } else {
          // Tie break in same shard: resolve however the
          // shard had resolved it:
          assert first.hitIndex != second.hitIndex;
          return first.hitIndex < second.hitIndex;
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes","unchecked"})
  private static class MergeSortQueue extends PriorityQueue<ShardRef> {
    // These are really FieldDoc instances:
    final ScoreDoc[][] shardHits;
    final FieldComparator<?>[] comparators;
    final int[] reverseMul;

    public MergeSortQueue(Sort sort, TopDocs[] shardHits) throws IOException {
      super(shardHits.length);
      this.shardHits = new ScoreDoc[shardHits.length][];
      for(int shardIDX=0;shardIDX<shardHits.length;shardIDX++) {
        final ScoreDoc[] shard = shardHits[shardIDX].scoreDocs;
        //System.out.println("  init shardIdx=" + shardIDX + " hits=" + shard);
        if (shard != null) {
          this.shardHits[shardIDX] = shard;
          // Fail gracefully if API is misused:
          for(int hitIDX=0;hitIDX<shard.length;hitIDX++) {
            final ScoreDoc sd = shard[hitIDX];
            if (!(sd instanceof FieldDoc)) {
              throw new IllegalArgumentException("shard " + shardIDX + " was not sorted by the provided Sort (expected FieldDoc but got ScoreDoc)");
            }
            final FieldDoc fd = (FieldDoc) sd;
            if (fd.fields == null) {
              throw new IllegalArgumentException("shard " + shardIDX + " did not set sort field values (FieldDoc.fields is null); you must pass fillFields=true to IndexSearcher.search on each shard");
            }
          }
        }
      }

      final SortField[] sortFields = sort.getSort();
      comparators = new FieldComparator[sortFields.length];
      reverseMul = new int[sortFields.length];
      for(int compIDX=0;compIDX<sortFields.length;compIDX++) {
        final SortField sortField = sortFields[compIDX];
        comparators[compIDX] = sortField.getComparator(1, compIDX);
        reverseMul[compIDX] = sortField.getReverse() ? -1 : 1;
      }
    }

    // Returns true if first is < second
    @SuppressWarnings({"unchecked","rawtypes"})
    public boolean lessThan(ShardRef first, ShardRef second) {
      assert first != second;
      final FieldDoc firstFD = (FieldDoc) shardHits[first.shardIndex][first.hitIndex];
      final FieldDoc secondFD = (FieldDoc) shardHits[second.shardIndex][second.hitIndex];
      //System.out.println("  lessThan:\n     first=" + first + " doc=" + firstFD.doc + " score=" + firstFD.score + "\n    second=" + second + " doc=" + secondFD.doc + " score=" + secondFD.score);

      for(int compIDX=0;compIDX<comparators.length;compIDX++) {
        final FieldComparator comp = comparators[compIDX];
        //System.out.println("    cmp idx=" + compIDX + " cmp1=" + firstFD.fields[compIDX] + " cmp2=" + secondFD.fields[compIDX] + " reverse=" + reverseMul[compIDX]);

        final int cmp = reverseMul[compIDX] * comp.compareValues(firstFD.fields[compIDX], secondFD.fields[compIDX]);
        
        if (cmp != 0) {
          //System.out.println("    return " + (cmp < 0));
          return cmp < 0;
        }
      }

      // Tie break: earlier shard wins
      if (first.shardIndex < second.shardIndex) {
        //System.out.println("    return tb true");
        return true;
      } else if (first.shardIndex > second.shardIndex) {
        //System.out.println("    return tb false");
        return false;
      } else {
        // Tie break in same shard: resolve however the
        // shard had resolved it:
        //System.out.println("    return tb " + (first.hitIndex < second.hitIndex));
        assert first.hitIndex != second.hitIndex;
        return first.hitIndex < second.hitIndex;
      }
    }
  }

  /** Returns a new TopDocs, containing topN results across
   *  the provided TopDocs, sorting by the specified {@link
   *  Sort}.  Each of the TopDocs must have been sorted by
   *  the same Sort, and sort field values must have been
   *  filled (ie, <code>fillFields=true</code> must be
   *  passed to {@link
   *  TopFieldCollector#create}.
   *
   * <p>Pass sort=null to merge sort by score descending.
   *
   * @lucene.experimental */
  public static TopDocs merge(Sort sort, int topN, TopDocs[] shardHits) throws IOException {

    final PriorityQueue<ShardRef> queue;
    if (sort == null) {
      queue = new ScoreMergeSortQueue(shardHits);
    } else {
      queue = new MergeSortQueue(sort, shardHits);
    }

    int totalHitCount = 0;
    int availHitCount = 0;
    float maxScore = Float.MIN_VALUE;
    for(int shardIDX=0;shardIDX<shardHits.length;shardIDX++) {
      final TopDocs shard = shardHits[shardIDX];
      // totalHits can be non-zero even if no hits were
      // collected, when searchAfter was used:
      totalHitCount += shard.totalHits;
      if (shard.scoreDocs != null && shard.scoreDocs.length > 0) {
        availHitCount += shard.scoreDocs.length;
        queue.add(new ShardRef(shardIDX));
        maxScore = Math.max(maxScore, shard.getMaxScore());
        //System.out.println("  maxScore now " + maxScore + " vs " + shard.getMaxScore());
      }
    }

    if (availHitCount == 0) {
      maxScore = Float.NaN;
    }

    final ScoreDoc[] hits = new ScoreDoc[Math.min(topN, availHitCount)];

    int hitUpto = 0;
    while(hitUpto < hits.length) {
      assert queue.size() > 0;
      ShardRef ref = queue.pop();
      final ScoreDoc hit = shardHits[ref.shardIndex].scoreDocs[ref.hitIndex++];
      hit.shardIndex = ref.shardIndex;
      hits[hitUpto] = hit;

      //System.out.println("  hitUpto=" + hitUpto);
      //System.out.println("    doc=" + hits[hitUpto].doc + " score=" + hits[hitUpto].score);

      hitUpto++;

      if (ref.hitIndex < shardHits[ref.shardIndex].scoreDocs.length) {
        // Not done with this these TopDocs yet:
        queue.add(ref);
      }
    }

    if (sort == null) {
      return new TopDocs(totalHitCount, hits, maxScore);
    } else {
      return new TopFieldDocs(totalHitCount, hits, sort.getSort(), maxScore);
    }
  }
}
