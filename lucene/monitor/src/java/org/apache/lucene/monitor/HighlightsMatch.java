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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * QueryMatch object that contains the hit positions of a matching Query
 * <p>
 * If the Query does not support interval iteration (eg, if it gets re-written to
 * a Filter), then no hits will be reported, but an IntervalsQueryMatch will still
 * be returned from an IntervalsMatcher to indicate a match.
 */
public class HighlightsMatch extends QueryMatch {

  public static final MatcherFactory<HighlightsMatch> MATCHER = searcher -> new CandidateMatcher<HighlightsMatch>(searcher) {

    @Override
    protected void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) throws IOException {
      Weight w = searcher.createWeight(searcher.rewrite(matchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
      for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
          Matches matches = w.matches(ctx, i);
          if (matches != null) {
            addMatch(buildMatch(matches, queryId), i);
          }
        }
      }
    }

    @Override
    public HighlightsMatch resolve(HighlightsMatch match1, HighlightsMatch match2) {
      return HighlightsMatch.merge(match1.getQueryId(), match1, match2);
    }

    private HighlightsMatch buildMatch(Matches matches, String queryId) throws IOException {
      HighlightsMatch m = new HighlightsMatch(queryId);
      for (String field : matches) {
        MatchesIterator mi = matches.getMatches(field);
        while (mi.next()) {
          MatchesIterator sub = mi.getSubMatches();
          if (sub != null) {
            while (sub.next()) {
              m.addHit(field, sub.startPosition(), sub.endPosition(), sub.startOffset(), sub.endOffset());
            }
          }
          else {
            m.addHit(field, mi.startPosition(), mi.endPosition(), mi.startOffset(), mi.endOffset());
          }
        }
      }
      return m;
    }
  };

  private final Map<String, Set<Hit>> hits;

  HighlightsMatch(String queryId) {
    super(queryId);
    this.hits = new TreeMap<>();
  }

  /**
   * @return a map of hits per field
   */
  public Map<String, Set<Hit>> getHits() {
    return Collections.unmodifiableMap(this.hits);
  }

  /**
   * @return the fields in which matches have been found
   */
  public Set<String> getFields() {
    return Collections.unmodifiableSet(hits.keySet());
  }

  /**
   * Get the hits for a specific field
   *
   * @param field the field
   * @return the Hits found in this field
   */
  public Collection<Hit> getHits(String field) {
    Collection<Hit> found = hits.get(field);
    if (found != null)
      return Collections.unmodifiableCollection(found);
    return Collections.emptyList();
  }

  /**
   * @return the total number of hits for the query
   */
  public int getHitCount() {
    int c = 0;
    for (Set<Hit> fieldhits : hits.values()) {
      c += fieldhits.size();
    }
    return c;
  }

  static HighlightsMatch merge(String queryId, HighlightsMatch... matches) {
    HighlightsMatch newMatch = new HighlightsMatch(queryId);
    for (HighlightsMatch match : matches) {
      for (String field : match.getFields()) {
        Set<Hit> hitSet = newMatch.hits.computeIfAbsent(field, f -> new TreeSet<>());
        hitSet.addAll(match.getHits(field));
      }
    }
    return newMatch;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HighlightsMatch)) return false;
    if (!super.equals(o)) return false;

    HighlightsMatch that = (HighlightsMatch) o;

    if (hits != null ? !hits.equals(that.hits) : that.hits != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (hits != null ? hits.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return super.toString() + "{hits=" + hits + "}";
  }

  void addHit(String field, int startPos, int endPos, int startOffset, int endOffset) {
    Set<Hit> hitSet = hits.computeIfAbsent(field, f -> new TreeSet<>());
    hitSet.add(new Hit(startPos, startOffset, endPos, endOffset));
  }

  /**
   * Represents an individual hit
   */
  public static class Hit implements Comparable<Hit> {

    /**
     * The start position
     */
    public final int startPosition;

    /**
     * The start offset
     */
    public final int startOffset;

    /**
     * The end positions
     */
    public final int endPosition;

    /**
     * The end offset
     */
    public final int endOffset;

    public Hit(int startPosition, int startOffset, int endPosition, int endOffset) {
      this.startPosition = startPosition;
      this.startOffset = startOffset;
      this.endPosition = endPosition;
      this.endOffset = endOffset;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof Hit))
        return false;
      Hit other = (Hit) obj;
      return this.startOffset == other.startOffset &&
          this.endOffset == other.endOffset &&
          this.startPosition == other.startPosition &&
          this.endPosition == other.endPosition;
    }

    @Override
    public int hashCode() {
      int result = startPosition;
      result = 31 * result + startOffset;
      result = 31 * result + endPosition;
      result = 31 * result + endOffset;
      return result;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "%d(%d)->%d(%d)", startPosition, startOffset, endPosition, endOffset);
    }

    @Override
    public int compareTo(Hit other) {
      if (this.startPosition != other.startPosition)
        return Integer.compare(this.startPosition, other.startPosition);
      return Integer.compare(this.endPosition, other.endPosition);
    }
  }
}
