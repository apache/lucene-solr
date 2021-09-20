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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;

/**
 * A {@link Collector} which allows running a search with several {@link Collector}s. It offers a
 * static {@link #wrap} method which accepts a list of collectors and wraps them with {@link
 * MultiCollector}, while filtering out the <code>null</code> null ones.
 *
 * <p><b>NOTE:</b>When mixing collectors that want to skip low-scoring hits ({@link
 * ScoreMode#TOP_SCORES}) with ones that require to see all hits, such as mixing {@link
 * TopScoreDocCollector} and {@link TotalHitCountCollector}, it should be faster to run the query
 * twice, once for each collector, rather than using this wrapper on a single search.
 */
public class MultiCollector implements Collector {

  /** See {@link #wrap(Iterable)}. */
  public static Collector wrap(Collector... collectors) {
    return wrap(Arrays.asList(collectors));
  }

  /**
   * Wraps a list of {@link Collector}s with a {@link MultiCollector}. This
   * method works as follows:
   * <ul>
   * <li>Filters out the <code>null</code> collectors, so they are not used
   * during search time.
   * <li>If the input contains 1 real collector (i.e. non-<code>null</code> ),
   * it is returned.
   * <li>Otherwise the method returns a {@link MultiCollector} which wraps the
   * non-<code>null</code> ones.
   * </ul>
   *
   * @throws IllegalArgumentException
   *           if either 0 collectors were input, or all collectors are
   *           <code>null</code>.
   */
  public static Collector wrap(Iterable<? extends Collector> collectors) {
    // For the user's convenience, we allow null collectors to be passed.
    // However, to improve performance, these null collectors are found
    // and dropped from the array we save for actual collection time.
    int n = 0;
    for (Collector c : collectors) {
      if (c != null) {
        n++;
      }
    }

    if (n == 0) {
      throw new IllegalArgumentException("At least 1 collector must not be null");
    } else if (n == 1) {
      // only 1 Collector - return it.
      Collector col = null;
      for (Collector c : collectors) {
        if (c != null) {
          col = c;
          break;
        }
      }
      return col;
    } else {
      Collector[] colls = new Collector[n];
      n = 0;
      for (Collector c : collectors) {
        if (c != null) {
          colls[n++] = c;
        }
      }
      return new MultiCollector(colls);
    }
  }

  private final boolean cacheScores;
  private final Collector[] collectors;

  private MultiCollector(Collector... collectors) {
    this.collectors = collectors;
    int numNeedsScores = 0;
    for (Collector collector : collectors) {
      if (collector.scoreMode().needsScores()) {
        numNeedsScores += 1;
      }
    }
    this.cacheScores = numNeedsScores >= 2;
  }

  @Override
  public ScoreMode scoreMode() {
    ScoreMode scoreMode = null;
    for (Collector collector : collectors) {
      if (scoreMode == null) {
        scoreMode = collector.scoreMode();
      } else if (scoreMode != collector.scoreMode()) {
        return ScoreMode.COMPLETE;
      }
    }
    return scoreMode;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final List<LeafCollector> leafCollectors = new ArrayList<>(collectors.length);
    ScoreMode leafScoreMode = null;
    for (Collector collector : collectors) {
      final LeafCollector leafCollector;
      try {
        leafCollector = collector.getLeafCollector(context);
      } catch (CollectionTerminatedException e) {
        // this leaf collector does not need this segment
        continue;
      }
      if (leafScoreMode == null) {
        leafScoreMode = collector.scoreMode();
      } else if (leafScoreMode != collector.scoreMode()) {
        leafScoreMode = ScoreMode.COMPLETE;
      }
      leafCollectors.add(leafCollector);
    }
    if (leafCollectors.isEmpty()) {
      throw new CollectionTerminatedException();
    } else {
      // Wraps single leaf collector that wants to skip low-scoring hits (ScoreMode.TOP_SCORES)
      // but the global score mode doesn't allow it.
      if (leafCollectors.size() == 1
          && (scoreMode() == ScoreMode.TOP_SCORES || leafScoreMode != ScoreMode.TOP_SCORES)) {
        return leafCollectors.get(0);
      }
      return new MultiLeafCollector(
          leafCollectors, cacheScores, scoreMode() == ScoreMode.TOP_SCORES);
    }
  }

  private static class MultiLeafCollector implements LeafCollector {

    private final boolean cacheScores;
    private final LeafCollector[] collectors;
    private final float[] minScores;
    private final boolean skipNonCompetitiveScores;

    private MultiLeafCollector(List<LeafCollector> collectors, boolean cacheScores, boolean skipNonCompetitive) {
      this.collectors = collectors.toArray(new LeafCollector[collectors.size()]);
      this.cacheScores = cacheScores;
      this.skipNonCompetitiveScores = skipNonCompetitive;
      this.minScores = this.skipNonCompetitiveScores ? new float[this.collectors.length] : null;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      if (cacheScores) {
        scorer = ScoreCachingWrappingScorer.wrap(scorer);
      }
      if (skipNonCompetitiveScores) {
        for (int i = 0; i < collectors.length; ++i) {
          final LeafCollector c = collectors[i];
          if (c != null) {
            c.setScorer(new MinCompetitiveScoreAwareScorable(scorer,  i,  minScores));
          }
        }
      } else {
        scorer = new FilterScorable(scorer) {
          @Override
          public void setMinCompetitiveScore(float minScore) throws IOException {
            // Ignore calls to setMinCompetitiveScore so that if we wrap two
            // collectors and one of them wants to skip low-scoring hits, then
            // the other collector still sees all hits.
          }

        };
        for (int i = 0; i < collectors.length; ++i) {
          final LeafCollector c = collectors[i];
          if (c != null) {
            c.setScorer(scorer);
          }
        }
      }
    }

    @Override
    public void collect(int doc) throws IOException {
      for (int i = 0; i < collectors.length; i++) {
        final LeafCollector collector = collectors[i];
        if (collector != null) {
          try {
            collector.collect(doc);
          } catch (CollectionTerminatedException e) {
            collectors[i] = null;
            if (allCollectorsTerminated()) {
              throw new CollectionTerminatedException();
            }
          }
        }
      }
    }

    private boolean allCollectorsTerminated() {
      for (int i = 0; i < collectors.length; i++) {
        if (collectors[i] != null) {
          return false;
        }
      }
      return true;
    }

  }

  final static class MinCompetitiveScoreAwareScorable extends FilterScorable {

    private final int idx;
    private final float[] minScores;

    MinCompetitiveScoreAwareScorable(Scorable in, int idx, float[] minScores) {
      super(in);
      this.idx = idx;
      this.minScores = minScores;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      if (minScore > minScores[idx]) {
        minScores[idx] = minScore;
        in.setMinCompetitiveScore(minScore());
      }
    }

    private float minScore() {
      float min = Float.MAX_VALUE;
      for (int i = 0; i < minScores.length; i++) {
        if (minScores[i] < min) {
          min = minScores[i];
        }
      }
      return min;
    }

  }

}
