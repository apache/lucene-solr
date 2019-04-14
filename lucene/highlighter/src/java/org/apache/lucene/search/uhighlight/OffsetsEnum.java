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

package org.apache.lucene.search.uhighlight;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.function.Supplier;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;

/**
 * An enumeration/iterator of a term and its offsets for use by {@link FieldHighlighter}.
 * It is advanced and is placed in a priority queue by
 * {@link FieldHighlighter#highlightOffsetsEnums(OffsetsEnum)} based on the start offset.
 *
 * @lucene.internal
 */
public abstract class OffsetsEnum implements Comparable<OffsetsEnum>, Closeable {

  // note: the ordering clearly changes as the postings enum advances
  // note: would be neat to use some Comparator utilities with method
  //  references but our methods throw IOException
  @Override
  public int compareTo(OffsetsEnum other) {
    try {
      int cmp = Integer.compare(startOffset(), other.startOffset());
      if (cmp != 0) {
        return cmp; // vast majority of the time we return here.
      }
      cmp = Integer.compare(endOffset(), other.endOffset());
      if (cmp != 0) {
        return cmp;
      }
      final BytesRef thisTerm = this.getTerm();
      final BytesRef otherTerm = other.getTerm();
      if (thisTerm == null || otherTerm == null) {
        if (thisTerm == null && otherTerm == null) {
          return 0;
        } else if (thisTerm == null) {
          return 1; // put "this" (wildcard mtq enum) last
        } else {
          return -1;
        }
      }
      return thisTerm.compareTo(otherTerm);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Advances to the next position and returns true, or if can't then returns false.
   * Note that the initial state of this class is not positioned.
   */
  public abstract boolean nextPosition() throws IOException;

  /** An estimate of the number of occurrences of this term/OffsetsEnum. */
  public abstract int freq() throws IOException;

  /**
   * The term at this position.
   * This BytesRef is safe to continue to refer to, even after we move to the next position.
   *
   * @see Passage#getMatchTerms()
   */
  public abstract BytesRef getTerm() throws IOException;

  public abstract int startOffset() throws IOException;

  public abstract int endOffset() throws IOException;

  @Override
  public void close() throws IOException {
  }

  @Override
  public String toString() {
    final String name = getClass().getSimpleName();
    String offset = "";
    try {
      offset = ",[" + startOffset() + "-" + endOffset() + "]";
    } catch (Exception e) {
      //ignore; for debugging only
    }
    try {
      return name + "(term:" + getTerm().utf8ToString() + offset + ")";
    } catch (Exception e) {
      return name;
    }
  }

  /**
   * Based on a {@link PostingsEnum} -- the typical/standard OE impl.
   */
  public static class OfPostings extends OffsetsEnum {
    private final BytesRef term;
    private final PostingsEnum postingsEnum; // with offsets
    private final int freq;

    private int posCounter = -1;

    public OfPostings(BytesRef term, int freq, PostingsEnum postingsEnum) throws IOException {
      this.term = Objects.requireNonNull(term);
      this.postingsEnum = Objects.requireNonNull(postingsEnum);
      this.freq = freq;
      this.posCounter = this.postingsEnum.freq();
    }

    public OfPostings(BytesRef term, PostingsEnum postingsEnum) throws IOException {
      this(term, postingsEnum.freq(), postingsEnum);
    }

    public PostingsEnum getPostingsEnum() {
      return postingsEnum;
    }

    @Override
    public boolean nextPosition() throws IOException {
      if (posCounter > 0) {
        posCounter--;
        postingsEnum.nextPosition(); // note: we don't need to save the position
        return true;
      } else {
        return false;
      }
    }

    @Override
    public BytesRef getTerm() throws IOException {
      return term;
    }

    @Override
    public int startOffset() throws IOException {
      return postingsEnum.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return postingsEnum.endOffset();
    }

    @Override
    public int freq() throws IOException {
      return freq;
    }
  }

  /** Based on a {@link MatchesIterator} with submatches. */
  public static class OfMatchesIteratorWithSubs extends OffsetsEnum {
    //Either CachedOE impls (which are the submatches) or OfMatchesIterator impls
    private final PriorityQueue<OffsetsEnum> pendingQueue = new PriorityQueue<>();
    private final HashMap<Query,BytesRef> queryToTermMap = new HashMap<>();

    public OfMatchesIteratorWithSubs(MatchesIterator matchesIterator) {
      pendingQueue.add(new OfMatchesIterator(matchesIterator, () -> queryToTerm(matchesIterator.getQuery())));
    }

    @Override
    public boolean nextPosition() throws IOException {
      OffsetsEnum formerHeadOE = pendingQueue.poll(); // removes the head
      if (formerHeadOE instanceof CachedOE) {
        // we're done with the former head.  CachedOE's are one use only.
        // Look at the new head...
        OffsetsEnum newHeadOE = pendingQueue.peek();
        if (newHeadOE instanceof OfMatchesIterator) {
          // We found the matchesIterator.  Requires processing.
          nextWhenMatchesIterator((OfMatchesIterator) newHeadOE);  // May or may not remove or re-queue itself
        } // else new head is a CacheOE or no more.  Nothing to do with it.

      } else { // formerHeadOE is OfMatchesIterator; advance it
        OfMatchesIterator miOE = (OfMatchesIterator) formerHeadOE;
        if (miOE.nextPosition()) {
          nextWhenMatchesIterator(miOE); // requires processing.  May or may not re-enqueue itself
        }
      }
      return pendingQueue.isEmpty() == false;
    }

    private void nextWhenMatchesIterator(OfMatchesIterator miOE) throws IOException {
      boolean isHead = miOE == pendingQueue.peek();
      MatchesIterator subMatches = miOE.matchesIterator.getSubMatches();
      if (subMatches != null) {
        // remove this miOE from the queue, add it's submatches, next() it, then re-enqueue it
        if (isHead) {
          pendingQueue.poll(); // remove
        }

        enqueueCachedMatches(subMatches);

        if (miOE.nextPosition()) {
          pendingQueue.add(miOE);
          assert pendingQueue.peek() != miOE; // miOE should follow cached entries
        }

      } else { // else has no subMatches.  It will stay enqueued.
        if (!isHead) {
          pendingQueue.add(miOE);
        } // else it's *already* in pendingQueue
      }
    }

    private boolean enqueueCachedMatches(MatchesIterator thisMI) throws IOException {
      if (thisMI == null) {
        return false;
      } else {
        while (thisMI.next()) {
          if (false == enqueueCachedMatches(thisMI.getSubMatches())) { // recursion
            // if no sub-matches then add ourselves
            pendingQueue.add(new CachedOE(queryToTerm(thisMI.getQuery()), thisMI.startOffset(), thisMI.endOffset()));
          }
        }
        return true;
      }
    }

    /** Maps a Query from {@link MatchesIterator#getQuery()} to {@link OffsetsEnum#getTerm()}.
     * See {@link Passage#getMatchTerms()}. */
    private BytesRef queryToTerm(Query query) {
      // compute an approximate BytesRef term of a Query.  We cache this since we're likely to see the same query again.
      // Our approach is to visit each matching term in order, concatenating them with an adjoining space.
      //  If we don't have any (perhaps due to an MTQ like a wildcard) then we fall back on the toString() of the query.
      return queryToTermMap.computeIfAbsent(query, (Query q) -> {
        BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
        q.visit(new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            for (Term term : terms) {
              if (bytesRefBuilder.length() > 0) {
                bytesRefBuilder.append((byte) ' ');
              }
              bytesRefBuilder.append(term.bytes());
            }
          }
        });
        if (bytesRefBuilder.length() > 0) {
          return bytesRefBuilder.get();
        }
        // fallback:  (likely a MultiTermQuery)
        return new BytesRef(q.toString());
      });
    }

    @Override
    public int freq() throws IOException {
      return pendingQueue.peek().freq();
    }

    @Override
    public BytesRef getTerm() throws IOException {
      return pendingQueue.peek().getTerm();
    }

    @Override
    public int startOffset() throws IOException {
      return pendingQueue.peek().startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return pendingQueue.peek().endOffset();
    }

    private static class CachedOE extends OffsetsEnum {
      final BytesRef term;
      final int startOffset;
      final int endOffset;

      private CachedOE(BytesRef term, int startOffset, int endOffset) {
        this.term = term;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
      }

      @Override
      public boolean nextPosition() throws IOException {
        return false;
      }

      @Override
      public int freq() throws IOException {
        return 1; // documented short-coming of MatchesIterator based UnifiedHighlighter
      }

      @Override
      public BytesRef getTerm() throws IOException {
        return term;
      }

      @Override
      public int startOffset() throws IOException {
        return startOffset;
      }

      @Override
      public int endOffset() throws IOException {
        return endOffset;
      }
    }
  }

  /** Based on a {@link MatchesIterator}; does not look at submatches. */
  public static class OfMatchesIterator extends OffsetsEnum {
    private final MatchesIterator matchesIterator;
    private final Supplier<BytesRef> termSupplier;

    public OfMatchesIterator(MatchesIterator matchesIterator, Supplier<BytesRef> termSupplier) {
      this.matchesIterator = matchesIterator;
      this.termSupplier = termSupplier;
    }

    @Override
    public boolean nextPosition() throws IOException {
      return matchesIterator.next();
    }

    @Override
    public int freq() throws IOException {
      return 1; // documented short-coming of MatchesIterator based UnifiedHighlighter
    }

    @Override
    public BytesRef getTerm() throws IOException {
      return termSupplier.get();
    }

    @Override
    public int startOffset() throws IOException {
      return matchesIterator.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return matchesIterator.endOffset();
    }
  }

  /**
   * Empty enumeration
   */
  public static final OffsetsEnum EMPTY = new OffsetsEnum() {
    @Override
    public boolean nextPosition() throws IOException {
      return false;
    }

    @Override
    public BytesRef getTerm() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int startOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int endOffset() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int freq() throws IOException {
      return 0;
    }

  };

  /**
   * A view over several OffsetsEnum instances, merging them in-place
   */
  //If OffsetsEnum and MatchesIterator ever truly merge then this could go away in lieu of DisjunctionMatchesIterator
  public static class MultiOffsetsEnum extends OffsetsEnum {

    private final PriorityQueue<OffsetsEnum> queue;
    private boolean started = false;

    public MultiOffsetsEnum(List<OffsetsEnum> inner) throws IOException {
      this.queue = new PriorityQueue<>();
      for (OffsetsEnum oe : inner) {
        if (oe.nextPosition())
          this.queue.add(oe);
      }
    }

    @Override
    public boolean nextPosition() throws IOException {
      if (started == false) {
        started = true;
        return this.queue.size() > 0;
      }
      if (this.queue.size() > 0) {
        OffsetsEnum top = this.queue.poll();
        if (top.nextPosition()) {
          this.queue.add(top);
          return true;
        }
        else {
          top.close();
        }
        return this.queue.size() > 0;
      }
      return false;
    }

    @Override
    public BytesRef getTerm() throws IOException {
      return this.queue.peek().getTerm();
    }

    @Override
    public int startOffset() throws IOException {
      return this.queue.peek().startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return this.queue.peek().endOffset();
    }

    @Override
    public int freq() throws IOException {
      return this.queue.peek().freq();
    }

    @Override
    public void close() throws IOException {
      // most child enums will have been closed in .nextPosition()
      // here all remaining non-exhausted enums are closed
      IOUtils.close(queue);
    }
  }
}
