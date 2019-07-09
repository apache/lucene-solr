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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.suggest.analyzing.FSTUtil;
import org.apache.lucene.search.suggest.document.CompletionPostingsFormat.FSTLoadMode;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBufferIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.OffHeapFSTStore;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.search.suggest.document.NRTSuggester.PayLoadProcessor.parseSurfaceForm;

/**
 * <p>
 * NRTSuggester executes Top N search on a weighted FST specified by a {@link CompletionScorer}
 * <p>
 * See {@link #lookup(CompletionScorer, Bits, TopSuggestDocsCollector)} for more implementation
 * details.
 * <p>
 * FST Format:
 * <ul>
 *   <li>Input: analyzed forms of input terms</li>
 *   <li>Output: Pair&lt;Long, BytesRef&gt; containing weight, surface form and docID</li>
 * </ul>
 * <p>
 * NOTE:
 * <ul>
 *   <li>having too many deletions or using a very restrictive filter can make the search inadmissible due to
 *     over-pruning of potential paths. See {@link CompletionScorer#accept(int, Bits)}</li>
 *   <li>when matched documents are arbitrarily filtered ({@link CompletionScorer#filtered} set to <code>true</code>,
 *     it is assumed that the filter will roughly filter out half the number of documents that match
 *     the provided automaton</li>
 *   <li>lookup performance will degrade as more accepted completions lead to filtered out documents</li>
 * </ul>
 *
 * @lucene.experimental
 */
public final class NRTSuggester implements Accountable {

  /**
   * FST<Weight,Surface>:
   * input is the analyzed form, with a null byte between terms
   * and a {@link NRTSuggesterBuilder#END_BYTE} to denote the
   * end of the input
   * weight is a long
   * surface is the original, unanalyzed form followed by the docID
   */
  private final FST<Pair<Long, BytesRef>> fst;

  /**
   * Highest number of analyzed paths we saw for any single
   * input surface form. This can be > 1, when index analyzer
   * creates graphs or if multiple surface form(s) yields the
   * same analyzed form
   */
  private final int maxAnalyzedPathsPerOutput;

  /**
   * Separator used between surface form and its docID in the FST output
   */
  private final int payloadSep;

  /**
   * Maximum queue depth for TopNSearcher
   *
   * NOTE: value should be <= Integer.MAX_VALUE
   */
  private static final long MAX_TOP_N_QUEUE_SIZE = 5000;

  private NRTSuggester(FST<Pair<Long, BytesRef>> fst, int maxAnalyzedPathsPerOutput, int payloadSep) {
    this.fst = fst;
    this.maxAnalyzedPathsPerOutput = maxAnalyzedPathsPerOutput;
    this.payloadSep = payloadSep;
  }

  @Override
  public long ramBytesUsed() {
    return fst == null ? 0 : fst.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  /**
   * Collects at most {@link TopSuggestDocsCollector#getCountToCollect()} completions that
   * match the provided {@link CompletionScorer}.
   * <p>
   * The {@link CompletionScorer#automaton} is intersected with the {@link #fst}.
   * {@link CompletionScorer#weight} is used to compute boosts and/or extract context
   * for each matched partial paths. A top N search is executed on {@link #fst} seeded with
   * the matched partial paths. Upon reaching a completed path, {@link CompletionScorer#accept(int, Bits)}
   * and {@link CompletionScorer#score(float, float)} is used on the document id, index weight
   * and query boost to filter and score the entry, before being collected via
   * {@link TopSuggestDocsCollector#collect(int, CharSequence, CharSequence, float)}
   */
  public void lookup(final CompletionScorer scorer, Bits acceptDocs, final TopSuggestDocsCollector collector) throws IOException {
    final double liveDocsRatio = calculateLiveDocRatio(scorer.reader.numDocs(), scorer.reader.maxDoc());
    if (liveDocsRatio == -1) {
      return;
    }
    final List<FSTUtil.Path<Pair<Long, BytesRef>>> prefixPaths = FSTUtil.intersectPrefixPaths(scorer.automaton, fst);
    // The topN is increased by a factor of # of intersected path
    // to ensure search admissibility. For example, one suggestion can
    // have multiple contexts, resulting in num_context paths for the
    // suggestion instead of 1 in the FST. When queried for the suggestion,
    // the topN value ensures that all paths to the suggestion are evaluated
    // (in case of a match all context query).
    // Note that collectors will early terminate as soon as enough suggestions
    // have been collected, regardless of the set topN value. This value is the
    // maximum number of suggestions that can be collected.
    final int topN = collector.getCountToCollect() * prefixPaths.size();
    final int queueSize = getMaxTopNSearcherQueueSize(topN, scorer.reader.numDocs(), liveDocsRatio, scorer.filtered);

    final CharsRefBuilder spare = new CharsRefBuilder();

    Comparator<Pair<Long, BytesRef>> comparator = getComparator();
    Util.TopNSearcher<Pair<Long, BytesRef>> searcher = new Util.TopNSearcher<Pair<Long, BytesRef>>(fst, topN, queueSize, comparator,
        new ScoringPathComparator(scorer)) {

      private final ByteArrayDataInput scratchInput = new ByteArrayDataInput();

      @Override
      protected boolean acceptPartialPath(Util.FSTPath<Pair<Long,BytesRef>> path) {
        if (collector.doSkipDuplicates()) {
          // We are removing dups
          if (path.payload == -1) {
            // This path didn't yet see the complete surface form; let's see if it just did with the arc output we just added:
            BytesRef arcOutput = path.arc.output.output2;
            BytesRef output = path.output.output2;
            for(int i=0;i<arcOutput.length;i++) {
              if (arcOutput.bytes[arcOutput.offset + i] == payloadSep) {
                // OK this arc that the path was just extended by contains the payloadSep, so we now have a full surface form in this path
                path.payload = output.length - arcOutput.length + i;
                assert output.bytes[output.offset + path.payload] == payloadSep;
                break;
              }
            }
          }

          if (path.payload != -1) {
            BytesRef output = path.output.output2;
            spare.copyUTF8Bytes(output.bytes, output.offset, path.payload);
            if (collector.seenSurfaceForms.contains(spare.chars(), 0, spare.length())) {
              return false;
            }
          }
        }
        return true;
      }

      @Override
      protected boolean acceptResult(Util.FSTPath<Pair<Long, BytesRef>> path) {
        BytesRef output = path.output.output2;
        int payloadSepIndex;
        if (path.payload != -1) {
          payloadSepIndex = path.payload;
          spare.copyUTF8Bytes(output.bytes, output.offset, payloadSepIndex);
        } else {
          assert collector.doSkipDuplicates() == false;
          payloadSepIndex = parseSurfaceForm(output, payloadSep, spare);
        }

        scratchInput.reset(output.bytes, output.offset + payloadSepIndex + 1, output.length - payloadSepIndex - 1);
        int docID = scratchInput.readVInt();
        
        if (!scorer.accept(docID, acceptDocs)) {
          return false;
        }
        if (collector.doSkipDuplicates()) {
          // now record that we've seen this surface form:
          char[] key = new char[spare.length()];
          System.arraycopy(spare.chars(), 0, key, 0, spare.length());
          if (collector.seenSurfaceForms.contains(key)) {
            // we already collected a higher scoring document with this key, in this segment:
            return false;
          }
          collector.seenSurfaceForms.add(key);
        }
        try {
          float score = scorer.score(decode(path.output.output1), path.boost);
          collector.collect(docID, spare.toCharsRef(), path.context, score);
          return true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };

    for (FSTUtil.Path<Pair<Long, BytesRef>> path : prefixPaths) {
      scorer.weight.setNextMatch(path.input.get());
      BytesRef output = path.output.output2;
      int payload = -1;
      if (collector.doSkipDuplicates()) {
        for(int j=0;j<output.length;j++) {
          if (output.bytes[output.offset+j] == payloadSep) {
            // Important to cache this, else we have a possibly O(N^2) cost where N is the length of suggestions
            payload = j;
            break;
          }
        }
      }
      
      searcher.addStartPaths(path.fstNode, path.output, false, path.input, scorer.weight.boost(),
                             scorer.weight.context(), payload);
    }
    // hits are also returned by search()
    // we do not use it, instead collect at acceptResult
    searcher.search();
    // search admissibility is not guaranteed
    // see comment on getMaxTopNSearcherQueueSize
    // assert  search.isComplete;
  }

  /**
   * Compares partial completion paths using {@link CompletionScorer#score(float, float)},
   * breaks ties comparing path inputs
   */
  private static class ScoringPathComparator implements Comparator<Util.FSTPath<Pair<Long, BytesRef>>> {
    private final CompletionScorer scorer;

    public ScoringPathComparator(CompletionScorer scorer) {
      this.scorer = scorer;
    }

    @Override
    public int compare(Util.FSTPath<Pair<Long, BytesRef>> first, Util.FSTPath<Pair<Long, BytesRef>> second) {
      int cmp = Float.compare(scorer.score(decode(second.output.output1), second.boost),
          scorer.score(decode(first.output.output1), first.boost));
      return (cmp != 0) ? cmp : first.input.get().compareTo(second.input.get());
    }
  }

  private static Comparator<Pair<Long, BytesRef>> getComparator() {
    return new Comparator<Pair<Long, BytesRef>>() {
      @Override
      public int compare(Pair<Long, BytesRef> o1, Pair<Long, BytesRef> o2) {
        return Long.compare(o1.output1, o2.output1);
      }
    };
  }

  /**
   * Simple heuristics to try to avoid over-pruning potential suggestions by the
   * TopNSearcher. Since suggestion entries can be rejected if they belong
   * to a deleted document, the length of the TopNSearcher queue has to
   * be increased by some factor, to account for the filtered out suggestions.
   * This heuristic will try to make the searcher admissible, but the search
   * can still lead to over-pruning
   * <p>
   * If a <code>filter</code> is applied, the queue size is increased by
   * half the number of live documents.
   * <p>
   * The maximum queue size is {@link #MAX_TOP_N_QUEUE_SIZE}
   */
  private int getMaxTopNSearcherQueueSize(int topN, int numDocs, double liveDocsRatio, boolean filterEnabled) {
    long maxQueueSize = topN * maxAnalyzedPathsPerOutput;
    // liveDocRatio can be at most 1.0 (if no docs were deleted)
    assert liveDocsRatio <= 1.0d;
    maxQueueSize = (long) (maxQueueSize / liveDocsRatio);
    if (filterEnabled) {
      maxQueueSize = maxQueueSize + (numDocs/2);
    }
    return (int) Math.min(MAX_TOP_N_QUEUE_SIZE, maxQueueSize);
  }

  private static double calculateLiveDocRatio(int numDocs, int maxDocs) {
    return (numDocs > 0) ? ((double) numDocs / maxDocs) : -1;
  }

  private static boolean shouldLoadFSTOffHeap(IndexInput input, FSTLoadMode fstLoadMode) {
    switch (fstLoadMode) {
      case ON_HEAP:
        return false;
      case OFF_HEAP:
        return true;
      case AUTO:
        return input instanceof ByteBufferIndexInput;
      default:
        throw new IllegalStateException("unknown enum constant: " + fstLoadMode);
    }
  }

  /**
   * Loads a {@link NRTSuggester} from {@link org.apache.lucene.store.IndexInput} on or off-heap
   * depending on the provided <code>fstLoadMode</code>
   */
  public static NRTSuggester load(IndexInput input, FSTLoadMode fstLoadMode) throws IOException {
    final FST<Pair<Long, BytesRef>> fst;
    if (shouldLoadFSTOffHeap(input, fstLoadMode)) {
      OffHeapFSTStore store = new OffHeapFSTStore();
      IndexInput clone = input.clone();
      clone.seek(input.getFilePointer());
      fst = new FST<>(clone, new PairOutputs<>(
          PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()), store);
      input.seek(clone.getFilePointer() + store.size());
    } else {
      fst = new FST<>(input, new PairOutputs<>(
          PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()));
    }

    /* read some meta info */
    int maxAnalyzedPathsPerOutput = input.readVInt();
    /*
     * Label used to denote the end of an input in the FST and
     * the beginning of dedup bytes
     */
    int endByte = input.readVInt();
    int payloadSep = input.readVInt();
    return new NRTSuggester(fst, maxAnalyzedPathsPerOutput, payloadSep);
  }

  static long encode(long input) {
    if (input < 0 || input > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + input);
    }
    return Integer.MAX_VALUE - input;
  }

  static long decode(long output) {
    assert output >= 0 && output <= Integer.MAX_VALUE :
        "decoded output: " + output + " is not within 0 and Integer.MAX_VALUE";
    return Integer.MAX_VALUE - output;
  }

  /**
   * Helper to encode/decode payload (surface + PAYLOAD_SEP + docID) output
   */
  static final class PayLoadProcessor {
    final static private int MAX_DOC_ID_LEN_WITH_SEP = 6; // vint takes at most 5 bytes

    static int parseSurfaceForm(final BytesRef output, int payloadSep, CharsRefBuilder spare) {
      int surfaceFormLen = -1;
      for (int i = 0; i < output.length; i++) {
        if (output.bytes[output.offset + i] == payloadSep) {
          surfaceFormLen = i;
          break;
        }
      }
      assert surfaceFormLen != -1 : "no payloadSep found, unable to determine surface form";
      spare.copyUTF8Bytes(output.bytes, output.offset, surfaceFormLen);
      return surfaceFormLen;
    }

    static BytesRef make(final BytesRef surface, int docID, int payloadSep) throws IOException {
      int len = surface.length + MAX_DOC_ID_LEN_WITH_SEP;
      byte[] buffer = new byte[len];
      ByteArrayDataOutput output = new ByteArrayDataOutput(buffer);
      output.writeBytes(surface.bytes, surface.length - surface.offset);
      output.writeByte((byte) payloadSep);
      output.writeVInt(docID);
      return new BytesRef(buffer, 0, output.getPosition());
    }
  }
}
