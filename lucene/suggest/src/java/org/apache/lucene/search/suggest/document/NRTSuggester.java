package org.apache.lucene.search.suggest.document;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.suggest.analyzing.FSTUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PairOutputs.Pair;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

import static org.apache.lucene.search.suggest.document.NRTSuggester.PayLoadProcessor.parseDocID;
import static org.apache.lucene.search.suggest.document.NRTSuggester.PayLoadProcessor.parseSurfaceForm;

/**
 * <p>
 * NRTSuggester returns Top N completions with corresponding documents matching a provided automaton.
 * The completions are returned in descending order of their corresponding weight.
 * Deleted documents are filtered out in near real time using the provided reader.
 * A {@link org.apache.lucene.search.DocIdSet} can be passed in at query time to filter out documents.
 * </p>
 * <p>
 * See {@link #lookup(LeafReader, Automaton, int, DocIdSet, TopSuggestDocsCollector)} for more implementation
 * details.
 * <p>
 * Builder: {@link NRTSuggesterBuilder}
 * </p>
 * <p>
 * FST Format:
 * <ul>
 *   <li>Input: analyzed forms of input terms</li>
 *   <li>Output: Pair&lt;Long, BytesRef&gt; containing weight, surface form and docID</li>
 * </ul>
 * <p>
 * NOTE:
 * <ul>
 *   <li>currently only {@link org.apache.lucene.search.DocIdSet} with random access capabilities are supported.</li>
 *   <li>having too many deletions or using a very restrictive filter can make the search inadmissible due to
 *     over-pruning of potential paths</li>
 *   <li>when a {@link org.apache.lucene.search.DocIdSet} is used, it is assumed that the filter will roughly
 *     filter out half the number of documents that match the provided automaton</li>
 *   <li>lookup performance will degrade as more accepted completions lead to filtered out documents</li>
 * </ul>
 *
 */
final class NRTSuggester implements Accountable {

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
   * Label used to denote the end of an input in the FST and
   * the beginning of dedup bytes
   */
  private final int endByte;

  /**
   * Maximum queue depth for TopNSearcher
   *
   * NOTE: value should be <= Integer.MAX_VALUE
   */
  private static final long MAX_TOP_N_QUEUE_SIZE = 1000;

  private NRTSuggester(FST<Pair<Long, BytesRef>> fst, int maxAnalyzedPathsPerOutput, int payloadSep, int endByte) {
    this.fst = fst;
    this.maxAnalyzedPathsPerOutput = maxAnalyzedPathsPerOutput;
    this.payloadSep = payloadSep;
    this.endByte = endByte;
  }

  @Override
  public long ramBytesUsed() {
    return fst == null ? 0 : fst.ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
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
   * Collects at most Top <code>num</code> completions, filtered by <code>filter</code> on
   * corresponding documents, which has a prefix accepted by <code>automaton</code>
   * <p>
   * Supports near real time deleted document filtering using <code>reader</code>
   * <p>
   * {@link TopSuggestDocsCollector#collect(int, CharSequence, long)} is called
   * for every matched completion
   * <p>
   * Completion collection can be early terminated by throwing {@link org.apache.lucene.search.CollectionTerminatedException}
   */
  public void lookup(final LeafReader reader, final Automaton automaton, final int num, final DocIdSet filter, final TopSuggestDocsCollector collector) {
    final Bits filterDocs;
    try {
      if (filter != null) {
        if (filter.iterator() == null) {
          return;
        }
        if (filter.bits() == null) {
          throw new IllegalArgumentException("DocIDSet does not provide random access interface");
        } else {
          filterDocs = filter.bits();
        }
      } else {
        filterDocs = null;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    int queueSize = getMaxTopNSearcherQueueSize(num, reader, filterDocs != null);
    if (queueSize == -1) {
      return;
    }

    final Bits liveDocs = reader.getLiveDocs();
    try {
      final List<FSTUtil.Path<Pair<Long, BytesRef>>> prefixPaths = FSTUtil.intersectPrefixPaths(automaton, fst);
      Util.TopNSearcher<Pair<Long, BytesRef>> searcher = new Util.TopNSearcher<Pair<Long, BytesRef>>(fst, num, queueSize, getComparator()) {

        private final CharsRefBuilder spare = new CharsRefBuilder();

        @Override
        protected boolean acceptResult(IntsRef input, Pair<Long, BytesRef> output) {
          int payloadSepIndex = parseSurfaceForm(output.output2, payloadSep, spare);
          int docID = parseDocID(output.output2, payloadSepIndex);

          // filter out deleted docs only if no filter is set
          if (filterDocs == null && liveDocs != null && !liveDocs.get(docID)) {
            return false;
          }

          // filter by filter context
          if (filterDocs != null && !filterDocs.get(docID)) {
            return false;
          }

          try {
            collector.collect(docID, spare.toCharsRef(), decode(output.output1));
            return true;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };

      // TODO: add fuzzy support
      for (FSTUtil.Path<Pair<Long, BytesRef>> path : prefixPaths) {
        searcher.addStartPaths(path.fstNode, path.output, false, path.input);
      }

      try {
        // hits are also returned by search()
        // we do not use it, instead collect at acceptResult
        Util.TopResults<Pair<Long, BytesRef>> search = searcher.search();
        // search admissibility is not guaranteed
        // see comment on getMaxTopNSearcherQueueSize
        // assert  search.isComplete;
      } catch (CollectionTerminatedException e) {
        // terminate
      }

    } catch (IOException bogus) {
      throw new RuntimeException(bogus);
    }
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
  private int getMaxTopNSearcherQueueSize(int num, LeafReader reader, boolean filterEnabled) {
    double liveDocsRatio = calculateLiveDocRatio(reader.numDocs(), reader.maxDoc());
    if (liveDocsRatio == -1) {
      return -1;
    }
    long maxQueueSize = num * maxAnalyzedPathsPerOutput;
    // liveDocRatio can be at most 1.0 (if no docs were deleted)
    assert liveDocsRatio <= 1.0d;
    maxQueueSize = (long) (maxQueueSize / liveDocsRatio);
    if (filterEnabled) {
      maxQueueSize = maxQueueSize + (reader.numDocs()/2);
    }
    return (int) Math.min(MAX_TOP_N_QUEUE_SIZE, maxQueueSize);
  }

  private static double calculateLiveDocRatio(int numDocs, int maxDocs) {
    return (numDocs > 0) ? ((double) numDocs / maxDocs) : -1;
  }

  /**
   * Loads a {@link NRTSuggester} from {@link org.apache.lucene.store.IndexInput}
   */
  public static NRTSuggester load(IndexInput input) throws IOException {
    final FST<Pair<Long, BytesRef>> fst = new FST<>(input, new PairOutputs<>(
        PositiveIntOutputs.getSingleton(), ByteSequenceOutputs.getSingleton()));

    /* read some meta info */
    int maxAnalyzedPathsPerOutput = input.readVInt();
    int endByte = input.readVInt();
    int payloadSep = input.readVInt();

    return new NRTSuggester(fst, maxAnalyzedPathsPerOutput, payloadSep, endByte);
  }

  static long encode(long input) {
    if (input < 0) {
      throw new UnsupportedOperationException("cannot encode value: " + input);
    }
    return Long.MAX_VALUE - input;
  }

  static long decode(long output) {
    return (Long.MAX_VALUE - output);
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

    static int parseDocID(final BytesRef output, int payloadSepIndex) {
      assert payloadSepIndex != -1 : "payload sep index can not be -1";
      ByteArrayDataInput input = new ByteArrayDataInput(output.bytes, payloadSepIndex + output.offset + 1, output.length - (payloadSepIndex + output.offset));
      return input.readVInt();
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
