package org.apache.lucene.util;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link DocIdSet} implementation based on word-aligned hybrid encoding on
 * words of 8 bits.
 * <p>This implementation doesn't support random-access but has a fast
 * {@link DocIdSetIterator} which can advance in logarithmic time thanks to
 * an index.</p>
 * <p>The compression scheme is simplistic and should work well with sparse doc
 * id sets while being only slightly larger than a {@link FixedBitSet} for
 * incompressible sets (overhead&lt;2% in the worst case) in spite of the index.</p>
 * <p><b>Format</b>: The format is byte-aligned. An 8-bits word is either clean,
 * meaning composed only of zeros, or dirty, meaning that it contains at least one
 * bit set. The idea is to encode sequences of clean words using run-length
 * encoding and to leave sequences of dirty words as-is.</p>
 * <table>
 *   <tr><th>Token</th><th>Clean length+</th><th>Dirty length+</th><th>Dirty words</th></tr>
 *   <tr><td>1 byte</td><td>0-n bytes</td><td>0-n bytes</td><td>0-n bytes</td></tr>
 * </table>
 * <ul>
 *   <li><b>Token</b> encodes the number of clean words minus 2 on the first 4
 * bits and the number of dirty words minus 1 on the last 4 bits. The
 * higher-order bit is a continuation bit, meaning that the number is incomplete
 * and needs additional bytes to be read.</li>
 *   <li><b>Clean length+</b>: If clean length has its higher-order bit set,
 * you need to read a {@link DataInput#readVInt() vint}, shift it by 3 bits on
 * the left side and add it to the 3 bits which have been read in the token.</li>
 *   <li><b>Dirty length+</b> works the same way as <b>Clean length+</b> but
 * for the length of dirty words.</li>
 *   <li><b>Dirty words</b> are the dirty words, there are <b>Dirty length</b>
 * of them.</li>
 * </ul>
 * <p>This format cannot encode sequences of less than 2 clean words and 1 dirty
 * word. The reason is that if you find a single clean word, you should rather
 * encode it as a dirty word. This takes the same space as starting a new
 * sequence (since you need one byte for the token) but will be lighter to
 * decode. There is however an exception for the first sequence. Since the first
 * sequence may start directly with a dirty word, the clean length is encoded
 * directly, without subtracting 2.</p>
 * <p>There is an additional restriction on the format: the sequence of dirty
 * words must start and end with a non-null word and is not allowed to contain
 * two consecutive null words. This restriction exists to make sure no space is
 * wasted and to make sure iterators can read the next doc ID by reading at most
 * 2 dirty words.</p>
 * @lucene.experimental
 */
public final class WAH8DocIdSet extends DocIdSet {

  // Minimum index interval, intervals below this value can't guarantee anymore
  // that this set implementation won't be significantly larger than a FixedBitSet
  // The reason is that a single sequence saves at least one byte and an index
  // entry requires at most 8 bytes (2 ints) so there shouldn't be more than one
  // index entry every 8 sequences
  private static final int MIN_INDEX_INTERVAL = 8;

  /** Default index interval. */
  // To compute this default value, I created a rather dense set (0.1% load
  // factor, which is close to the worst case regarding both compression and
  // speed for this DocIdSet impl since sequences are going to be short) and I
  // started with interval=1 and doubled it at each iteration until advance
  // became slower
  public static final int DEFAULT_INDEX_INTERVAL = 16;

  private static final PackedInts.Reader EMPTY_READER = new PackedInts.NullReader(1);
  private static WAH8DocIdSet EMPTY = new WAH8DocIdSet(new byte[0], EMPTY_READER, EMPTY_READER);

  private static final Comparator<Iterator> SERIALIZED_LENGTH_COMPARATOR = new Comparator<Iterator>() {
    @Override
    public int compare(Iterator wi1, Iterator wi2) {
      return wi1.in.length() - wi2.in.length();
    }
  };

  /** Same as {@link #copyOf(DocIdSetIterator, int)} with the default index interval. */
  public static WAH8DocIdSet copyOf(DocIdSetIterator it) throws IOException {
    return copyOf(it, DEFAULT_INDEX_INTERVAL);
  }

  /** Return a copy of the provided iterator. */
  public static WAH8DocIdSet copyOf(DocIdSetIterator it, int indexInterval) throws IOException {
    Builder builder = new Builder().setIndexInterval(indexInterval);
    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
      builder.add(doc);
    }
    return builder.build();
  }

  /** Same as {@link #intersect(Collection, int)} with the default index interval. */
  public static WAH8DocIdSet intersect(Collection<WAH8DocIdSet> docIdSets) {
    return intersect(docIdSets, DEFAULT_INDEX_INTERVAL);
  }

  /**
   * Compute the intersection of the provided sets. This method is much faster than
   * computing the intersection manually since it operates directly at the byte level.
   */
  public static WAH8DocIdSet intersect(Collection<WAH8DocIdSet> docIdSets, int indexInterval) {
    switch (docIdSets.size()) {
      case 0:
        throw new IllegalArgumentException("There must be at least one set to intersect");
      case 1:
        return docIdSets.iterator().next();
    }
    // The logic below is similar to ConjunctionScorer
    final int numSets = docIdSets.size();
    final Iterator[] iterators = new Iterator[numSets];
    int i = 0;
    for (WAH8DocIdSet set : docIdSets) {
      final Iterator it = set.iterator();
      iterators[i++] = it;
    }
    Arrays.sort(iterators, SERIALIZED_LENGTH_COMPARATOR);
    final WordBuilder builder = new WordBuilder().setIndexInterval(indexInterval);
    int wordNum = 0;
    main:
    while (true) {
      // Advance the least costly iterator first
      iterators[0].advanceWord(wordNum);
      wordNum = iterators[0].wordNum;
      if (wordNum == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      byte word = iterators[0].word;
      for (i = 1; i < numSets; ++i) {
        if (iterators[i].wordNum < wordNum) {
          iterators[i].advanceWord(wordNum);
        }
        if (iterators[i].wordNum > wordNum) {
          wordNum = iterators[i].wordNum;
          continue main;
        }
        assert iterators[i].wordNum == wordNum;
        word &= iterators[i].word;
        if (word == 0) {
          // There are common words, but they don't share any bit
          ++wordNum;
          continue main;
        }
      }
      // Found a common word
      assert word != 0;
      builder.addWord(wordNum, word);
      ++wordNum;
    }
    return builder.build();
  }

  /** Same as {@link #union(Collection, int)} with the default index interval. */
  public static WAH8DocIdSet union(Collection<WAH8DocIdSet> docIdSets) {
    return union(docIdSets, DEFAULT_INDEX_INTERVAL);
  }

  /**
   * Compute the union of the provided sets. This method is much faster than
   * computing the union manually since it operates directly at the byte level.
   */
  public static WAH8DocIdSet union(Collection<WAH8DocIdSet> docIdSets, int indexInterval) {
    switch (docIdSets.size()) {
      case 0:
        return EMPTY;
      case 1:
        return docIdSets.iterator().next();
    }
    // The logic below is very similar to DisjunctionScorer
    final int numSets = docIdSets.size();
    final PriorityQueue<Iterator> iterators = new PriorityQueue<WAH8DocIdSet.Iterator>(numSets) {
      @Override
      protected boolean lessThan(Iterator a, Iterator b) {
        return a.wordNum < b.wordNum;
      }
    };
    for (WAH8DocIdSet set : docIdSets) {
      Iterator iterator = set.iterator();
      iterator.nextWord();
      iterators.add(iterator);
    }

    Iterator top = iterators.top();
    if (top.wordNum == Integer.MAX_VALUE) {
      return EMPTY;
    }
    int wordNum = top.wordNum;
    byte word = top.word;
    final WordBuilder builder = new WordBuilder().setIndexInterval(indexInterval);
    while (true) {
      top.nextWord();
      iterators.updateTop();
      top = iterators.top();
      if (top.wordNum == wordNum) {
        word |= top.word;
      } else {
        builder.addWord(wordNum, word);
        if (top.wordNum == Integer.MAX_VALUE) {
          break;
        }
        wordNum = top.wordNum;
        word = top.word;
      }
    }
    return builder.build();
  }

  static int wordNum(int docID) {
    assert docID >= 0;
    return docID >>> 3;
  }

  /** Word-based builder. */
  static class WordBuilder {

    final GrowableByteArrayDataOutput out;
    final GrowableByteArrayDataOutput dirtyWords;
    int clean;
    int lastWordNum;
    int numSequences;
    int indexInterval;

    WordBuilder() {
      out = new GrowableByteArrayDataOutput(1024);
      dirtyWords = new GrowableByteArrayDataOutput(128);
      clean = 0;
      lastWordNum = -1;
      numSequences = 0;
      indexInterval = DEFAULT_INDEX_INTERVAL;
    }

    /** Set the index interval. Smaller index intervals improve performance of
     *  {@link DocIdSetIterator#advance(int)} but make the {@link DocIdSet}
     *  larger. An index interval <code>i</code> makes the index add an overhead
     *  which is at most <code>4/i</code>, but likely much less.The default index
     *  interval is <code>16</code>, meaning the index has an overhead of at most
     *  25%. To disable indexing, you can pass {@link Integer#MAX_VALUE} as an
     *  index interval. */
    public WordBuilder setIndexInterval(int indexInterval) {
      if (indexInterval < MIN_INDEX_INTERVAL) {
        throw new IllegalArgumentException("indexInterval must be >= " + MIN_INDEX_INTERVAL);
      }
      this.indexInterval = indexInterval;
      return this;
    }

    void writeHeader(int cleanLength) throws IOException {
      final int cleanLengthMinus2 = cleanLength - 2;
      final int dirtyLengthMinus1 = dirtyWords.length - 1;
      assert cleanLengthMinus2 >= 0;
      assert dirtyLengthMinus1 >= 0;
      int token = ((cleanLengthMinus2 & 0x07) << 4) | (dirtyLengthMinus1 & 0x07);
      if (cleanLengthMinus2 > 0x07) {
        token |= 1 << 7;
      }
      if (dirtyLengthMinus1 > 0x07) {
        token |= 1 << 3;
      }
      out.writeByte((byte) token);
      if (cleanLengthMinus2 > 0x07) {
        out.writeVInt(cleanLengthMinus2 >>> 3);
      }
      if (dirtyLengthMinus1 > 0x07) {
        out.writeVInt(dirtyLengthMinus1 >>> 3);
      }
    }

    void writeSequence(int cleanLength) {
      try {
        writeHeader(cleanLength);
        out.writeBytes(dirtyWords.bytes, dirtyWords.length);
      } catch (IOException cannotHappen) {
        throw new AssertionError(cannotHappen);
      }
      dirtyWords.length = 0;
      ++numSequences;
    }

    void addWord(int wordNum, byte word) {
      assert wordNum > lastWordNum;
      assert word != 0;

      if (lastWordNum == -1) {
        clean = 2 + wordNum; // special case for the 1st sequence
        dirtyWords.writeByte(word);
      } else {
        switch (wordNum - lastWordNum) {
          case 1:
            dirtyWords.writeByte(word);
            break;
          case 2:
            dirtyWords.writeByte((byte) 0);
            dirtyWords.writeByte(word);
            break;
          default:
            writeSequence(clean);
            clean = wordNum - lastWordNum - 1;
            dirtyWords.writeByte(word);
        }
      }
      lastWordNum = wordNum;
    }

    /** Build a new {@link WAH8DocIdSet}. */
    public WAH8DocIdSet build() {
      if (lastWordNum == -1) {
        return EMPTY;
      }
      writeSequence(clean);
      final byte[] data = Arrays.copyOf(out.bytes, out.length);

      // Now build the index
      final int valueCount = (numSequences - 1) / indexInterval + 1;
      final PackedInts.Reader indexPositions;
      final PackedInts.Reader indexWordNums;
      if (valueCount <= 1) {
        indexPositions = indexWordNums = EMPTY_READER;
      } else {
        // From the tests I ran, there is no need to expose acceptableOverheadRatio, these packed ints are never the bottleneck
        final PackedInts.Mutable positions = PackedInts.getMutable(valueCount, PackedInts.bitsRequired(data.length - 1), PackedInts.COMPACT);
        final PackedInts.Mutable wordNums = PackedInts.getMutable(valueCount, PackedInts.bitsRequired(lastWordNum), PackedInts.COMPACT);
  
        final Iterator it = new Iterator(data, null, null);
        assert it.in.getPosition() == 0;
        assert it.wordNum == -1;
        for (int i = 1; i < valueCount; ++i) {
          // skip indexInterval sequences
          for (int j = 0; j < indexInterval; ++j) {
            final boolean readSequence = it.readSequence();
            assert readSequence;
            it.skipDirtyBytes();
          }
          final int position = it.in.getPosition();
          final int wordNum = it.wordNum;
          positions.set(i, position);
          wordNums.set(i, wordNum + 1);
        }
        indexPositions = positions;
        indexWordNums = wordNums;
      }

      return new WAH8DocIdSet(data, indexPositions, indexWordNums);
    }

  }

  /** A builder for {@link WAH8DocIdSet}s. */
  public static final class Builder extends WordBuilder {

    private int lastDocID;
    private int wordNum, word;

    /** Sole constructor */
    public Builder() {
      super();
      lastDocID = -1;
      wordNum = -1;
      word = 0;
    }

    /** Add a document to this builder. Documents must be added in order. */
    public Builder add(int docID) {
      if (docID <= lastDocID) {
        throw new IllegalArgumentException("Doc ids must be added in-order, got " + docID + " which is <= lastDocID=" + lastDocID);
      }
      final int wordNum = wordNum(docID);
      if (this.wordNum == -1) {
        this.wordNum = wordNum;
        word = 1 << (docID & 0x07);
      } else if (wordNum == this.wordNum) {
        word |= 1 << (docID & 0x07);
      } else {
        addWord(this.wordNum, (byte) word);
        this.wordNum = wordNum;
        word = 1 << (docID & 0x07);
      }
      lastDocID = docID;
      return this;
    }

    @Override
    public Builder setIndexInterval(int indexInterval) {
      return (Builder) super.setIndexInterval(indexInterval);
    }

    @Override
    public WAH8DocIdSet build() {
      if (this.wordNum != -1) {
        addWord(wordNum, (byte) word);
      }
      return super.build();
    }

  }

  // where the doc IDs are stored
  private final byte[] data;
  // index for advance(int)
  private final PackedInts.Reader positions, wordNums; // wordNums[i] starts at the sequence at positions[i]

  WAH8DocIdSet(byte[] data, PackedInts.Reader positions, PackedInts.Reader wordNums) {
    this.data = data;
    this.positions = positions;
    this.wordNums = wordNums;
  }

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public Iterator iterator() {
    return new Iterator(data, positions, wordNums);
  }

  static int readLength(ByteArrayDataInput in, int len) {
    if ((len & 0x08) == 0) {
      // no continuation bit
      return len;
    }
    return (len & 0x07) | (in.readVInt() << 3);
  }

  static class Iterator extends DocIdSetIterator {

    final ByteArrayDataInput in;
    final PackedInts.Reader positions, wordNums;
    int dirtyLength;

    int wordNum; // byte offset
    byte word; // current word
    int bitList; // list of bits set in the current word

    int docID;

    Iterator(byte[] data, PackedInts.Reader positions, PackedInts.Reader wordNums) {
      this.in = new ByteArrayDataInput(data);
      this.positions = positions;
      this.wordNums = wordNums;
      wordNum = -1;
      word = 0;
      bitList = 0;
      docID = -1;
    }

    boolean readSequence() {
      if (in.eof()) {
        wordNum = Integer.MAX_VALUE;
        return false;
      }
      final int token = in.readByte() & 0xFF;
      final int cleanLength = (in.getPosition() == 1 ? 0 : 2) + readLength(in, token >>> 4);
      wordNum += cleanLength;
      dirtyLength = 1 + readLength(in, token & 0x0F);
      return true;
    }

    void skipDirtyBytes(int count) {
      assert count >= 0;
      assert count <= dirtyLength;
      in.skipBytes(count);
      wordNum += count;
      dirtyLength -= count;
    }

    void skipDirtyBytes() {
      in.skipBytes(dirtyLength);
      wordNum += dirtyLength;
      dirtyLength = 0;
    }

    void nextWord() {
      if (dirtyLength == 0 && !readSequence()) {
        return;
      }
      word = in.readByte();
      if (word == 0) {
        word = in.readByte();
        assert word != 0; // there can never be two consecutive null dirty words
        ++wordNum;
        --dirtyLength;
      }
      ++wordNum;
      --dirtyLength;
    }

    int binarySearch(int targetWordNum) {
      int lo = 0, hi = positions.size() - 1;
      while (lo <= hi) {
        final int mid = (lo + hi) >>> 1;
        final int midWordNum = (int) wordNums.get(mid);
        if (midWordNum <= targetWordNum) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }
      assert wordNums.get(hi) <= targetWordNum;
      assert hi+1 == wordNums.size() || wordNums.get(hi + 1) > targetWordNum;
      return hi;
    }

    void advanceWord(int targetWordNum) {
      assert targetWordNum > wordNum;
      if (dirtyLength == 0 && !readSequence()) {
        return;
      }
      int delta = targetWordNum - wordNum;
      if (delta <= dirtyLength + 1) {
        if (delta > 1) {
          skipDirtyBytes(delta - 1);
        }
      } else {
        skipDirtyBytes();
        assert dirtyLength == 0;
        // use the index
        final int i = binarySearch(targetWordNum);
        final int position = (int) positions.get(i);
        if (position > in.getPosition()) { // if the binary search returned a backward offset, don't move
          wordNum = (int) wordNums.get(i) - 1;
          in.setPosition(position);
        }

        while (true) {
          if (!readSequence()) {
            return;
          }
          delta = targetWordNum - wordNum;
          if (delta <= dirtyLength + 1) {
            if (delta > 1) {
              skipDirtyBytes(delta - 1);
            }
            break;
          }
          skipDirtyBytes();
        }
      }

      nextWord();
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      if (bitList != 0) { // there are remaining bits in the current word
        docID = (wordNum << 3) | ((bitList & 0x0F) - 1);
        bitList >>>= 4;
        return docID;
      }
      nextWord();
      if (wordNum == Integer.MAX_VALUE) {
        return docID = NO_MORE_DOCS;
      }
      bitList = BitUtil.bitList(word);
      assert bitList != 0;
      docID = (wordNum << 3) | ((bitList & 0x0F) - 1);
      bitList >>>= 4;
      return docID;
    }

    @Override
    public int advance(int target) throws IOException {
      assert target > docID;
      final int targetWordNum = wordNum(target);
      if (targetWordNum > wordNum) {
        advanceWord(targetWordNum);
        bitList = BitUtil.bitList(word);
      }
      return slowAdvance(target);
    }

    @Override
    public long cost() {
      return in.length(); // good estimation of the cost of iterating over all docs
    }

  }

  /** Return the number of documents in this {@link DocIdSet}. This method
   *  runs in linear time but is much faster than counting documents. */
  public int cardinality() {
    int cardinality = 0;
    for (Iterator it = iterator(); it.wordNum != Integer.MAX_VALUE; it.nextWord()) {
      cardinality += BitUtil.bitCount(it.word);
    }
    return cardinality;
  }

  /** Return the memory usage of this class in bytes. */
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(3 * RamUsageEstimator.NUM_BYTES_OBJECT_REF)
        + RamUsageEstimator.sizeOf(data)
        + positions.ramBytesUsed()
        + wordNums.ramBytesUsed();
  }

}
