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
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * {@link DocIdSet} implementation based on word-aligned hybrid encoding on
 * words of 8 bits.
 * <p>This implementation doesn't support random-access but has a fast
 * {@link DocIdSetIterator} which can advance in logarithmic time thanks to
 * an index.</p>
 * <p>The compression scheme is simplistic and should work well with sparse and
 * very dense doc id sets while being only slightly larger than a
 * {@link FixedBitSet} for incompressible sets (overhead&lt;2% in the worst
 * case) in spite of the index.</p>
 * <p><b>Format</b>: The format is byte-aligned. An 8-bits word is either clean,
 * meaning composed only of zeros or ones, or dirty, meaning that it contains
 * between 1 and 7 bits set. The idea is to encode sequences of clean words
 * using run-length encoding and to leave sequences of dirty words as-is.</p>
 * <table>
 *   <tr><th>Token</th><th>Clean length+</th><th>Dirty length+</th><th>Dirty words</th></tr>
 *   <tr><td>1 byte</td><td>0-n bytes</td><td>0-n bytes</td><td>0-n bytes</td></tr>
 * </table>
 * <ul>
 *   <li><b>Token</b> encodes whether clean means full of zeros or ones in the
 * first bit, the number of clean words minus 2 on the next 3 bits and the
 * number of dirty words on the last 4 bits. The higher-order bit is a
 * continuation bit, meaning that the number is incomplete and needs additional
 * bytes to be read.</li>
 *   <li><b>Clean length+</b>: If clean length has its higher-order bit set,
 * you need to read a {@link DataInput#readVInt() vint}, shift it by 3 bits on
 * the left side and add it to the 3 bits which have been read in the token.</li>
 *   <li><b>Dirty length+</b> works the same way as <b>Clean length+</b> but
 * on 4 bits and for the length of dirty words.</li>
 *   <li><b>Dirty words</b> are the dirty words, there are <b>Dirty length</b>
 * of them.</li>
 * </ul>
 * <p>This format cannot encode sequences of less than 2 clean words and 0 dirty
 * word. The reason is that if you find a single clean word, you should rather
 * encode it as a dirty word. This takes the same space as starting a new
 * sequence (since you need one byte for the token) but will be lighter to
 * decode. There is however an exception for the first sequence. Since the first
 * sequence may start directly with a dirty word, the clean length is encoded
 * directly, without subtracting 2.</p>
 * <p>There is an additional restriction on the format: the sequence of dirty
 * words is not allowed to contain two consecutive clean words. This restriction
 * exists to make sure no space is wasted and to make sure iterators can read
 * the next doc ID by reading at most 2 dirty words.</p>
 * @lucene.experimental
 */
public final class WAH8DocIdSet extends DocIdSet implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(WAH8DocIdSet.class);

  // Minimum index interval, intervals below this value can't guarantee anymore
  // that this set implementation won't be significantly larger than a FixedBitSet
  // The reason is that a single sequence saves at least one byte and an index
  // entry requires at most 8 bytes (2 ints) so there shouldn't be more than one
  // index entry every 8 sequences
  private static final int MIN_INDEX_INTERVAL = 8;

  /** Default index interval. */
  public static final int DEFAULT_INDEX_INTERVAL = 24;

  private static final PackedLongValues SINGLE_ZERO = PackedLongValues.packedBuilder(PackedInts.COMPACT).add(0L).build();
  private static WAH8DocIdSet EMPTY = new WAH8DocIdSet(new byte[0], 0, 1, SINGLE_ZERO, SINGLE_ZERO);

  private static final Comparator<Iterator> SERIALIZED_LENGTH_COMPARATOR = new Comparator<Iterator>() {
    @Override
    public int compare(Iterator wi1, Iterator wi2) {
      return wi1.in.length() - wi2.in.length();
    }
  };

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
    int cardinality;
    boolean reverse;

    WordBuilder() {
      out = new GrowableByteArrayDataOutput(1024);
      dirtyWords = new GrowableByteArrayDataOutput(128);
      clean = 0;
      lastWordNum = -1;
      numSequences = 0;
      indexInterval = DEFAULT_INDEX_INTERVAL;
      cardinality = 0;
    }

    /** Set the index interval. Smaller index intervals improve performance of
     *  {@link DocIdSetIterator#advance(int)} but make the {@link DocIdSet}
     *  larger. An index interval <code>i</code> makes the index add an overhead
     *  which is at most <code>4/i</code>, but likely much less.The default index
     *  interval is <code>8</code>, meaning the index has an overhead of at most
     *  50%. To disable indexing, you can pass {@link Integer#MAX_VALUE} as an
     *  index interval. */
    public WordBuilder setIndexInterval(int indexInterval) {
      if (indexInterval < MIN_INDEX_INTERVAL) {
        throw new IllegalArgumentException("indexInterval must be >= " + MIN_INDEX_INTERVAL);
      }
      this.indexInterval = indexInterval;
      return this;
    }

    void writeHeader(boolean reverse, int cleanLength, int dirtyLength) throws IOException {
      final int cleanLengthMinus2 = cleanLength - 2;
      assert cleanLengthMinus2 >= 0;
      assert dirtyLength >= 0;
      int token = ((cleanLengthMinus2 & 0x03) << 4) | (dirtyLength & 0x07);
      if (reverse) {
        token |= 1 << 7;
      }
      if (cleanLengthMinus2 > 0x03) {
        token |= 1 << 6;
      }
      if (dirtyLength > 0x07) {
        token |= 1 << 3;
      }
      out.writeByte((byte) token);
      if (cleanLengthMinus2 > 0x03) {
        out.writeVInt(cleanLengthMinus2 >>> 2);
      }
      if (dirtyLength > 0x07) {
        out.writeVInt(dirtyLength >>> 3);
      }
    }

    private boolean sequenceIsConsistent() {
      for (int i = 1; i < dirtyWords.length; ++i) {
        assert dirtyWords.bytes[i-1] != 0 || dirtyWords.bytes[i] != 0;
        assert dirtyWords.bytes[i-1] != (byte) 0xFF || dirtyWords.bytes[i] != (byte) 0xFF;
      }
      return true;
    }

    void writeSequence() {
      assert sequenceIsConsistent();
      try {
        writeHeader(reverse, clean, dirtyWords.length);
      } catch (IOException cannotHappen) {
        throw new AssertionError(cannotHappen);
      }
      out.writeBytes(dirtyWords.bytes, 0, dirtyWords.length);
      dirtyWords.length = 0;
      ++numSequences;
    }

    void addWord(int wordNum, byte word) {
      assert wordNum > lastWordNum;
      assert word != 0;

      if (!reverse) {
        if (lastWordNum == -1) {
          clean = 2 + wordNum; // special case for the 1st sequence
          dirtyWords.writeByte(word);
        } else {
          switch (wordNum - lastWordNum) {
            case 1:
              if (word == (byte) 0xFF && dirtyWords.bytes[dirtyWords.length-1] == (byte) 0xFF) {
                --dirtyWords.length;
                writeSequence();
                reverse = true;
                clean = 2;
              } else {
                dirtyWords.writeByte(word);
              }
              break;
            case 2:
              dirtyWords.writeByte((byte) 0);
              dirtyWords.writeByte(word);
              break;
            default:
              writeSequence();
              clean = wordNum - lastWordNum - 1;
              dirtyWords.writeByte(word);
          }
        }
      } else {
        assert lastWordNum >= 0;
        switch (wordNum - lastWordNum) {
          case 1:
            if (word == (byte) 0xFF) {
              if (dirtyWords.length == 0) {
                ++clean;
              } else if (dirtyWords.bytes[dirtyWords.length - 1] == (byte) 0xFF) {
                --dirtyWords.length;
                writeSequence();
                clean = 2;
              } else {
                dirtyWords.writeByte(word);
              }
            } else {
              dirtyWords.writeByte(word);
            }
            break;
          case 2:
            dirtyWords.writeByte((byte) 0);
            dirtyWords.writeByte(word);
            break;
          default:
            writeSequence();
            reverse = false;
            clean = wordNum - lastWordNum - 1;
            dirtyWords.writeByte(word);
        }
      }
      lastWordNum = wordNum;
      cardinality += BitUtil.bitCount(word);
    }

    /** Build a new {@link WAH8DocIdSet}. */
    public WAH8DocIdSet build() {
      if (cardinality == 0) {
        assert lastWordNum == -1;
        return EMPTY;
      }
      writeSequence();
      final byte[] data = Arrays.copyOf(out.bytes, out.length);

      // Now build the index
      final int valueCount = (numSequences - 1) / indexInterval + 1;
      final PackedLongValues indexPositions, indexWordNums;
      if (valueCount <= 1) {
        indexPositions = indexWordNums = SINGLE_ZERO;
      } else {
        final int pageSize = 128;
        final PackedLongValues.Builder positions = PackedLongValues.monotonicBuilder(pageSize, PackedInts.COMPACT);
        final PackedLongValues.Builder wordNums = PackedLongValues.monotonicBuilder(pageSize, PackedInts.COMPACT);

        positions.add(0L);
        wordNums.add(0L);
        final Iterator it = new Iterator(data, cardinality, Integer.MAX_VALUE, SINGLE_ZERO, SINGLE_ZERO);
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
          positions.add(position);
          wordNums.add(wordNum + 1);
        }
        indexPositions = positions.build();
        indexWordNums = wordNums.build();
      }

      return new WAH8DocIdSet(data, cardinality, indexInterval, indexPositions, indexWordNums);
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

    /** Add the content of the provided {@link DocIdSetIterator}. */
    public Builder add(DocIdSetIterator disi) throws IOException {
      for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
        add(doc);
      }
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
  private final int cardinality;
  private final int indexInterval;
  // index for advance(int)
  private final PackedLongValues positions, wordNums; // wordNums[i] starts at the sequence at positions[i]

  WAH8DocIdSet(byte[] data, int cardinality, int indexInterval, PackedLongValues positions, PackedLongValues wordNums) {
    this.data = data;
    this.cardinality = cardinality;
    this.indexInterval = indexInterval;
    this.positions = positions;
    this.wordNums = wordNums;
  }

  @Override
  public boolean isCacheable() {
    return true;
  }

  @Override
  public Iterator iterator() {
    return new Iterator(data, cardinality, indexInterval, positions, wordNums);
  }

  static int readCleanLength(ByteArrayDataInput in, int token) {
    int len = (token >>> 4) & 0x07;
    final int startPosition = in.getPosition();
    if ((len & 0x04) != 0) {
      len = (len & 0x03) | (in.readVInt() << 2);
    }
    if (startPosition != 1) {
      len += 2;
    }
    return len;
  }

  static int readDirtyLength(ByteArrayDataInput in, int token) {
    int len = token & 0x0F;
    if ((len & 0x08) != 0) {
      len = (len & 0x07) | (in.readVInt() << 3);
    }
    return len;
  }

  static class Iterator extends DocIdSetIterator {

    /* Using the index can be costly for close targets. */
    static int indexThreshold(int cardinality, int indexInterval) {
      // Short sequences encode for 3 words (2 clean words and 1 dirty byte),
      // don't advance if we are going to read less than 3 x indexInterval
      // sequences
      long indexThreshold = 3L * 3 * indexInterval;
      return (int) Math.min(Integer.MAX_VALUE, indexThreshold);
    }

    final ByteArrayDataInput in;
    final int cardinality;
    final int indexInterval;
    final PackedLongValues positions, wordNums;
    final int indexThreshold;
    int allOnesLength;
    int dirtyLength;

    int wordNum; // byte offset
    byte word; // current word
    int bitList; // list of bits set in the current word
    int sequenceNum; // in which sequence are we?

    int docID;

    Iterator(byte[] data, int cardinality, int indexInterval, PackedLongValues positions, PackedLongValues wordNums) {
      this.in = new ByteArrayDataInput(data);
      this.cardinality = cardinality;
      this.indexInterval = indexInterval;
      this.positions = positions;
      this.wordNums = wordNums;
      wordNum = -1;
      word = 0;
      bitList = 0;
      sequenceNum = -1;
      docID = -1;
      indexThreshold = indexThreshold(cardinality, indexInterval);
    }

    boolean readSequence() {
      if (in.eof()) {
        wordNum = Integer.MAX_VALUE;
        return false;
      }
      final int token = in.readByte() & 0xFF;
      if ((token & (1 << 7)) == 0) {
        final int cleanLength = readCleanLength(in, token);
        wordNum += cleanLength;
      } else {
        allOnesLength = readCleanLength(in, token);
      }
      dirtyLength = readDirtyLength(in, token);
      assert in.length() - in.getPosition() >= dirtyLength : in.getPosition() + " " + in.length() + " " + dirtyLength;
      ++sequenceNum;
      return true;
    }

    void skipDirtyBytes(int count) {
      assert count >= 0;
      assert count <= allOnesLength + dirtyLength;
      wordNum += count;
      if (count <= allOnesLength) {
        allOnesLength -= count;
      } else {
        count -= allOnesLength;
        allOnesLength = 0;
        in.skipBytes(count);
        dirtyLength -= count;
      }
    }

    void skipDirtyBytes() {
      wordNum += allOnesLength + dirtyLength;
      in.skipBytes(dirtyLength);
      allOnesLength = 0;
      dirtyLength = 0;
    }

    void nextWord() {
      if (allOnesLength > 0) {
        word = (byte) 0xFF;
        ++wordNum;
        --allOnesLength;
        return;
      }
      if (dirtyLength > 0) {
        word = in.readByte();
        ++wordNum;
        --dirtyLength;
        if (word != 0) {
          return;
        }
        if (dirtyLength > 0) {
          word = in.readByte();
          ++wordNum;
          --dirtyLength;
          assert word != 0; // never more than one consecutive 0
          return;
        }
      }
      if (readSequence()) {
        nextWord();
      }
    }

    int forwardBinarySearch(int targetWordNum) {
      // advance forward and double the window at each step
      final int indexSize = (int) wordNums.size();
      int lo = sequenceNum / indexInterval, hi = lo + 1;
      assert sequenceNum == -1 || wordNums.get(lo) <= wordNum;
      assert lo + 1 == wordNums.size() || wordNums.get(lo + 1) > wordNum;
      while (true) {
        if (hi >= indexSize) {
          hi = indexSize - 1;
          break;
        } else if (wordNums.get(hi) >= targetWordNum) {
          break;
        }
        final int newLo = hi;
        hi += (hi - lo) << 1;
        lo = newLo;
      }

      // we found a window containing our target, let's binary search now
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
      int delta = targetWordNum - wordNum;
      if (delta <= allOnesLength + dirtyLength + 1) {
        skipDirtyBytes(delta - 1);
      } else {
        skipDirtyBytes();
        assert dirtyLength == 0;
        if (delta > indexThreshold) {
          // use the index
          final int i = forwardBinarySearch(targetWordNum);
          final int position = (int) positions.get(i);
          if (position > in.getPosition()) { // if the binary search returned a backward offset, don't move
            wordNum = (int) wordNums.get(i) - 1;
            in.setPosition(position);
            sequenceNum = i * indexInterval - 1;
          }
        }

        while (true) {
          if (!readSequence()) {
            return;
          }
          delta = targetWordNum - wordNum;
          if (delta <= allOnesLength + dirtyLength + 1) {
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
      return cardinality;
    }

  }

  /** Return the number of documents in this {@link DocIdSet} in constant time. */
  public int cardinality() {
    return cardinality;
  }

  @Override
  public long ramBytesUsed() {
    if (this == EMPTY) {
      return 0L;
    }
    long ramBytesUsed = BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(data);
    if (positions != SINGLE_ZERO) {
      ramBytesUsed += positions.ramBytesUsed();
    }
    if (wordNums != SINGLE_ZERO) {
      ramBytesUsed += wordNums.ramBytesUsed();
    }
    return ramBytesUsed;
  }

}
