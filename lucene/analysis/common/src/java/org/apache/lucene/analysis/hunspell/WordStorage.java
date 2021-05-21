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
package org.apache.lucene.analysis.hunspell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.IntSequenceOutputs;

/**
 * A data structure for memory-efficient word storage and fast lookup/enumeration. Each dictionary
 * entry is stored as:
 *
 * <ol>
 *   <li>the last character
 *   <li>pointer to a similar entry for the prefix (all characters except the last one)
 *   <li>value data: a list of ints representing word flags and morphological data, and a pointer to
 *       hash collisions, if any
 * </ol>
 *
 * There's only one entry for each prefix, so it's like a trie/{@link
 * org.apache.lucene.util.fst.FST}, but a reversed one: each node points to a single previous node
 * instead of several following ones. For example, "abc" and "abd" point to the same prefix entry
 * "ab" which points to "a" which points to 0.<br>
 * <br>
 * The entries are stored in a contiguous byte array, identified by their offsets, using {@link
 * DataOutput#writeVInt} ()} VINT} format for compression.
 */
class WordStorage {
  private static final int OFFSET_BITS = 25;
  private static final int OFFSET_MASK = (1 << OFFSET_BITS) - 1;
  private static final int COLLISION_MASK = 0x40;
  private static final int MAX_STORED_LENGTH = COLLISION_MASK - 1;

  /**
   * A map from word's hash (modulo array's length) into an int containing:
   *
   * <ul>
   *   <li>lower {@link #OFFSET_BITS}: the offset in {@link #wordData} of the last entry with this
   *       hash
   *   <li>the remaining highest bits: COLLISION+LENGTH info for that entry, i.e. one bit indicating
   *       whether there are other entries with the same hash, and the length of the entry in chars,
   *       or {@link #MAX_STORED_LENGTH} if the length exceeds that limit (next highest bits)
   * </ul>
   */
  private final int[] hashTable;

  /**
   * An array of word entries:
   *
   * <ul>
   *   <li>VINT: the word's last character
   *   <li>VINT: a delta pointer to the entry for the same word without the last character.
   *       Precisely, it's the difference of this entry's start and the prefix's entry start. 0 for
   *       single-character entries
   *   <li>(Optional, for hash-colliding entries only)
   *       <ul>
   *         <li>BYTE: COLLISION+LENGTH info (see {@link #hashTable}) for the previous entry with
   *             the same hash
   *         <li>VINT: (delta) pointer to the previous entry
   *       </ul>
   *   <li>(Optional, for non-leaf entries only) VINT+: word form data, returned from {@link
   *       #lookupWord}, preceded by its length
   * </ul>
   */
  private final byte[] wordData;

  private WordStorage(int[] hashTable, byte[] wordData) {
    this.hashTable = hashTable;
    this.wordData = wordData;
  }

  IntsRef lookupWord(char[] word, int offset, int length) {
    assert length > 0;

    int hash = Math.abs(CharsRef.stringHashCode(word, offset, length) % hashTable.length);
    int entryCode = hashTable[hash];
    if (entryCode == 0) {
      return null;
    }

    int pos = entryCode & OFFSET_MASK;
    int mask = entryCode >>> OFFSET_BITS;

    char lastChar = word[offset + length - 1];
    ByteArrayDataInput in = new ByteArrayDataInput(wordData);
    while (true) {
      in.setPosition(pos);
      char c = (char) in.readVInt();
      int prevPos = pos - in.readVInt();

      boolean last = !hasCollision(mask);
      boolean mightMatch = c == lastChar && hasLength(mask, length);

      if (!last) {
        mask = in.readByte();
        pos -= in.readVInt();
      }

      if (mightMatch) {
        int beforeForms = in.getPosition();
        if (isSameString(word, offset, length - 1, prevPos, in)) {
          in.setPosition(beforeForms);
          int formLength = in.readVInt();
          IntsRef forms = new IntsRef(formLength);
          readForms(forms, in, formLength);
          return forms;
        }
      }

      if (last) {
        return null;
      }
    }
  }

  private static boolean hasCollision(int mask) {
    return (mask & COLLISION_MASK) != 0;
  }

  /**
   * Calls the processor for every dictionary entry with length between minLength and maxLength,
   * both ends inclusive. Note that the callback arguments (word and forms) are reused, so they can
   * be modified in any way, but may not be saved for later by the processor
   */
  void processAllWords(int minLength, int maxLength, BiConsumer<CharsRef, IntsRef> processor) {
    assert minLength <= maxLength;
    CharsRef chars = new CharsRef(maxLength);
    IntsRef forms = new IntsRef();
    ByteArrayDataInput in = new ByteArrayDataInput(wordData);
    for (int entryCode : hashTable) {
      int pos = entryCode & OFFSET_MASK;
      int mask = entryCode >>> OFFSET_BITS;

      while (pos != 0) {
        int wordStart = maxLength - 1;

        in.setPosition(pos);
        chars.chars[wordStart] = (char) in.readVInt();
        int prevPos = pos - in.readVInt();

        boolean last = !hasCollision(mask);
        boolean mightMatch = hasLengthInRange(mask, minLength, maxLength);

        if (!last) {
          mask = in.readByte();
          pos -= in.readVInt();
        }

        if (mightMatch) {
          int dataLength = in.readVInt();
          if (forms.ints.length < dataLength) {
            forms.ints = new int[dataLength];
          }
          readForms(forms, in, dataLength);
          while (prevPos != 0 && wordStart > 0) {
            in.setPosition(prevPos);
            chars.chars[--wordStart] = (char) in.readVInt();
            prevPos -= in.readVInt();
          }

          if (prevPos == 0) {
            chars.offset = wordStart;
            chars.length = maxLength - wordStart;
            processor.accept(chars, forms);
          }
        }

        if (last) {
          break;
        }
      }
    }
  }

  private boolean hasLength(int mask, int length) {
    int lenCode = mask & MAX_STORED_LENGTH;
    return lenCode == MAX_STORED_LENGTH ? length >= MAX_STORED_LENGTH : lenCode == length;
  }

  private static boolean hasLengthInRange(int mask, int minLength, int maxLength) {
    int lenCode = mask & MAX_STORED_LENGTH;
    if (lenCode == MAX_STORED_LENGTH) {
      return maxLength >= MAX_STORED_LENGTH;
    }
    return lenCode >= minLength && lenCode <= maxLength;
  }

  private boolean isSameString(
      char[] word, int offset, int length, int dataPos, ByteArrayDataInput in) {
    for (int i = length - 1; i >= 0; i--) {
      in.setPosition(dataPos);
      char c = (char) in.readVInt();
      if (c != word[i + offset]) {
        return false;
      }
      dataPos -= in.readVInt();
      if (dataPos == 0) {
        return i == 0;
      }
    }
    return length == 0 && dataPos == 0;
  }

  private void readForms(IntsRef forms, ByteArrayDataInput in, int length) {
    for (int i = 0; i < length; i++) {
      forms.ints[i] = in.readVInt();
    }
    forms.length = length;
  }

  static class Builder {
    private final boolean hasCustomMorphData;
    private final int[] hashTable;
    private byte[] wordData;
    private final int[] chainLengths;

    private final IntsRefBuilder currentOrds = new IntsRefBuilder();
    private final List<char[]> group = new ArrayList<>();
    private final List<Integer> morphDataIDs = new ArrayList<>();
    private String currentEntry = null;
    private final int wordCount;
    private final FlagEnumerator flagEnumerator;

    private final ByteArrayDataOutput dataWriter;
    private int commonPrefixLength, commonPrefixPos;
    private int actualWords;

    /**
     * @param wordCount an approximate number of the words in the resulting dictionary, used to
     *     pre-size the hash table. This argument can be a bit larger than the actual word count,
     *     but not smaller.
     */
    Builder(int wordCount, boolean hasCustomMorphData, FlagEnumerator flagEnumerator) {
      this.wordCount = wordCount;
      this.flagEnumerator = flagEnumerator;
      this.hasCustomMorphData = hasCustomMorphData;

      hashTable = new int[wordCount];
      wordData = new byte[wordCount * 6];

      dataWriter =
          new ByteArrayDataOutput(wordData) {
            @Override
            public void writeByte(byte b) {
              int pos = getPosition();
              if (pos == wordData.length) {
                wordData = ArrayUtil.grow(wordData);
                reset(wordData, pos, wordData.length - pos);
              }
              super.writeByte(b);
            }
          };
      dataWriter.writeByte((byte) 0); // zero index is root, contains nothing
      chainLengths = new int[hashTable.length];
    }

    /**
     * Add a dictionary entry. This method should be called for entries sorted non-descending by
     * {@link String#compareTo} rules.
     */
    void add(String entry, char[] flags, int morphDataID) throws IOException {
      if (!entry.equals(currentEntry)) {
        if (currentEntry != null) {
          if (entry.compareTo(currentEntry) < 0) {
            throw new IllegalArgumentException("out of order: " + entry + " < " + currentEntry);
          }
          int pos = flushGroup();

          commonPrefixLength = GeneratingSuggester.commonPrefix(currentEntry, entry);
          ByteArrayDataInput in = new ByteArrayDataInput(wordData);
          in.setPosition(pos);
          for (int i = currentEntry.length() - 1; i >= commonPrefixLength; i--) {
            char c = (char) in.readVInt();
            assert c == currentEntry.charAt(i);
            pos -= in.readVInt();
            in.setPosition(pos);
          }
          commonPrefixPos = pos;
        }
        currentEntry = entry;
      }

      group.add(flags);
      if (hasCustomMorphData) {
        morphDataIDs.add(morphDataID);
      }
    }

    private int flushGroup() throws IOException {
      if (++actualWords > wordCount) {
        throw new RuntimeException("Don't add more words than wordCount!");
      }

      currentOrds.clear();
      boolean hasNonHidden = false;
      for (char[] flags : group) {
        if (!hasHiddenFlag(flags)) {
          hasNonHidden = true;
          break;
        }
      }

      for (int i = 0; i < group.size(); i++) {
        char[] flags = group.get(i);
        if (hasNonHidden && hasHiddenFlag(flags)) {
          continue;
        }

        currentOrds.append(flagEnumerator.add(flags));
        if (hasCustomMorphData) {
          currentOrds.append(morphDataIDs.get(i));
        }
      }

      // write the non-leaf entries for chars after the shared prefix, except the last one
      int lastPos = commonPrefixPos;
      for (int i = commonPrefixLength; i < currentEntry.length() - 1; i++) {
        int pos = dataWriter.getPosition();
        dataWriter.writeVInt(currentEntry.charAt(i));
        dataWriter.writeVInt(pos - lastPos);
        lastPos = pos;
      }

      int pos = dataWriter.getPosition();
      if (pos >= 1 << OFFSET_BITS) {
        throw new RuntimeException(
            "Too much word data, please report this to dev@lucene.apache.org");
      }
      int hash = Math.abs(currentEntry.hashCode() % hashTable.length);
      int prevCode = hashTable[hash];

      int mask =
          (prevCode == 0 ? 0 : COLLISION_MASK) | Math.min(currentEntry.length(), MAX_STORED_LENGTH);
      hashTable[hash] = (mask << OFFSET_BITS) | pos;

      if (++chainLengths[hash] > 20) {
        throw new RuntimeException(
            "Too many collisions, please report this to dev@lucene.apache.org");
      }

      // write the leaf entry for the last character
      dataWriter.writeVInt(currentEntry.charAt(currentEntry.length() - 1));
      dataWriter.writeVInt(pos - lastPos);
      if (prevCode != 0) {
        dataWriter.writeByte((byte) (prevCode >>> OFFSET_BITS));
        dataWriter.writeVInt(pos - (prevCode & OFFSET_MASK));
      }
      IntSequenceOutputs.getSingleton().write(currentOrds.get(), dataWriter);

      group.clear();
      morphDataIDs.clear();
      return pos;
    }

    private static boolean hasHiddenFlag(char[] flags) {
      for (char flag : flags) {
        if (flag == Dictionary.HIDDEN_FLAG) {
          return true;
        }
      }
      return false;
    }

    WordStorage build() throws IOException {
      assert !group.isEmpty() : "build() should be only called once";
      flushGroup();
      return new WordStorage(
          hashTable, ArrayUtil.copyOfSubArray(wordData, 0, dataWriter.getPosition()));
    }
  }
}
