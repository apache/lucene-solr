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
  /**
   * A map from word's hash (modulo array's length) into the offset of the last entry in {@link
   * #wordData} with this hash. Negated, if there's more than one entry with the same hash.
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
   *   <li>Optional, for non-leaf entries only:
   *       <ul>
   *         <li>VINT: the length of the word form data, returned from {@link #lookupWord}
   *         <li>n * VINT: the word form data
   *         <li>Optional, for hash-colliding entries only:
   *             <ul>
   *               <li>BYTE: 1 if the next collision entry has further collisions, 0 if it's the
   *                   last of the entries with the same hash
   *               <li>VINT: (delta) pointer to the previous entry with the same hash
   *             </ul>
   *       </ul>
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
    int pos = hashTable[hash];
    if (pos == 0) {
      return null;
    }

    boolean collision = pos < 0;
    pos = Math.abs(pos);

    char lastChar = word[offset + length - 1];
    ByteArrayDataInput in = new ByteArrayDataInput(wordData);
    while (true) {
      in.setPosition(pos);
      char c = (char) in.readVInt();
      int prevPos = pos - in.readVInt();
      int beforeForms = in.getPosition();
      boolean found = c == lastChar && isSameString(word, offset, length - 1, prevPos, in);
      if (!collision && !found) {
        return null;
      }

      in.setPosition(beforeForms);
      int formLength = in.readVInt();
      if (found) {
        IntsRef forms = new IntsRef(formLength);
        readForms(forms, in, formLength);
        return forms;
      } else {
        skipVInts(in, formLength);
      }

      collision = in.readByte() == 1;
      pos -= in.readVInt();
    }
  }

  private static void skipVInts(ByteArrayDataInput in, int count) {
    for (int i = 0; i < count; ) {
      if (in.readByte() >= 0) i++;
    }
  }

  /**
   * @param maxLength the limit on the length of words to be processed, the callback won't be
   *     invoked for the longer ones
   * @param processor is invoked for each word. Note that the passed arguments (word and form) are
   *     reused, so they can be modified in any way, but may not be saved for later by the processor
   */
  void processAllWords(int maxLength, BiConsumer<CharsRef, IntsRef> processor) {
    CharsRef chars = new CharsRef(maxLength);
    IntsRef forms = new IntsRef();
    ByteArrayDataInput in = new ByteArrayDataInput(wordData);
    for (int pos : hashTable) {
      boolean collision = pos < 0;
      pos = Math.abs(pos);

      while (pos != 0) {
        int wordStart = maxLength - 1;

        in.setPosition(pos);
        chars.chars[wordStart] = (char) in.readVInt();
        int prevPos = pos - in.readVInt();

        int dataLength = in.readVInt();
        if (forms.ints.length < dataLength) {
          forms.ints = new int[dataLength];
        }
        readForms(forms, in, dataLength);

        int afterForms = in.getPosition();

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

        if (!collision) {
          break;
        }

        in.setPosition(afterForms);
        collision = in.readVInt() == 1;
        pos -= in.readVInt();
      }
    }
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
      int hash = Math.abs(currentEntry.hashCode() % hashTable.length);
      int collision = hashTable[hash];
      hashTable[hash] = collision == 0 ? pos : -pos;

      if (++chainLengths[hash] > 20) {
        throw new RuntimeException(
            "Too many collisions, please report this to dev@lucene.apache.org");
      }

      // write the leaf entry for the last character
      dataWriter.writeVInt(currentEntry.charAt(currentEntry.length() - 1));
      dataWriter.writeVInt(pos - lastPos);
      IntSequenceOutputs.getSingleton().write(currentOrds.get(), dataWriter);
      if (collision != 0) {
        dataWriter.writeByte(collision < 0 ? (byte) 1 : 0);
        dataWriter.writeVInt(pos - Math.abs(collision));
      }

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
