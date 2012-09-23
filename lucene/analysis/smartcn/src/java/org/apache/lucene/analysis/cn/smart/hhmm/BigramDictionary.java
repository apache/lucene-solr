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

package org.apache.lucene.analysis.cn.smart.hhmm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.lucene.analysis.cn.smart.AnalyzerProfile;

/**
 * SmartChineseAnalyzer Bigram dictionary.
 * @lucene.experimental
 */
class BigramDictionary extends AbstractDictionary {

  private BigramDictionary() {
  }

  public static final char WORD_SEGMENT_CHAR = '@';

  private static BigramDictionary singleInstance;

  public static final int PRIME_BIGRAM_LENGTH = 402137;

  /*
   * The word associations are stored as FNV1 hashcodes, which have a small probability of collision, but save memory.  
   */
  private long[] bigramHashTable;

  private int[] frequencyTable;

  private int max = 0;

  private int repeat = 0;

  // static Logger log = Logger.getLogger(BigramDictionary.class);

  public synchronized static BigramDictionary getInstance() {
    if (singleInstance == null) {
      singleInstance = new BigramDictionary();
      try {
        singleInstance.load();
      } catch (IOException e) {
        String dictRoot = AnalyzerProfile.ANALYSIS_DATA_DIR;
        singleInstance.load(dictRoot);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return singleInstance;
  }

  private boolean loadFromObj(File serialObj) {
    try {
      loadFromInputStream(new FileInputStream(serialObj));
      return true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void loadFromInputStream(InputStream serialObjectInputStream)
      throws IOException, ClassNotFoundException {
    ObjectInputStream input = new ObjectInputStream(serialObjectInputStream);
    bigramHashTable = (long[]) input.readObject();
    frequencyTable = (int[]) input.readObject();
    // log.info("load bigram dict from serialization.");
    input.close();
  }

  private void saveToObj(File serialObj) {
    try {
      ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(
          serialObj));
      output.writeObject(bigramHashTable);
      output.writeObject(frequencyTable);
      output.close();
      // log.info("serialize bigram dict.");
    } catch (Exception e) {
      // log.warn(e.getMessage());
    }
  }

  private void load() throws IOException, ClassNotFoundException {
    InputStream input = this.getClass().getResourceAsStream("bigramdict.mem");
    loadFromInputStream(input);
  }

  private void load(String dictRoot) {
    String bigramDictPath = dictRoot + "/bigramdict.dct";

    File serialObj = new File(dictRoot + "/bigramdict.mem");

    if (serialObj.exists() && loadFromObj(serialObj)) {

    } else {
      try {
        bigramHashTable = new long[PRIME_BIGRAM_LENGTH];
        frequencyTable = new int[PRIME_BIGRAM_LENGTH];
        for (int i = 0; i < PRIME_BIGRAM_LENGTH; i++) {
          // it is possible for a value to hash to 0, but the probability is extremely low
          bigramHashTable[i] = 0;
          frequencyTable[i] = 0;
        }
        loadFromFile(bigramDictPath);
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage());
      }
      saveToObj(serialObj);
    }
  }

  /**
   * Load the datafile into this BigramDictionary
   * 
   * @param dctFilePath path to the Bigramdictionary (bigramdict.dct)
   * @throws IOException If there is a low-level I/O error
   */
  public void loadFromFile(String dctFilePath) throws IOException {

    int i, cnt, length, total = 0;
    // The file only counted 6763 Chinese characters plus 5 reserved slots 3756~3760.  
    // The 3756th is used (as a header) to store information.
    int[] buffer = new int[3];
    byte[] intBuffer = new byte[4];
    String tmpword;
    RandomAccessFile dctFile = new RandomAccessFile(dctFilePath, "r");

    // GB2312 characters 0 - 6768
    for (i = GB2312_FIRST_CHAR; i < GB2312_FIRST_CHAR + CHAR_NUM_IN_FILE; i++) {
      String currentStr = getCCByGB2312Id(i);
      // if (i == 5231)
      // System.out.println(i);

      dctFile.read(intBuffer);
      // the dictionary was developed for C, and byte order must be converted to work with Java
      cnt = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (cnt <= 0) {
        continue;
      }
      total += cnt;
      int j = 0;
      while (j < cnt) {
        dctFile.read(intBuffer);
        buffer[0] = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN)
            .getInt();// frequency
        dctFile.read(intBuffer);
        buffer[1] = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN)
            .getInt();// length
        dctFile.read(intBuffer);
        // buffer[2] = ByteBuffer.wrap(intBuffer).order(
        // ByteOrder.LITTLE_ENDIAN).getInt();// handle

        length = buffer[1];
        if (length > 0) {
          byte[] lchBuffer = new byte[length];
          dctFile.read(lchBuffer);
          tmpword = new String(lchBuffer, "GB2312");
          if (i != 3755 + GB2312_FIRST_CHAR) {
            tmpword = currentStr + tmpword;
          }
          char carray[] = tmpword.toCharArray();
          long hashId = hash1(carray);
          int index = getAvaliableIndex(hashId, carray);
          if (index != -1) {
            if (bigramHashTable[index] == 0) {
              bigramHashTable[index] = hashId;
              // bigramStringTable[index] = tmpword;
            }
            frequencyTable[index] += buffer[0];
          }
        }
        j++;
      }
    }
    dctFile.close();
    // log.info("load dictionary done! " + dctFilePath + " total:" + total);
  }

  private int getAvaliableIndex(long hashId, char carray[]) {
    int hash1 = (int) (hashId % PRIME_BIGRAM_LENGTH);
    int hash2 = hash2(carray) % PRIME_BIGRAM_LENGTH;
    if (hash1 < 0)
      hash1 = PRIME_BIGRAM_LENGTH + hash1;
    if (hash2 < 0)
      hash2 = PRIME_BIGRAM_LENGTH + hash2;
    int index = hash1;
    int i = 1;
    while (bigramHashTable[index] != 0 && bigramHashTable[index] != hashId
        && i < PRIME_BIGRAM_LENGTH) {
      index = (hash1 + i * hash2) % PRIME_BIGRAM_LENGTH;
      i++;
    }
    // System.out.println(i - 1);

    if (i < PRIME_BIGRAM_LENGTH
        && (bigramHashTable[index] == 0 || bigramHashTable[index] == hashId)) {
      return index;
    } else
      return -1;
  }

  /*
   * lookup the index into the frequency array.
   */
  private int getBigramItemIndex(char carray[]) {
    long hashId = hash1(carray);
    int hash1 = (int) (hashId % PRIME_BIGRAM_LENGTH);
    int hash2 = hash2(carray) % PRIME_BIGRAM_LENGTH;
    if (hash1 < 0)
      hash1 = PRIME_BIGRAM_LENGTH + hash1;
    if (hash2 < 0)
      hash2 = PRIME_BIGRAM_LENGTH + hash2;
    int index = hash1;
    int i = 1;
    repeat++;
    while (bigramHashTable[index] != 0 && bigramHashTable[index] != hashId
        && i < PRIME_BIGRAM_LENGTH) {
      index = (hash1 + i * hash2) % PRIME_BIGRAM_LENGTH;
      i++;
      repeat++;
      if (i > max)
        max = i;
    }
    // System.out.println(i - 1);

    if (i < PRIME_BIGRAM_LENGTH && bigramHashTable[index] == hashId) {
      return index;
    } else
      return -1;
  }

  public int getFrequency(char[] carray) {
    int index = getBigramItemIndex(carray);
    if (index != -1)
      return frequencyTable[index];
    return 0;
  }

}
