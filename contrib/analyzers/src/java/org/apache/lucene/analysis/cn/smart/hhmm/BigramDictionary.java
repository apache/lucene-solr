/**
 * Copyright 2009 www.imdict.net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

public class BigramDictionary extends AbstractDictionary {

  private BigramDictionary() {
  }

  public static final char WORD_SEGMENT_CHAR = '@';

  private static BigramDictionary singleInstance;

  public static final int PRIME_BIGRAM_LENGTH = 402137;

  /**
   * bigramTable 来存储词与词之间的跳转频率， bigramHashTable 和 frequencyTable
   * 就是用来存储这些频率的数据结构。 为了提高查询速度和节省内存， 采用 hash 值来代替关联词作为查询依据， 关联词就是
   * (formWord+'@'+toWord) ， 利用 FNV1 hash 算法来计算关联词的hash值 ，并保存在 bigramHashTable
   * 中，利用 hash 值来代替关联词有可能会产生很小概率的冲突， 但是 long 类型
   * (64bit)的hash值有效地将此概率降到极低。bigramHashTable[i]与frequencyTable[i]一一对应
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
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return false;
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
          // 实际上将0作为初始值有一点问题，因为某个字符串可能hash值为0，但是概率非常小，因此影响不大
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
   * 将词库文件加载到WordDictionary的相关数据结构中，只是加载，没有进行合并和修改操作
   * 
   * @param dctFilePath
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   * @throws UnsupportedEncodingException
   */
  public void loadFromFile(String dctFilePath) throws FileNotFoundException,
      IOException, UnsupportedEncodingException {

    int i, cnt, length, total = 0;
    // 文件中只统计了6763个汉字加5个空汉字符3756~3760，其中第3756个用来存储符号信息。
    int[] buffer = new int[3];
    byte[] intBuffer = new byte[4];
    String tmpword;
    RandomAccessFile dctFile = new RandomAccessFile(dctFilePath, "r");

    // 字典文件中第一个汉字出现的位置是0，最后一个是6768
    for (i = GB2312_FIRST_CHAR; i < GB2312_FIRST_CHAR + CHAR_NUM_IN_FILE; i++) {
      String currentStr = getCCByGB2312Id(i);
      // if (i == 5231)
      // System.out.println(i);

      dctFile.read(intBuffer);// 原词库文件在c下开发，所以写入的文件为little
      // endian编码，而java为big endian，必须转换过来
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

  /*
   * public void test(String dctFilePath) throws IOException { int i, cnt,
   * length, total = 0; int corrupt = 0, notFound = 0; //
   * 文件中只统计了6763个汉字加5个空汉字符3756~3760，其中第3756个用来存储符号信息。 int[] buffer = new int[3];
   * byte[] intBuffer = new byte[4]; String tmpword; RandomAccessFile dctFile =
   * new RandomAccessFile(dctFilePath, "r");
   * 
   * // 字典文件中第一个汉字出现的位置是0，最后一个是6768 for (i = GB2312_FIRST_CHAR; i <
   * GB2312_FIRST_CHAR + CHAR_NUM_IN_FILE; i++) { String currentStr =
   * getCCByGB2312Id(i); // if (i == 5231) // System.out.println(i);
   * 
   * dctFile.read(intBuffer);// 原词库文件在c下开发，所以写入的文件为little // endian编码，而java为big
   * endian，必须转换过来 cnt =
   * ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN) .getInt(); if
   * (cnt <= 0) { continue; } total += cnt; int j = 0; while (j < cnt) {
   * dctFile.read(intBuffer); buffer[0] = ByteBuffer.wrap(intBuffer).order(
   * ByteOrder.LITTLE_ENDIAN).getInt();// frequency dctFile.read(intBuffer);
   * buffer[1] = ByteBuffer.wrap(intBuffer).order(
   * ByteOrder.LITTLE_ENDIAN).getInt();// length dctFile.read(intBuffer); //
   * buffer[2] = ByteBuffer.wrap(intBuffer).order( //
   * ByteOrder.LITTLE_ENDIAN).getInt();// handle
   * 
   * length = buffer[1]; if (length > 0) { byte[] lchBuffer = new byte[length];
   * dctFile.read(lchBuffer); tmpword = new String(lchBuffer, "GB2312"); if (i
   * != 3755 + GB2312_FIRST_CHAR) { tmpword = currentStr + tmpword; } char
   * carray[] = tmpword.toCharArray(); int index = getBigramItemIndex(carray);
   * if (index != -1) { // if (!bigramStringTable[index].equals(tmpword)) { //
   * System.out.println("corrupt: " + tmpword + "<->" // +
   * bigramStringTable[index]); // corrupt++; // } } else {
   * System.out.println("not found: " + tmpword); notFound++; } } j++; } }
   * dctFile.close(); System.out.println("num not found:" + notFound);
   * System.out.println("num corrupt:" + corrupt);
   * 
   * log.info("test dictionary done! " + dctFilePath + " total:" + total); cnt =
   * 0; for (int j = 0; j < PRIME_BIGRAM_LENGTH; j++) { if (bigramHashTable[j]
   * != 0) { cnt++; } } System.out.println("total num in bigramTable: " + cnt);
   * }
   */

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

  /**
   * @param c
   * @return
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

  public static void main(String[] args) throws FileNotFoundException,
      UnsupportedEncodingException, IOException {
    BigramDictionary dic = new BigramDictionary();
    dic.load("D:/analysis-data");
    // dic.test("D:/analysis-data/BigramDict.dct");
    System.out.println("max:" + dic.max);
    System.out.println("average repeat:" + (double) dic.repeat / 328856);
    System.out.println("end");
  }
}
