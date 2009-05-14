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
import org.apache.lucene.analysis.cn.smart.Utility;

public class WordDictionary extends AbstractDictionary {

  private WordDictionary() {
  }

  private static WordDictionary singleInstance;

  /**
   * 一个较大的素数，保证hash查找能够遍历所有位置
   */
  public static final int PRIME_INDEX_LENGTH = 12071;

  /**
   * wordIndexTable保证将Unicode中的所有汉字编码hash到PRIME_INDEX_LENGTH长度的数组中，
   * 当然会有冲突，但实际上本程序只处理GB2312字符部分，6768个字符加上一些ASCII字符，
   * 因此对这些字符是有效的，为了保证比较的准确性，保留原来的字符在charIndexTable中以确定查找的准确性
   */
  private short[] wordIndexTable;

  private char[] charIndexTable;

  /**
   * 存储所有词库的真正数据结构，为了避免占用空间太多，用了两个单独的多维数组来存储词组和频率。
   * 每个词放在一个char[]中，每个char对应一个汉字或其他字符，每个频率放在一个int中，
   * 这两个数组的前两个下表是一一对应的。因此可以利用wordItem_charArrayTable[i][j]来查词，
   * 用wordItem_frequencyTable[i][j]来查询对应的频率
   */
  private char[][][] wordItem_charArrayTable;

  private int[][] wordItem_frequencyTable;

  // static Logger log = Logger.getLogger(WordDictionary.class);

  public synchronized static WordDictionary getInstance() {
    if (singleInstance == null) {
      singleInstance = new WordDictionary();
      try {
        singleInstance.load();
      } catch (IOException e) {
        String wordDictRoot = AnalyzerProfile.ANALYSIS_DATA_DIR;
        singleInstance.load(wordDictRoot);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return singleInstance;
  }

  /**
   * 从外部文件夹dctFileRoot加载词典库文件，首先测试是否有coredict.mem文件， 如果有则直接作为序列化对象加载，
   * 如果没有则加载词典库源文件coredict.dct
   * 
   * @param dctFileName 词典库文件的路径
   */
  public void load(String dctFileRoot) {
    String dctFilePath = dctFileRoot + "/coredict.dct";
    File serialObj = new File(dctFileRoot + "/coredict.mem");

    if (serialObj.exists() && loadFromObj(serialObj)) {

    } else {
      try {
        wordIndexTable = new short[PRIME_INDEX_LENGTH];
        charIndexTable = new char[PRIME_INDEX_LENGTH];
        for (int i = 0; i < PRIME_INDEX_LENGTH; i++) {
          charIndexTable[i] = 0;
          wordIndexTable[i] = -1;
        }
        wordItem_charArrayTable = new char[GB2312_CHAR_NUM][][];
        wordItem_frequencyTable = new int[GB2312_CHAR_NUM][];
        // int total =
        loadMainDataFromFile(dctFilePath);
        expandDelimiterData();
        mergeSameWords();
        sortEachItems();
        // log.info("load dictionary: " + dctFilePath + " total:" + total);
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage());
      }

      saveToObj(serialObj);
    }

  }

  /**
   * 从jar内部加载词典库文件，要求保证WordDictionary类当前路径中有coredict.mem文件，以将其作为序列化对象加载
   * 
   * @param dctFileName 词典库文件的路径
   * @throws ClassNotFoundException
   * @throws IOException
   */
  public void load() throws IOException, ClassNotFoundException {
    InputStream input = this.getClass().getResourceAsStream("coredict.mem");
    loadFromObjectInputStream(input);
  }

  private boolean loadFromObj(File serialObj) {
    try {
      loadFromObjectInputStream(new FileInputStream(serialObj));
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

  private void loadFromObjectInputStream(InputStream serialObjectInputStream)
      throws IOException, ClassNotFoundException {
    ObjectInputStream input = new ObjectInputStream(serialObjectInputStream);
    wordIndexTable = (short[]) input.readObject();
    charIndexTable = (char[]) input.readObject();
    wordItem_charArrayTable = (char[][][]) input.readObject();
    wordItem_frequencyTable = (int[][]) input.readObject();
    // log.info("load core dict from serialization.");
    input.close();
  }

  private void saveToObj(File serialObj) {
    try {
      ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(
          serialObj));
      output.writeObject(wordIndexTable);
      output.writeObject(charIndexTable);
      output.writeObject(wordItem_charArrayTable);
      output.writeObject(wordItem_frequencyTable);
      output.close();
      // log.info("serialize core dict.");
    } catch (Exception e) {
      // log.warn(e.getMessage());
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
  private int loadMainDataFromFile(String dctFilePath)
      throws FileNotFoundException, IOException, UnsupportedEncodingException {
    int i, cnt, length, total = 0;
    // 文件中只统计了6763个汉字加5个空汉字符3756~3760，其中第3756个用来存储符号信息。
    int[] buffer = new int[3];
    byte[] intBuffer = new byte[4];
    String tmpword;
    RandomAccessFile dctFile = new RandomAccessFile(dctFilePath, "r");

    // 字典文件中第一个汉字出现的位置是0，最后一个是6768
    for (i = GB2312_FIRST_CHAR; i < GB2312_FIRST_CHAR + CHAR_NUM_IN_FILE; i++) {
      // if (i == 5231)
      // System.out.println(i);

      dctFile.read(intBuffer);// 原词库文件在c下开发，所以写入的文件为little
      // endian编码，而java为big endian，必须转换过来
      cnt = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (cnt <= 0) {
        wordItem_charArrayTable[i] = null;
        wordItem_frequencyTable[i] = null;
        continue;
      }
      wordItem_charArrayTable[i] = new char[cnt][];
      wordItem_frequencyTable[i] = new int[cnt];
      total += cnt;
      int j = 0;
      while (j < cnt) {
        // wordItemTable[i][j] = new WordItem();
        dctFile.read(intBuffer);
        buffer[0] = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN)
            .getInt();// frequency
        dctFile.read(intBuffer);
        buffer[1] = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN)
            .getInt();// length
        dctFile.read(intBuffer);
        buffer[2] = ByteBuffer.wrap(intBuffer).order(ByteOrder.LITTLE_ENDIAN)
            .getInt();// handle

        // wordItemTable[i][j].frequency = buffer[0];
        wordItem_frequencyTable[i][j] = buffer[0];

        length = buffer[1];
        if (length > 0) {
          byte[] lchBuffer = new byte[length];
          dctFile.read(lchBuffer);
          tmpword = new String(lchBuffer, "GB2312");
          // indexTable[i].wordItems[j].word = tmpword;
          // wordItemTable[i][j].charArray = tmpword.toCharArray();
          wordItem_charArrayTable[i][j] = tmpword.toCharArray();
        } else {
          // wordItemTable[i][j].charArray = null;
          wordItem_charArrayTable[i][j] = null;
        }
        // System.out.println(indexTable[i].wordItems[j]);
        j++;
      }

      String str = getCCByGB2312Id(i);
      setTableIndex(str.charAt(0), i);
    }
    dctFile.close();
    return total;
  }

  /**
   * 原词库将所有标点符号的信息合并到一个列表里(从1开始的3755处)。这里将其展开，分别放到各个符号对应的列表中
   */
  private void expandDelimiterData() {
    int i;
    int cnt;
    // 标点符号在从1开始的3755处，将原始的标点符号对应的字典分配到对应的标点符号中
    int delimiterIndex = 3755 + GB2312_FIRST_CHAR;
    i = 0;
    while (i < wordItem_charArrayTable[delimiterIndex].length) {
      char c = wordItem_charArrayTable[delimiterIndex][i][0];
      int j = getGB2312Id(c);// 该标点符号应该所在的index值
      if (wordItem_charArrayTable[j] == null) {

        int k = i;
        // 从i开始计数后面以j开头的符号的worditem的个数
        while (k < wordItem_charArrayTable[delimiterIndex].length
            && wordItem_charArrayTable[delimiterIndex][k][0] == c) {
          k++;
        }
        // 此时k-i为id为j的标点符号对应的wordItem的个数
        cnt = k - i;
        if (cnt != 0) {
          wordItem_charArrayTable[j] = new char[cnt][];
          wordItem_frequencyTable[j] = new int[cnt];
        }

        // 为每一个wordItem赋值
        for (k = 0; k < cnt; k++, i++) {
          // wordItemTable[j][k] = new WordItem();
          wordItem_frequencyTable[j][k] = wordItem_frequencyTable[delimiterIndex][i];
          wordItem_charArrayTable[j][k] = new char[wordItem_charArrayTable[delimiterIndex][i].length - 1];
          System.arraycopy(wordItem_charArrayTable[delimiterIndex][i], 1,
              wordItem_charArrayTable[j][k], 0,
              wordItem_charArrayTable[j][k].length);
        }
        setTableIndex(c, j);
      }
    }
    // 将原符号对应的数组删除
    wordItem_charArrayTable[delimiterIndex] = null;
    wordItem_frequencyTable[delimiterIndex] = null;
  }

  /**
   * 本程序不做词性标注，因此将相同词不同词性的频率合并到同一个词下，以减小存储空间，加快搜索速度
   */
  private void mergeSameWords() {
    int i;
    for (i = 0; i < GB2312_FIRST_CHAR + CHAR_NUM_IN_FILE; i++) {
      if (wordItem_charArrayTable[i] == null)
        continue;
      int len = 1;
      for (int j = 1; j < wordItem_charArrayTable[i].length; j++) {
        if (Utility.compareArray(wordItem_charArrayTable[i][j], 0,
            wordItem_charArrayTable[i][j - 1], 0) != 0)
          len++;

      }
      if (len < wordItem_charArrayTable[i].length) {
        char[][] tempArray = new char[len][];
        int[] tempFreq = new int[len];
        int k = 0;
        tempArray[0] = wordItem_charArrayTable[i][0];
        tempFreq[0] = wordItem_frequencyTable[i][0];
        for (int j = 1; j < wordItem_charArrayTable[i].length; j++) {
          if (Utility.compareArray(wordItem_charArrayTable[i][j], 0,
              tempArray[k], 0) != 0) {
            k++;
            // temp[k] = wordItemTable[i][j];
            tempArray[k] = wordItem_charArrayTable[i][j];
            tempFreq[k] = wordItem_frequencyTable[i][j];
          } else {
            // temp[k].frequency += wordItemTable[i][j].frequency;
            tempFreq[k] += wordItem_frequencyTable[i][j];
          }
        }
        // wordItemTable[i] = temp;
        wordItem_charArrayTable[i] = tempArray;
        wordItem_frequencyTable[i] = tempFreq;
      }
    }
  }

  private void sortEachItems() {
    char[] tmpArray;
    int tmpFreq;
    for (int i = 0; i < wordItem_charArrayTable.length; i++) {
      if (wordItem_charArrayTable[i] != null
          && wordItem_charArrayTable[i].length > 1) {
        for (int j = 0; j < wordItem_charArrayTable[i].length - 1; j++) {
          for (int j2 = j + 1; j2 < wordItem_charArrayTable[i].length; j2++) {
            if (Utility.compareArray(wordItem_charArrayTable[i][j], 0,
                wordItem_charArrayTable[i][j2], 0) > 0) {
              tmpArray = wordItem_charArrayTable[i][j];
              tmpFreq = wordItem_frequencyTable[i][j];
              wordItem_charArrayTable[i][j] = wordItem_charArrayTable[i][j2];
              wordItem_frequencyTable[i][j] = wordItem_frequencyTable[i][j2];
              wordItem_charArrayTable[i][j2] = tmpArray;
              wordItem_frequencyTable[i][j2] = tmpFreq;
            }
          }
        }
      }
    }
  }

  /**
   * 计算字符c在哈希表中应该在的位置，然后将地址列表中该位置的值初始化
   * 
   * @param c
   * @param j
   * @return
   */
  private boolean setTableIndex(char c, int j) {
    int index = getAvaliableTableIndex(c);
    if (index != -1) {
      charIndexTable[index] = c;
      wordIndexTable[index] = (short) j;
      return true;
    } else
      return false;
  }

  private short getAvaliableTableIndex(char c) {
    int hash1 = (int) (hash1(c) % PRIME_INDEX_LENGTH);
    int hash2 = hash2(c) % PRIME_INDEX_LENGTH;
    if (hash1 < 0)
      hash1 = PRIME_INDEX_LENGTH + hash1;
    if (hash2 < 0)
      hash2 = PRIME_INDEX_LENGTH + hash2;
    int index = hash1;
    int i = 1;
    while (charIndexTable[index] != 0 && charIndexTable[index] != c
        && i < PRIME_INDEX_LENGTH) {
      index = (hash1 + i * hash2) % PRIME_INDEX_LENGTH;
      i++;
    }
    // System.out.println(i - 1);

    if (i < PRIME_INDEX_LENGTH
        && (charIndexTable[index] == 0 || charIndexTable[index] == c)) {
      return (short) index;
    } else
      return -1;
  }

  /**
   * @param c
   * @return
   */
  private short getWordItemTableIndex(char c) {
    int hash1 = (int) (hash1(c) % PRIME_INDEX_LENGTH);
    int hash2 = hash2(c) % PRIME_INDEX_LENGTH;
    if (hash1 < 0)
      hash1 = PRIME_INDEX_LENGTH + hash1;
    if (hash2 < 0)
      hash2 = PRIME_INDEX_LENGTH + hash2;
    int index = hash1;
    int i = 1;
    while (charIndexTable[index] != 0 && charIndexTable[index] != c
        && i < PRIME_INDEX_LENGTH) {
      index = (hash1 + i * hash2) % PRIME_INDEX_LENGTH;
      i++;
    }

    if (i < PRIME_INDEX_LENGTH && charIndexTable[index] == c) {
      return (short) index;
    } else
      return -1;
  }

  /**
   * 在字典库中查找单词对应的char数组为charArray的字符串。返回该单词在单词序列中的位置
   * 
   * @param charArray 查找单词对应的char数组
   * @return 单词在单词数组中的位置，如果没找到则返回-1
   */
  private int findInTable(char[] charArray) {
    if (charArray == null || charArray.length == 0)
      return -1;
    short index = getWordItemTableIndex(charArray[0]);
    if (index == -1)
      return -1;

    return findInTable(index, charArray);

  }

  /**
   * 在字典库中查找单词对应的char数组为charArray的字符串。返回该单词在单词序列中的位置
   * 
   * @param knownHashIndex 已知单词第一个字符charArray[0]在hash表中的位置，如果未计算，可以用函数int
   *        findInTable(char[] charArray) 代替
   * @param charArray 查找单词对应的char数组
   * @return 单词在单词数组中的位置，如果没找到则返回-1
   */
  private int findInTable(short knownHashIndex, char[] charArray) {
    if (charArray == null || charArray.length == 0)
      return -1;

    char[][] items = wordItem_charArrayTable[wordIndexTable[knownHashIndex]];
    int start = 0, end = items.length - 1;
    int mid = (start + end) / 2, cmpResult;

    // Binary search for the index of idArray
    while (start <= end) {
      cmpResult = Utility.compareArray(items[mid], 0, charArray, 1);

      if (cmpResult == 0)
        return mid;// find it
      else if (cmpResult < 0)
        start = mid + 1;
      else if (cmpResult > 0)
        end = mid - 1;

      mid = (start + end) / 2;
    }
    return -1;
  }

  /**
   * charArray这个单词对应的词组在不在WordDictionary中出现
   * 
   * @param charArray
   * @return true表示存在，false表示不存在
   */
  public boolean isExist(char[] charArray) {
    return findInTable(charArray) != -1;
  }

  /**
   * @see{getPrefixMatch(char[] charArray, int knownStart)}
   * @param charArray
   * @return
   */
  public int getPrefixMatch(char[] charArray) {
    return getPrefixMatch(charArray, 0);
  }

  /**
   * 从词典中查找以charArray对应的单词为前缀(prefix)的单词的位置, 并返回第一个满足条件的位置。为了减小搜索代价,
   * 可以根据已有知识设置起始搜索位置, 如果不知道起始位置，默认是0
   * 
   * @see{getPrefixMatch(char[] charArray)}
   * @param charArray 前缀单词
   * @param knownStart 已知的起始位置
   * @return 满足前缀条件的第一个单词的位置
   */
  public int getPrefixMatch(char[] charArray, int knownStart) {
    short index = getWordItemTableIndex(charArray[0]);
    if (index == -1)
      return -1;
    char[][] items = wordItem_charArrayTable[wordIndexTable[index]];
    int start = knownStart, end = items.length - 1;

    int mid = (start + end) / 2, cmpResult;

    // Binary search for the index of idArray
    while (start <= end) {
      cmpResult = Utility.compareArrayByPrefix(charArray, 1, items[mid], 0);
      if (cmpResult == 0) {
        // Get the first item which match the current word
        while (mid >= 0
            && Utility.compareArrayByPrefix(charArray, 1, items[mid], 0) == 0)
          mid--;
        mid++;
        return mid;// 找到第一个以charArray为前缀的单词
      } else if (cmpResult < 0)
        end = mid - 1;
      else
        start = mid + 1;
      mid = (start + end) / 2;
    }
    return -1;
  }

  /**
   * 获取idArray对应的词的词频，若pos为-1则获取所有词性的词频
   * 
   * @param charArray 输入的单词对应的charArray
   * @param pos 词性，-1表示要求求出所有的词性的词频
   * @return idArray对应的词频
   */
  public int getFrequency(char[] charArray) {
    short hashIndex = getWordItemTableIndex(charArray[0]);
    if (hashIndex == -1)
      return 0;
    int itemIndex = findInTable(hashIndex, charArray);
    if (itemIndex != -1)
      return wordItem_frequencyTable[wordIndexTable[hashIndex]][itemIndex];
    return 0;

  }

  /**
   * 判断charArray对应的字符串是否跟词典中charArray[0]对应的wordIndex的charArray相等,
   * 也就是说charArray的位置查找结果是不是就是wordIndex
   * 
   * @param charArray 输入的charArray词组，第一个数表示词典中的索引号
   * @param itemIndex 位置编号
   * @return 是否相等
   */
  public boolean isEqual(char[] charArray, int itemIndex) {
    short hashIndex = getWordItemTableIndex(charArray[0]);
    return Utility.compareArray(charArray, 1,
        wordItem_charArrayTable[wordIndexTable[hashIndex]][itemIndex], 0) == 0;
  }

  public static void main(String[] args) throws FileNotFoundException,
      IOException {
    WordDictionary dic = new WordDictionary();
    dic.load("D:/analysis-data");
    Utility.getCharType('。');
    Utility.getCharType('汗');
    Utility.getCharType(' ');// 0020
    Utility.getCharType('　');// 3000
    Utility.getCharType('');// E095
    Utility.getCharType(' ');// 3000
    Utility.getCharType('\r');// 000D
    Utility.getCharType('\n');// 000A
    Utility.getCharType('\t');// 0009
  }
}
