package org.apache.lucene.analysis.kuromoji.dict;

/**
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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.lucene.analysis.kuromoji.util.CSVUtil;

public class TokenInfoDictionary implements Dictionary{
  
  public static final String FILENAME = "tid.dat";
  
  public static final String TARGETMAP_FILENAME = "tid_map.dat";
  
  protected ByteBuffer buffer;
  
  protected int[][] targetMap;
  
  public TokenInfoDictionary() {
  }
  
  public TokenInfoDictionary(int size) {
    targetMap = new int[1][];
    buffer = ByteBuffer.allocate(size);
  }
  
  /**
   * put the entry in map
   * @return current position of buffer, which will be wordId of next entry
   */
  public int put(String[] entry) {
    short leftId = Short.parseShort(entry[1]);
    short rightId = Short.parseShort(entry[2]);
    short wordCost = Short.parseShort(entry[3]);
    
    StringBuilder sb = new StringBuilder();
    for (int i = 4; i < entry.length; i++){
      sb.append(entry[i]).append(INTERNAL_SEPARATOR);
    }
    String features = sb.deleteCharAt(sb.length() - 1).toString();
    int featuresSize = features.length()* 2;
    
    // extend buffer if necessary
    int left = buffer.limit() - buffer.position();
    if (8 + featuresSize > left) { // four short and features
      ByteBuffer newBuffer = ByteBuffer.allocate(buffer.limit() * 2);
      buffer.flip();
      newBuffer.put(buffer);
      buffer = newBuffer;
    }
    
    buffer.putShort(leftId);
    buffer.putShort(rightId);
    buffer.putShort(wordCost);
    buffer.putShort((short)featuresSize);
    for (char c : features.toCharArray()){
      buffer.putChar(c);
    }
    
    return buffer.position();
  }
  
  public void addMapping(int sourceId, int wordId) {
    if(targetMap.length <= sourceId) {
      int[][] newArray = new int[sourceId + 1][];
      System.arraycopy(targetMap, 0, newArray, 0, targetMap.length);
      targetMap = newArray;
    }
    
    // Prepare array -- extend the length of array by one
    int[] current = targetMap[sourceId];
    if (current == null) {
      current = new int[1];
    } else {
      int[] newArray = new int[current.length + 1];
      System.arraycopy(current, 0, newArray, 0, current.length);
      current = newArray;
    }
    targetMap[sourceId] = current;
    
    int[] targets = targetMap[sourceId];
    targets[targets.length - 1] = wordId;
  }
  
  public int[] lookupWordIds(int sourceId) {
    return targetMap[sourceId];
  }
  
  @Override	
  public int getLeftId(int wordId) {
    return buffer.getShort(wordId);
  }
  
  @Override
  public int getRightId(int wordId) {
    return buffer.getShort(wordId + 2);	// Skip left id
  }
  
  @Override
  public int getWordCost(int wordId) {
    return buffer.getShort(wordId + 4);	// Skip left id and right id
  }
  
  @Override
  public String[] getAllFeaturesArray(int wordId) {
    int size = buffer.getShort(wordId + 6) / 2; // Read length of feature String. Skip 6 bytes, see data structure.
    char[] targetArr = new char[size];
    int offset = wordId + 6 + 2; // offset is position where features string starts
    for(int i = 0; i < size; i++){
      targetArr[i] = buffer.getChar(offset + i * 2);
    }
    String allFeatures = new String(targetArr);
    return allFeatures.split(INTERNAL_SEPARATOR);
  }
  
  @Override
  public String getFeature(int wordId, int... fields) {
    String[] allFeatures = getAllFeaturesArray(wordId);
    StringBuilder sb = new StringBuilder();
    
    if(fields.length == 0){ // All features
      for(String feature : allFeatures) {
        sb.append(CSVUtil.quoteEscape(feature)).append(",");
      }
    } else if(fields.length == 1) { // One feature doesn't need to escape value
      sb.append(allFeatures[fields[0]]).append(",");			
    } else {
      for(int field : fields){
        sb.append(CSVUtil.quoteEscape(allFeatures[field])).append(",");
      }
    }
    
    return sb.deleteCharAt(sb.length() - 1).toString();
  }
  
  @Override
  public String getReading(int wordId) {
    return getFeature(wordId, 7);
  }
  
  @Override
  public String getAllFeatures(int wordId) {
    return getFeature(wordId);
  }
  
  @Override
  public String getPartOfSpeech(int wordId) {
    return getFeature(wordId, 0, 1, 2, 3);
  }
  
  @Override
  public String getBaseForm(int wordId) {
    String form = getFeature(wordId, 6);
    return "*".equals(form) ? null : form;
  }

  /**
   * Write dictionary in file
   * Dictionary format is:
   * [Size of dictionary(int)], [entry:{left id(short)}{right id(short)}{word cost(short)}{length of pos info(short)}{pos info(char)}], [entry...], [entry...].....
   * @throws IOException
   */
  public void write(String directoryname) throws IOException {
    writeDictionary(directoryname + File.separator + FILENAME);
    writeTargetMap(directoryname + File.separator + TARGETMAP_FILENAME);
  }
  
  protected void writeTargetMap(String filename) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));		
    oos.writeObject(targetMap);
    oos.close();
  }
  
  protected void writeDictionary(String filename) throws IOException {
    FileOutputStream fos = new FileOutputStream(filename);
    DataOutputStream dos = new DataOutputStream(fos);
    dos.writeInt(buffer.position());
    WritableByteChannel channel = Channels.newChannel(fos);
    // Write Buffer
    buffer.flip();  // set position to 0, set limit to current position
    channel.write(buffer);
    
    fos.close();
  }
  
  /**
   * Read dictionary into directly allocated buffer.
   * @return TokenInfoDictionary instance
   * @throws IOException
   * @throws ClassNotFoundException 
   */
  public static TokenInfoDictionary getInstance() throws IOException, ClassNotFoundException {
    TokenInfoDictionary dictionary = new TokenInfoDictionary();
    ClassLoader loader = dictionary.getClass().getClassLoader();
    dictionary.loadDictionary(loader.getResourceAsStream(FILENAME));
    dictionary.loadTargetMap(loader.getResourceAsStream(TARGETMAP_FILENAME));
    return dictionary;
  }
  
  protected void loadTargetMap(InputStream is) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(is));
    targetMap = (int[][]) ois.readObject();
    is.close();
  }
  
  protected void loadDictionary(InputStream is) throws IOException {
    DataInputStream dis = new DataInputStream(is);
    int size = dis.readInt();
    
    ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(size);
    
    ReadableByteChannel channel = Channels.newChannel(is);
    channel.read(tmpBuffer);
    is.close();
    buffer = tmpBuffer.asReadOnlyBuffer();
  }
  
}
