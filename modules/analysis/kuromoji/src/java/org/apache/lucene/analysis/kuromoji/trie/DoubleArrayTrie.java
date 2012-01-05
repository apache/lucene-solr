package org.apache.lucene.analysis.kuromoji.trie;

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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.lucene.analysis.kuromoji.trie.Trie.Node;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.CodecUtil;

public class DoubleArrayTrie {
  
  public static final String FILENAME = "dat.dat";
  public static final String HEADER = "kuromoji_double_arr_trie";
  public static final int VERSION = 1;
  
  public static final char TERMINATING_CHARACTER = '\u0001';
  
  private static final int BASE_CHECK_INITILAL_SIZE = 1000000;
  
  private static final int TAIL_INITIAL_SIZE = 10000;
  
  private static final int TAIL_OFFSET = 10000000;
  
  private IntBuffer baseBuffer;
  
  private IntBuffer checkBuffer;
  
  private CharBuffer tailBuffer;
  
  private int tailIndex = TAIL_OFFSET;
  
  
  public DoubleArrayTrie(){
  }
  
  /**
   * Write to file
   * @throws IOException
   */
  public void write(String directoryname) throws IOException  {
    String filename = directoryname + File.separator + FILENAME;
    
    baseBuffer.rewind();
    checkBuffer.rewind();
    tailBuffer.rewind();
    
    final FileOutputStream os = new FileOutputStream(filename);
    try {
      final DataOutput out = new OutputStreamDataOutput(os);
      CodecUtil.writeHeader(out, HEADER, VERSION);
      out.writeVInt(baseBuffer.capacity());
      out.writeVInt(tailBuffer.capacity());
      final WritableByteChannel channel = Channels.newChannel(os);
      
      ByteBuffer tmpBuffer = ByteBuffer.allocate(baseBuffer.capacity() * 4);
      IntBuffer tmpIntBuffer = tmpBuffer.asIntBuffer();
      tmpIntBuffer.put(baseBuffer);
      tmpBuffer.rewind();
      channel.write(tmpBuffer);
      assert tmpBuffer.remaining() == 0L;
      
      tmpBuffer = ByteBuffer.allocate(checkBuffer.capacity() * 4);
      tmpIntBuffer = tmpBuffer.asIntBuffer();
      tmpIntBuffer.put(checkBuffer);
      tmpBuffer.rewind();
      channel.write(tmpBuffer);
      assert tmpBuffer.remaining() == 0L;
      
      tmpBuffer = ByteBuffer.allocate(tailBuffer.capacity() * 2);
      CharBuffer tmpCharBuffer = tmpBuffer.asCharBuffer();
      tmpCharBuffer.put(tailBuffer);
      tmpBuffer.rewind();
      channel.write(tmpBuffer);
      assert tmpBuffer.remaining() == 0L;
    } finally {
      os.close();
    }
  }
  
  public static DoubleArrayTrie getInstance() throws IOException {
    InputStream is = DoubleArrayTrie.class.getResourceAsStream(FILENAME);
    return read(is);
  }
  
  /**
   * Load Stored data
   * @throws IOException
   */
  public static DoubleArrayTrie read(InputStream is) throws IOException {
    is = new BufferedInputStream(is);
    try {
      final DataInput in = new InputStreamDataInput(is);
      CodecUtil.checkHeader(in, HEADER, VERSION, VERSION);
      int baseCheckSize = in.readVInt();	// Read size of baseArr and checkArr
      int tailSize = in.readVInt();		// Read size of tailArr
      
      ReadableByteChannel channel = Channels.newChannel(is);
      
      DoubleArrayTrie trie = new DoubleArrayTrie();

      int toRead, read;
      ByteBuffer tmpBaseBuffer = ByteBuffer.allocateDirect(toRead = baseCheckSize * 4);	// The size is 4 times the baseCheckSize since it is the length of array
      read = channel.read(tmpBaseBuffer);
      if (read != toRead) {
        throw new EOFException("Cannot read DoubleArrayTree");
      }
      tmpBaseBuffer.rewind();
      trie.baseBuffer = tmpBaseBuffer.asIntBuffer().asReadOnlyBuffer();
      
      ByteBuffer tmpCheckBuffer = ByteBuffer.allocateDirect(toRead = baseCheckSize * 4);
      read = channel.read(tmpCheckBuffer);
      if (read != toRead) {
        throw new EOFException("Cannot read DoubleArrayTree");
      }
      tmpCheckBuffer.rewind();
      trie.checkBuffer = tmpCheckBuffer.asIntBuffer().asReadOnlyBuffer();
      
      ByteBuffer tmpTailBuffer = ByteBuffer.allocateDirect(toRead = tailSize * 2);			// The size is 2 times the tailSize since it is the length of array
      read = channel.read(tmpTailBuffer);
      if (read != toRead) {
        throw new EOFException("Cannot read DoubleArrayTree");
      }
      tmpTailBuffer.rewind();
      trie.tailBuffer = tmpTailBuffer.asCharBuffer().asReadOnlyBuffer();
      
      return trie;
    } finally {
      is.close();
    }
  }
  
  /**
   * Construct double array trie which is equivalent to input trie
   * @param trie normal trie which contains all dictionary words
   */
  public void build(Trie trie) {
    baseBuffer = ByteBuffer.allocate(BASE_CHECK_INITILAL_SIZE * 4).asIntBuffer();
    baseBuffer.put(0, 1);
    checkBuffer = ByteBuffer.allocate(BASE_CHECK_INITILAL_SIZE * 4).asIntBuffer();
    tailBuffer = ByteBuffer.allocate(TAIL_INITIAL_SIZE * 2).asCharBuffer();
    add(-1, 0, trie.getRoot());
  }
  
  /**
   * Add Node(character) to double array trie
   * @param previous
   * @param index
   * @param node
   */
  private void add(int previous, int index, Node node) {
    Node[] children = node.getChildren();	// nodes following current node
    
    if(node.getChildren().length > 0 && node.hasSinglePath() && node.getChildren()[0].getKey() != TERMINATING_CHARACTER) {	// If node has only one path, put the rest in tail array
      baseBuffer.put(index, tailIndex);	// current index of tail array
      addToTail(node.children[0]);
      checkBuffer.put(index, previous);
      return;	// No more child to process
    }
    
    int base = findBase(index, children);	// Get base value for current index
    baseBuffer.put(index, base);
    
    if(previous >= 0){
      checkBuffer.put(index, previous);	// Set check value
    }
    
    for(Trie.Node child : children) {	// For each child to double array trie
      add(index, index + base + child.getKey(), child);
    }
    
  }
  
  /**
   * Match input keyword.
   * @param key key to match
   * @return index value of last character in baseBuffer(double array id) if it is complete match. Negative value if it doesn't match. 0 if it is prefix match.
   */
  public int lookup(String key) {
    int index = 0;
    int base = 1; // base at index 0 should be 1
    
    int keyLength = key.length();
    for(int i = 0; i < keyLength; i++) {
      int previous = index;
      index = index + base + key.charAt(i);
      
      if(index > baseBuffer.limit()) { // Too long
        return -1;
      }
      
      base = baseBuffer.get(index);
      
      if (base == 0 ) { // Didn't find match
        return -1;
      }
      
      if(checkBuffer.get(index) != previous){	// check doesn't match
        return -1;
      }
      
      if(base >= TAIL_OFFSET) {	// If base is bigger than TAIL_OFFSET, start processing "tail"
        return matchTail(base, index, key.substring(i + 1));
      }
      
    }
    
    // If we reach at the end of input keyword, check if it is complete match by looking for following terminating character		
    int endIndex = index + base + TERMINATING_CHARACTER;
    
    return checkBuffer.get(endIndex) == index ? index : 0;
  }
  
  /**
   * Check match in tail array
   * @param base
   * @param index
   * @param key
   * @return	index if it is complete match. 0 if it is prefix match. negative value if it doesn't match
   */
  private int matchTail(int base, int index, String key) {
    int positionInTailArr = base - TAIL_OFFSET;
    
    int keyLength = key.length();
    for(int i = 0; i < keyLength; i++) {
      if(key.charAt(i) != tailBuffer.get(positionInTailArr + i)){
        return -1;
      }
    }
    return tailBuffer.get(positionInTailArr + keyLength) == TERMINATING_CHARACTER ? index : 0;
    
  }
  
  /**
   * Find base value for current node, which contains input nodes. They are children of current node.
   * Set default base value , which is one, at the index of each input node.
   * @param index
   * @param nodes
   * @return	base value for current node
   */
  private int findBase(int index, Node[] nodes){
    int base = baseBuffer.get(index);
    if(base < 0) {
      return base;
    }
    
    while(true) {
      boolean collision = false;	// already taken?
      for(Node node : nodes) {
        /*
         * NOTE:
         * Originally, nextIndex is base + node.getKey(). But to reduce construction time, we use index + base + node.getKey().
         * However, this makes array bigger. If there is a need to compat the file dat.dat, it's possbile to modify here and there.
         * Although the size of jar file doesn't change, memory consumption will be smaller.
         */
        int nextIndex = index + base + node.getKey();
        
        if(baseBuffer.capacity() <= nextIndex) {
          int newLength = nextIndex + 1;
          IntBuffer newBaseBuffer = ByteBuffer.allocate(newLength * 4).asIntBuffer();
          baseBuffer.rewind();
          newBaseBuffer.put(baseBuffer);
          baseBuffer = newBaseBuffer;
          IntBuffer newCheckBuffer = ByteBuffer.allocate(newLength * 4).asIntBuffer();
          checkBuffer.rewind();
          newCheckBuffer.put(checkBuffer);
          checkBuffer = newCheckBuffer;
        }
        
        if(baseBuffer.get(nextIndex) != 0) {	// already taken
          base++;	// check next base value
          collision = true;
          break;
        }
      }
      
      if(!collision){
        break;	// if there is no collision, found proper base value. Break the while loop.
      }
      
    }
    
    for(Node node : nodes) {
      baseBuffer.put(index + base + node.getKey(), node.getKey() == TERMINATING_CHARACTER ? -1 : 1);	// Set -1 if key is terminating character. Set default base value 1 if not.
    }
    
    return base;
  }
  
  /**
   * Add characters(nodes) to tail array
   * @param node
   */
  private void addToTail(Node node) {
    while(true) {
      if(tailBuffer.capacity() < tailIndex - TAIL_OFFSET + 1){
        CharBuffer newTailBuffer = ByteBuffer.allocate((tailBuffer.capacity() + TAIL_INITIAL_SIZE / 100) * 2).asCharBuffer();
        tailBuffer.rewind();
        newTailBuffer.put(tailBuffer);
        tailBuffer = newTailBuffer;
      }
      tailBuffer.put(tailIndex++ - TAIL_OFFSET, node.getKey());// set character of current node
      
      if(node.getChildren().length == 0) {	// if it reached the end of input, break.
        break;
      }
      node = node.getChildren()[0];	// Move to next node
    }
  }
}
