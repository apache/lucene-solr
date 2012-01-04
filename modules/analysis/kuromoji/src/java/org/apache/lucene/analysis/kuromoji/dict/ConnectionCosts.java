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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ConnectionCosts implements Serializable{
  
  private static final long serialVersionUID = -7704592689635266457L;
  
  public static final String FILENAME = "cc.dat";
  
  private short[][] costs; // array is backward IDs first since get is called using the same backward ID consecutively. maybe doesn't matter.
  
  public ConnectionCosts() {
    
  }
  
  public ConnectionCosts(int forwardSize, int backwardSize) {
    this.costs = new short[backwardSize][forwardSize]; 
  }
  
  public void add(int forwardId, int backwardId, int cost) {
    this.costs[backwardId][forwardId] = (short)cost;
  }
  
  public int get(int forwardId, int backwardId) {
    // FIXME: There seems to be something wrong with the double array trie in some rare
    // cases causing and IndexOutOfBoundsException.  Use a guard as a temporary work-around
    // and return a high cost to advise Mr. Viterbi strongly to not use this transition
    if (backwardId < costs.length && forwardId < costs[backwardId].length ) {
      return costs[backwardId][forwardId];
    } else {
      return 50000;
    }
  }
  
  public void write(String directoryname) throws IOException {
    String filename = directoryname + File.separator + FILENAME;
    ObjectOutputStream outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
    outputStream.writeObject(this);
    outputStream.close();
  }
  
  public static ConnectionCosts getInstance() throws IOException, ClassNotFoundException {
    InputStream is = ConnectionCosts.class.getResourceAsStream(FILENAME);
    return read(is);
  }
  
  public static ConnectionCosts read(InputStream is) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(is));
    ConnectionCosts instance = (ConnectionCosts) ois.readObject();
    ois.close();
    return instance;
  }
  
}
