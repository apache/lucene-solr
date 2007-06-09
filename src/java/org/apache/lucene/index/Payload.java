package org.apache.lucene.index;

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

import java.io.Serializable;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

 /**
  *  A Payload is metadata that can be stored together with each occurrence 
  *  of a term. This metadata is stored inline in the posting list of the
  *  specific term.  
  *  <p>
  *  To store payloads in the index a {@link TokenStream} has to be used that
  *  produces {@link Token}s containing payload data.
  *  <p>
  *  Use {@link TermPositions#getPayloadLength()} and {@link TermPositions#getPayload(byte[], int)}
  *  to retrieve the payloads from the index.<br>
  *  <br>
  *  
  * <p><font color="#FF0000">
  * WARNING: The status of the <b>Payloads</b> feature is experimental. 
  * The APIs introduced here might change in the future and will not be 
  * supported anymore in such a case.</font>
  */    
  // TODO: Remove warning after API has been finalized
  public class Payload implements Serializable {
    /** the byte array containing the payload data */
    protected byte[] data;
    
    /** the offset within the byte array */
    protected int offset;
    
    /** the length of the payload data */
    protected int length;
    
    /** Creates an empty payload and does not allocate a byte array. */
    protected Payload() {
      // no-arg constructor since this class implements Serializable
    }
    
    /**
     * Creates a new payload with the the given array as data.
     * 
     * @param data the data of this payload
     */
    public Payload(byte[] data) {
      this(data, 0, data.length);
    }

    /**
     * Creates a new payload with the the given array as data. 
     * 
     * @param data the data of this payload
     * @param offset the offset in the data byte array
     * @param length the length of the data
     */
    public Payload(byte[] data, int offset, int length) {
      if (offset < 0 || offset + length > data.length) {
        throw new IllegalArgumentException();
      }
      this.data = data;
      this.offset = offset;
      this.length = length;
    }
    
    /**
     * Returns the length of the payload data. 
     */
    public int length() {
      return this.length;
    }
    
    /**
     * Returns the byte at the given index.
     */
    public byte byteAt(int index) {
      if (0 <= index && index < this.length) {
        return this.data[this.offset + index];    
      }
      throw new ArrayIndexOutOfBoundsException(index);
    }
    
    /**
     * Allocates a new byte array, copies the payload data into it and returns it. 
     */
    public byte[] toByteArray() {
      byte[] retArray = new byte[this.length];
      System.arraycopy(this.data, this.offset, retArray, 0, this.length);
      return retArray;
    }
    
    /**
     * Copies the payload data to a byte array.
     * 
     * @param target the target byte array
     * @param targetOffset the offset in the target byte array
     */
    public void copyTo(byte[] target, int targetOffset) {
      if (this.length > target.length + targetOffset) {
        throw new ArrayIndexOutOfBoundsException();
      }
      System.arraycopy(this.data, this.offset, target, targetOffset, this.length);
    }
}
