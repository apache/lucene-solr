package org.apache.lucene.analysis.tokenattributes;

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

import org.apache.lucene.index.Payload;
import org.apache.lucene.util.Attribute;

/**
 * The payload of a Token. See also {@link Payload}.
 * 
 * <p><font color="#FF0000">
 * WARNING: The status of the new TokenStream, AttributeSource and Attributes is experimental. 
 * The APIs introduced in these classes with Lucene 2.9 might change in the future. 
 * We will make our best efforts to keep the APIs backwards-compatible.</font>
 */
public class PayloadAttribute extends Attribute implements Cloneable, Serializable {
  private Payload payload;  
  
  /**
   * Initialize this attribute with no payload.
   */
  public PayloadAttribute() {}
  
  /**
   * Initialize this attribute with the given payload. 
   */
  public PayloadAttribute(Payload payload) {
    this.payload = payload;
  }
  
  /**
   * Returns this Token's payload.
   */ 
  public Payload getPayload() {
    return this.payload;
  }

  /** 
   * Sets this Token's payload.
   */
  public void setPayload(Payload payload) {
    this.payload = payload;
  }
  
  public void clear() {
    payload = null;
  }

  public String toString() {
    if (payload == null) {
      return "payload=null";
    } 
    
    return "payload=" + payload.toString();
  }
  
  public Object clone()  {
    PayloadAttribute clone = (PayloadAttribute) super.clone();
    if (payload != null) {
      clone.payload = (Payload) payload.clone();
    }
    return clone;
  }

  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof PayloadAttribute) {
      PayloadAttribute o = (PayloadAttribute) other;
      if (o.payload == null || payload == null) {
        return o.payload == null && payload == null;
      }
      
      return o.payload.equals(payload);
    }
    
    return false;
  }

  public int hashCode() {
    return (payload == null) ? 0 : payload.hashCode();
  }

  public void copyTo(Attribute target) {
    PayloadAttribute t = (PayloadAttribute) target;
    t.setPayload((payload == null) ? null : (Payload) payload.clone());
  }  

  
}
