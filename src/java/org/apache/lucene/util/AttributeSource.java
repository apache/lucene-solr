package org.apache.lucene.util;

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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenStream;


/**
 * An AttributeSource contains a list of different {@link Attribute}s,
 * and methods to add and get them. There can only be a single instance
 * of an attribute in the same AttributeSource instance. This is ensured
 * by passing in the actual type of the Attribute (Class&lt;Attribute&gt;) to 
 * the {@link #addAttribute(Class)}, which then checks if an instance of
 * that type is already present. If yes, it returns the instance, otherwise
 * it creates a new instance and returns it.
 * 
 * <p><font color="#FF0000">
 * WARNING: The status of the new TokenStream, AttributeSource and Attributes is experimental. 
 * The APIs introduced in these classes with Lucene 2.9 might change in the future. 
 * We will make our best efforts to keep the APIs backwards-compatible.</font>
 */
public class AttributeSource {
  /**
   * An AttributeAcceptor defines only a single method {@link #accept(Class)}.
   * It can be used for e. g. buffering purposes to specify which attributes
   * to buffer. 
   */
  public static abstract class AttributeAcceptor {
    /** Return true, to accept this attribute; false otherwise */
    public abstract boolean accept(Class attClass);
  }
  
  /**
   * Default AttributeAcceptor that accepts all attributes.
   */
  public static final AttributeAcceptor AllAcceptor = new AttributeAcceptor() {
    public boolean accept(Class attClass) {return true;}      
  };

  /**
   * Holds the Class&lt;Attribute&gt; -> Attribute mapping
   */
  protected Map attributes;

  public AttributeSource() {
    this.attributes = new LinkedHashMap();
  }
  
  public AttributeSource(AttributeSource input) {
    if (input == null) {
      throw new IllegalArgumentException("input AttributeSource must not be null");
    }
    this.attributes = input.attributes;
  }
  
  /** Returns an iterator that iterates the attributes 
   * in the same order they were added in.
   */
  public Iterator getAttributesIterator() {
    return attributes.values().iterator();
  }
  
  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value.
   * This method first checks if an instance of that class is 
   * already in this AttributeSource and returns it. Otherwise a
   * new instance is created, added to this AttributeSource and returned. 
   */
  public Attribute addAttribute(Class attClass) {
    Attribute att = (Attribute) attributes.get(attClass);
    if (att == null) {
      try {
        att = (Attribute) attClass.newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Could not instantiate class " + attClass);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Could not instantiate class " + attClass);      
      }
      
      attributes.put(attClass, att);
    }
    return att;
  }
  
  /** Returns true, iff this AttributeSource has any attributes */
  public boolean hasAttributes() {
    return !this.attributes.isEmpty();
  }

  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value. 
   * Returns true, iff this AttributeSource contains the passed-in Attribute.
   */
  public boolean hasAttribute(Class attClass) {
    return this.attributes.containsKey(attClass);
  }

  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value. 
   * Returns the instance of the passed in Attribute contained in this AttributeSource
   * 
   * @throws IllegalArgumentException if this AttributeSource does not contain the
   *         Attribute
   */
  public Attribute getAttribute(Class attClass) {
    Attribute att = (Attribute) this.attributes.get(attClass);
    if (att == null) {
      throw new IllegalArgumentException("This token does not have the attribute '" + attClass + "'.");
    }

    return att;
  }
  
  /**
   * Resets all Attributes in this AttributeSource by calling
   * {@link Attribute#clear()} on each Attribute.
   */
  public void clearAttributes() {
    Iterator it = getAttributesIterator();
    while (it.hasNext()) {
      ((Attribute) it.next()).clear();
    }
  }
  
  /**
   * Captures the current state of the passed in TokenStream.
   * <p>
   * This state will contain all of the passed in TokenStream's
   * {@link Attribute}s. If only a subset of the attributes is needed
   * please use {@link #captureState(AttributeAcceptor)} 
   */
  public AttributeSource captureState() {
    return captureState(AllAcceptor);
  }

  /**
   * Captures the current state of the passed in TokenStream.
   * <p>
   * This state will contain all of the passed in TokenStream's
   * {@link Attribute}s which the {@link AttributeAcceptor} accepts. 
   */
  public AttributeSource captureState(AttributeAcceptor acceptor) {
    AttributeSource state = new AttributeSource();
     
    Iterator it = getAttributesIterator();
    while(it.hasNext()) {
      Attribute att = (Attribute) it.next();
      if (acceptor.accept(att.getClass())) {
        Attribute clone = (Attribute) att.clone();
        state.attributes.put(att.getClass(), clone);
      }
    }
    
    return state;
  }
  
  /**
   * Restores this state by copying the values of all attributes 
   * that this state contains into the attributes of the targetStream.
   * The targetStream must contain a corresponding instance for each argument
   * contained in this state.
   * <p>
   * Note that this method does not affect attributes of the targetStream
   * that are not contained in this state. In other words, if for example
   * the targetStream contains an OffsetAttribute, but this state doesn't, then
   * the value of the OffsetAttribute remains unchanged. It might be desirable to
   * reset its value to the default, in which case the caller should first
   * call {@link TokenStream#clearAttributes()} on the targetStream.   
   */
  public void restoreState(AttributeSource target) {
    Iterator it = getAttributesIterator();
    while (it.hasNext()) {
      Attribute att = (Attribute) it.next();
      Attribute targetAtt = target.getAttribute(att.getClass());
      att.copyTo(targetAtt);
    }
  }
  
  public int hashCode() {
    int code = 0;
    if (hasAttributes()) {
      Iterator it = getAttributesIterator();
      while (it.hasNext()) {
        code = code * 31 + it.next().hashCode();
      }
    }
    
    return code;
  }
  
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof AttributeSource) {
      AttributeSource other = (AttributeSource) obj;  
    
      if (hasAttributes()) {
        if (!other.hasAttributes()) {
          return false;
        }
        
        if (attributes.size() != other.attributes.size()) {
          return false;
        }
  
        Iterator it = getAttributesIterator();
        while (it.hasNext()) {
          Class attName = it.next().getClass();
          
          Attribute otherAtt = (Attribute) other.attributes.get(attName);
          if (otherAtt == null || !otherAtt.equals(attributes.get(attName))) {
            return false;
          }
        }
        return true;
      } else {
        return !other.hasAttributes();
      }
    } else
      return false;
  }

  
// TODO: Java 1.5
//  private Map<Class<? extends Attribute>, Attribute> attributes;  
//  public <T extends Attribute> T addAttribute(Class<T> attClass) {
//    T att = (T) attributes.get(attClass);
//    if (att == null) {
//      try {
//        att = attClass.newInstance();
//      } catch (InstantiationException e) {
//        throw new IllegalArgumentException("Could not instantiate class " + attClass);
//      } catch (IllegalAccessException e) {
//        throw new IllegalArgumentException("Could not instantiate class " + attClass);      
//      }
//      
//      attributes.put(attClass, att);
//    }
//    return att;
//  }
//
//  public boolean hasAttribute(Class<? extends Attribute> attClass) {
//    return this.attributes.containsKey(attClass);
//  }
//
//  public <T extends Attribute> T getAttribute(Class<T> attClass) {
//    Attribute att = this.attributes.get(attClass);
//    if (att == null) {
//      throw new IllegalArgumentException("This token does not have the attribute '" + attClass + "'.");
//    }
//
//    return (T) att;
//  }
//

}
