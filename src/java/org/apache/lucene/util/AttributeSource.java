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

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.WeakHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.analysis.TokenStream; // for javadocs

/**
 * An AttributeSource contains a list of different {@link AttributeImpl}s,
 * and methods to add and get them. There can only be a single instance
 * of an attribute in the same AttributeSource instance. This is ensured
 * by passing in the actual type of the Attribute (Class&lt;Attribute&gt;) to 
 * the {@link #addAttribute(Class)}, which then checks if an instance of
 * that type is already present. If yes, it returns the instance, otherwise
 * it creates a new instance and returns it.
 */
public class AttributeSource {
  /**
   * An AttributeFactory creates instances of {@link AttributeImpl}s.
   */
  public static abstract class AttributeFactory {
    /**
     * returns an {@link AttributeImpl} for the supplied {@link Attribute} interface class.
     * <p>Signature for Java 1.5: <code>public AttributeImpl createAttributeInstance(Class%lt;? extends Attribute&gt; attClass)</code>
     */
    public abstract AttributeImpl createAttributeInstance(Class attClass);
    
    /**
     * This is the default factory that creates {@link AttributeImpl}s using the
     * class name of the supplied {@link Attribute} interface class by appending <code>Impl</code> to it.
     */
    public static final AttributeFactory DEFAULT_ATTRIBUTE_FACTORY = new DefaultAttributeFactory();
    
    private static final class DefaultAttributeFactory extends AttributeFactory {
      private static final WeakHashMap/*<Class<? extends Attribute>, WeakReference<Class<? extends AttributeImpl>>>*/ attClassImplMap = new WeakHashMap();
      
      private DefaultAttributeFactory() {}
    
      public AttributeImpl createAttributeInstance(Class attClass) {
        try {
          return (AttributeImpl) getClassForInterface(attClass).newInstance();
        } catch (InstantiationException e) {
          throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
        }
      }
      
      private static Class getClassForInterface(Class attClass) {
        synchronized(attClassImplMap) {
          final WeakReference ref = (WeakReference) attClassImplMap.get(attClass);
          Class clazz = (ref == null) ? null : ((Class) ref.get());
          if (clazz == null) {
            try {
              attClassImplMap.put(attClass, new WeakReference(
                clazz = Class.forName(attClass.getName() + "Impl", true, attClass.getClassLoader())
              ));
            } catch (ClassNotFoundException e) {
              throw new IllegalArgumentException("Could not find implementing class for " + attClass.getName());
            }
          }
          return clazz;
        }
      }
    }
  }
      
  // These two maps must always be in sync!!!
  // So they are private, final and read-only from the outside (read-only iterators)
  private final Map/*<Class<Attribute>,AttributeImpl>*/ attributes;
  private final Map/*<Class<AttributeImpl>,AttributeImpl>*/ attributeImpls;

  private AttributeFactory factory;
  
  /**
   * An AttributeSource using the default attribute factory {@link AttributeSource.AttributeFactory#DEFAULT_ATTRIBUTE_FACTORY}.
   */
  public AttributeSource() {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
  }
  
  /**
   * An AttributeSource that uses the same attributes as the supplied one.
   */
  public AttributeSource(AttributeSource input) {
    if (input == null) {
      throw new IllegalArgumentException("input AttributeSource must not be null");
    }
    this.attributes = input.attributes;
    this.attributeImpls = input.attributeImpls;
    this.factory = input.factory;
  }
  
  /**
   * An AttributeSource using the supplied {@link AttributeFactory} for creating new {@link Attribute} instances.
   */
  public AttributeSource(AttributeFactory factory) {
    this.attributes = new LinkedHashMap();
    this.attributeImpls = new LinkedHashMap();
    this.factory = factory;
  }
  
  /**
   * returns the used AttributeFactory.
   */
  public AttributeFactory getAttributeFactory() {
    return this.factory;
  }
  
  /** Returns a new iterator that iterates the attribute classes
   * in the same order they were added in.
   * <p>Signature for Java 1.5: <code>public Iterator&lt;Class&lt;? extends Attribute&gt;&gt; getAttributeClassesIterator()</code>
   */
  public Iterator getAttributeClassesIterator() {
    return Collections.unmodifiableSet(attributes.keySet()).iterator();
  }
  
  /** Returns a new iterator that iterates all unique Attribute implementations.
   * This iterator may contain less entries that {@link #getAttributeClassesIterator},
   * if one instance implements more than one Attribute interface.
   * <p>Signature for Java 1.5: <code>public Iterator&lt;AttributeImpl&gt; getAttributeImplsIterator()</code>
   */
  public Iterator getAttributeImplsIterator() {
    if (hasAttributes()) {
      if (currentState == null) {
        computeCurrentState();
      }
      final State initState = currentState;
      return new Iterator() {
        private State state = initState;
      
        public void remove() {
          throw new UnsupportedOperationException();
        }
        
        public Object next() {
          if (state == null)
            throw new NoSuchElementException();
          final AttributeImpl att = state.attribute;
          state = state.next;
          return att;
        }
        
        public boolean hasNext() {
          return state != null;
        }
      };
    } else {
      return Collections.EMPTY_SET.iterator();
    }
  }
  
  /** a cache that stores all interfaces for known implementation classes for performance (slow reflection) */
  private static final WeakHashMap/*<Class<? extends AttributeImpl>,LinkedList<WeakReference<Class<? extends Attribute>>>>*/
    knownImplClasses = new WeakHashMap();
  
  /** Adds a custom AttributeImpl instance with one or more Attribute interfaces. */
  public void addAttributeImpl(final AttributeImpl att) {
    final Class clazz = att.getClass();
    if (attributeImpls.containsKey(clazz)) return;
    LinkedList foundInterfaces;
    synchronized(knownImplClasses) {
      foundInterfaces = (LinkedList) knownImplClasses.get(clazz);
      if (foundInterfaces == null) {
        // we have a strong reference to the class instance holding all interfaces in the list (parameter "att"),
        // so all WeakReferences are never evicted by GC
        knownImplClasses.put(clazz, foundInterfaces=new LinkedList());
        // find all interfaces that this attribute instance implements
        // and that extend the Attribute interface
        Class actClazz = clazz;
        do {
          Class[] interfaces = actClazz.getInterfaces();
          for (int i = 0; i < interfaces.length; i++) {
            final Class curInterface = interfaces[i];
            if (curInterface != Attribute.class && Attribute.class.isAssignableFrom(curInterface)) {
              foundInterfaces.add(new WeakReference(curInterface));
            }
          }
          actClazz = actClazz.getSuperclass();
        } while (actClazz != null);
      }
    }
    
    // add all interfaces of this AttributeImpl to the maps
    for (Iterator it = foundInterfaces.iterator(); it.hasNext(); ) {
      final WeakReference curInterfaceRef = (WeakReference) it.next();
      final Class curInterface = (Class) curInterfaceRef.get();
      assert (curInterface != null) :
        "We have a strong reference on the class holding the interfaces, so they should never get evicted";
      // Attribute is a superclass of this interface
      if (!attributes.containsKey(curInterface)) {
        // invalidate state to force recomputation in captureState()
        this.currentState = null;
        attributes.put(curInterface, att);
        attributeImpls.put(clazz, att);
      }
    }
  }
  
  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value.
   * This method first checks if an instance of that class is 
   * already in this AttributeSource and returns it. Otherwise a
   * new instance is created, added to this AttributeSource and returned. 
   * <p>Signature for Java 1.5: <code>public &lt;T extends Attribute&gt; T addAttribute(Class&lt;T&gt;)</code>
   */
  public Attribute addAttribute(Class attClass) {
    final Attribute att = (Attribute) attributes.get(attClass);
    if (att == null) {
      if (!(attClass.isInterface() && Attribute.class.isAssignableFrom(attClass))) {
        throw new IllegalArgumentException(
          "addAttribute() only accepts an interface that extends Attribute, but " +
          attClass.getName() + " does not fulfil this contract."
        );
      }
      final AttributeImpl attImpl = this.factory.createAttributeInstance(attClass);
      addAttributeImpl(attImpl);
      return attImpl;
    } else {
      return att;
    }
  }
  
  /** Returns true, iff this AttributeSource has any attributes */
  public boolean hasAttributes() {
    return !this.attributes.isEmpty();
  }

  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value. 
   * Returns true, iff this AttributeSource contains the passed-in Attribute.
   * <p>Signature for Java 1.5: <code>public boolean hasAttribute(Class&lt;? extends Attribute&gt;)</code>
   */
  public boolean hasAttribute(Class attClass) {
    return this.attributes.containsKey(attClass);
  }

  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value. 
   * Returns the instance of the passed in Attribute contained in this AttributeSource
   * <p>Signature for Java 1.5: <code>public &lt;T extends Attribute&gt; T getAttribute(Class&lt;T&gt;)</code>
   * 
   * @throws IllegalArgumentException if this AttributeSource does not contain the
   *         Attribute. It is recommended to always use {@link #addAttribute} even in consumers
   *         of TokenStreams, because you cannot know if a specific TokenStream really uses
   *         a specific Attribute. {@link #addAttribute} will automatically make the attribute
   *         available. If you want to only use the attribute, if it is available (to optimize
   *         consuming), use {@link #hasAttribute}.
   */
  public Attribute getAttribute(Class attClass) {
    final Attribute att = (Attribute) this.attributes.get(attClass);
    if (att == null) {
      throw new IllegalArgumentException("This AttributeSource does not have the attribute '" + attClass.getName() + "'.");
    }

    return att;
  }
  
  /**
   * This class holds the state of an AttributeSource.
   * @see #captureState
   * @see #restoreState
   */
  public static final class State implements Cloneable {
    private AttributeImpl attribute;
    private State next;
    
    public Object clone() {
      State clone = new State();
      clone.attribute = (AttributeImpl) attribute.clone();
      
      if (next != null) {
        clone.next = (State) next.clone();
      }
      
      return clone;
    }
  }
  
  private State currentState = null;
  
  private void computeCurrentState() {
    currentState = new State();
    State c = currentState;
    Iterator it = attributeImpls.values().iterator();
    c.attribute = (AttributeImpl) it.next();
    while (it.hasNext()) {
      c.next = new State();
      c = c.next;
      c.attribute = (AttributeImpl) it.next();
    }        
  }
  
  /**
   * Resets all Attributes in this AttributeSource by calling
   * {@link AttributeImpl#clear()} on each Attribute implementation.
   */
  public void clearAttributes() {
    if (hasAttributes()) {
      if (currentState == null) {
        computeCurrentState();
      }
      for (State state = currentState; state != null; state = state.next) {
        state.attribute.clear();
      }
    }
  }
  
  /**
   * Captures the state of all Attributes. The return value can be passed to
   * {@link #restoreState} to restore the state of this or another AttributeSource.
   */
  public State captureState() {
    if (!hasAttributes()) {
      return null;
    }
      
    if (currentState == null) {
      computeCurrentState();
    }
    return (State) this.currentState.clone();
  }
  
  /**
   * Restores this state by copying the values of all attribute implementations
   * that this state contains into the attributes implementations of the targetStream.
   * The targetStream must contain a corresponding instance for each argument
   * contained in this state (e.g. it is not possible to restore the state of
   * an AttributeSource containing a TermAttribute into a AttributeSource using
   * a Token instance as implementation).
   * <p>
   * Note that this method does not affect attributes of the targetStream
   * that are not contained in this state. In other words, if for example
   * the targetStream contains an OffsetAttribute, but this state doesn't, then
   * the value of the OffsetAttribute remains unchanged. It might be desirable to
   * reset its value to the default, in which case the caller should first
   * call {@link TokenStream#clearAttributes()} on the targetStream.   
   */
  public void restoreState(State state) {
    if (state == null)  return;
    
    do {
      AttributeImpl targetImpl = (AttributeImpl) attributeImpls.get(state.attribute.getClass());
      if (targetImpl == null)
        throw new IllegalArgumentException("State contains an AttributeImpl that is not in this AttributeSource");
      state.attribute.copyTo(targetImpl);
      state = state.next;
    } while (state != null);
  }

  public int hashCode() {
    int code = 0;
    if (hasAttributes()) {
      if (currentState == null) {
        computeCurrentState();
      }
      for (State state = currentState; state != null; state = state.next) {
        code = code * 31 + state.attribute.hashCode();
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
        
        if (this.attributeImpls.size() != other.attributeImpls.size()) {
          return false;
        }
  
        // it is only equal if all attribute impls are the same in the same order
        if (this.currentState == null) {
          this.computeCurrentState();
        }
        State thisState = this.currentState;
        if (other.currentState == null) {
          other.computeCurrentState();
        }
        State otherState = other.currentState;
        while (thisState != null && otherState != null) {
          if (otherState.attribute.getClass() != thisState.attribute.getClass() || !otherState.attribute.equals(thisState.attribute)) {
            return false;
          }
          thisState = thisState.next;
          otherState = otherState.next;
        }
        return true;
      } else {
        return !other.hasAttributes();
      }
    } else
      return false;
  }
  
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append('(');
    
    if (hasAttributes()) {
      if (currentState == null) {
        computeCurrentState();
      }
      for (State state = currentState; state != null; state = state.next) {
        if (state != currentState) sb.append(',');
        sb.append(state.attribute.toString());
      }
    }
    sb.append(')');
    return sb.toString();
  }
  
  /**
   * Performs a clone of all {@link AttributeImpl} instances returned in a new
   * AttributeSource instance. This method can be used to e.g. create another TokenStream
   * with exactly the same attributes (using {@link #AttributeSource(AttributeSource)})
   */
  public AttributeSource cloneAttributes() {
    AttributeSource clone = new AttributeSource(this.factory);
    
    // first clone the impls
    if (hasAttributes()) {
      if (currentState == null) {
        computeCurrentState();
      }
      for (State state = currentState; state != null; state = state.next) {
        clone.attributeImpls.put(state.attribute.getClass(), state.attribute.clone());
      }
    }
    
    // now the interfaces
    Iterator/*<Entry<Class<Attribute>, AttributeImpl>>*/ attIt = this.attributes.entrySet().iterator(); 
    while (attIt.hasNext()) {
      Entry/*<Class<Attribute>, AttributeImpl>*/ entry = (Entry/*<Class<Attribute>, AttributeImpl>*/) attIt.next();
      clone.attributes.put(entry.getKey(), clone.attributeImpls.get(entry.getValue().getClass()));
    }
    
    return clone;
  }

}
