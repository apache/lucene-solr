package org.apache.lucene.analysis.stages;

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

import java.io.IOException;
import java.io.Reader;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.WeakIdentityMap;

/** Represents one stage of an analysis pipeline. */
public abstract class Stage {
  protected final Stage prevStage;

  // Single NodeTracker instance is shared across all
  // stages:
  protected final NodeTracker nodes;

  // nocommit is all this hair really worth the separation
  // of interface from impl?

  private static final WeakIdentityMap<Class<? extends Attribute>, WeakReference<Class<? extends AttributeImpl>>> attClassImplMap =
    WeakIdentityMap.newConcurrentHashMap(false);


  /** Which Attributes this stage defines */
  private final Map<Class<? extends AttributeImpl>, AttributeImpl> attImpls = new LinkedHashMap<Class<? extends AttributeImpl>, AttributeImpl>();
  private final Map<Class<? extends Attribute>, AttributeImpl> atts = new LinkedHashMap<Class<? extends Attribute>, AttributeImpl>();

  protected Stage(Stage prevStage) {
    this.prevStage = prevStage;
    if (prevStage == null) {
      this.nodes = new NodeTracker();
    } else {
      this.nodes = prevStage.nodes;
    }
  }

  /** a cache that stores all interfaces for known implementation classes for performance (slow reflection) */
  private static final WeakIdentityMap<Class<? extends AttributeImpl>,LinkedList<WeakReference<Class<? extends Attribute>>>> knownImplClasses =
    WeakIdentityMap.newConcurrentHashMap(false);
  
  static LinkedList<WeakReference<Class<? extends Attribute>>> getAttributeInterfaces(final Class<? extends AttributeImpl> clazz) {
    LinkedList<WeakReference<Class<? extends Attribute>>> foundInterfaces = knownImplClasses.get(clazz);
    if (foundInterfaces == null) {
      // we have the slight chance that another thread may do the same, but who cares?
      foundInterfaces = new LinkedList<WeakReference<Class<? extends Attribute>>>();
      // find all interfaces that this attribute instance implements
      // and that extend the Attribute interface
      Class<?> actClazz = clazz;
      do {
        for (Class<?> curInterface : actClazz.getInterfaces()) {
          if (curInterface != Attribute.class && Attribute.class.isAssignableFrom(curInterface)) {
            foundInterfaces.add(new WeakReference<Class<? extends Attribute>>(curInterface.asSubclass(Attribute.class)));
          }
        }
        actClazz = actClazz.getSuperclass();
      } while (actClazz != null);
      knownImplClasses.put(clazz, foundInterfaces);
    }
    return foundInterfaces;
  }

  /** <b>Expert:</b> Adds a custom AttributeImpl instance with one or more Attribute interfaces.
   * <p><font color="red"><b>Please note:</b> It is not guaranteed, that <code>att</code> is added to
   * the <code>AttributeSource</code>, because the provided attributes may already exist.
   * You should always retrieve the wanted attributes using {@link #getAttribute} after adding
   * with this method and cast to your class.
   * The recommended way to use custom implementations is using an {@link AttributeFactory}.
   * </font></p>
   */
  public final void addAttributeImpl(final AttributeImpl att) {
    final Class<? extends AttributeImpl> clazz = att.getClass();
    if (attImpls.containsKey(clazz)) return;
    
    // add all interfaces of this AttributeImpl to the maps
    for (WeakReference<Class<? extends Attribute>> curInterfaceRef : getAttributeInterfaces(clazz)) {
      final Class<? extends Attribute> curInterface = curInterfaceRef.get();
      assert (curInterface != null) :
        "We have a strong reference on the class holding the interfaces, so they should never get evicted";
      // Attribute is a superclass of this interface
      if (!atts.containsKey(curInterface)) {
        atts.put(curInterface, att);
        attImpls.put(clazz, att);
      }
    }
  }
  
  private static Class<? extends AttributeImpl> getClassForInterface(Class<? extends Attribute> attClass) {
    final WeakReference<Class<? extends AttributeImpl>> ref = attClassImplMap.get(attClass);
    Class<? extends AttributeImpl> clazz = (ref == null) ? null : ref.get();
    if (clazz == null) {
      // we have the slight chance that another thread may do the same, but who cares?
      try {
        attClassImplMap.put(attClass,
                            new WeakReference<Class<? extends AttributeImpl>>(
                                clazz = Class.forName(attClass.getName() + "Impl", true, attClass.getClassLoader())
                                .asSubclass(AttributeImpl.class))
                            );
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Could not find implementing class for " + attClass.getName());
      }
    }
    return clazz;
  }

  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value.
   * This method first checks if an instance of that class is 
   * already in this AttributeSource and returns it. Otherwise a
   * new instance is created, added to this AttributeSource and returned. 
   */
  protected final <A extends Attribute> A create(Class<A> attClass) {
    AttributeImpl attImpl = atts.get(attClass);
    if (attImpl == null) {
      if (!(attClass.isInterface() && Attribute.class.isAssignableFrom(attClass))) {
        throw new IllegalArgumentException(
          "addAttribute() only accepts an interface that extends Attribute, but " +
          attClass.getName() + " does not fulfil this contract."
        );
      }

      try {
        attImpl = getClassForInterface(attClass).newInstance();
      } catch (InstantiationException e) {
        throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Could not instantiate implementing class for " + attClass.getName());
      }

      addAttributeImpl(attImpl);
      return attClass.cast(attImpl);
    } else {
      throw new IllegalArgumentException(attClass + " was already added");
    }
  }

  public final <A extends Attribute> A get(Class<A> attClass) {
    AttributeImpl attImpl = atts.get(attClass);
    if (attImpl == null) {
      if (prevStage != null) {
        return prevStage.get(attClass);
      } else {
        //throw new IllegalArgumentException("This AttributeSource does not have the attribute '" + attClass.getName() + "'.");
        return null;
      }
    }

    return attClass.cast(attImpl);
  }

  public abstract boolean next() throws IOException;

  public void reset(Reader reader) {
    if (prevStage != null) {
      prevStage.reset(reader);
    } else {
      nodes.reset();
    }
  }

  public boolean anyNodesCanChange() {
    System.out.println("    nodes=" + nodes);
    return nodes.anyNodesCanChange();
  }
}
