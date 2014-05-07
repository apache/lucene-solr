package org.apache.lucene.util;

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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

/**
 * An AttributeFactory creates instances of {@link AttributeImpl}s.
 */
public abstract class AttributeFactory {
  
  /**
   * Returns an {@link AttributeImpl} for the supplied {@link Attribute} interface class.
   */
  public abstract AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass);
  
  /**
   * Returns a correctly typed {@link MethodHandle} for the no-arg ctor of the given class.
   */
  static final MethodHandle findAttributeImplCtor(Class<? extends AttributeImpl> clazz) {
    try {
      return lookup.findConstructor(clazz, NO_ARG_CTOR).asType(NO_ARG_RETURNING_ATTRIBUTEIMPL);
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new IllegalArgumentException("Cannot lookup accessible no-arg constructor for: " + clazz.getName(), e);
    }
  }
  
  private static final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
  private static final MethodType NO_ARG_CTOR = MethodType.methodType(void.class);
  private static final MethodType NO_ARG_RETURNING_ATTRIBUTEIMPL = MethodType.methodType(AttributeImpl.class);
  
  /**
   * This is the default factory that creates {@link AttributeImpl}s using the
   * class name of the supplied {@link Attribute} interface class by appending <code>Impl</code> to it.
   */
  public static final AttributeFactory DEFAULT_ATTRIBUTE_FACTORY = new DefaultAttributeFactory(true);
  
  static final class DefaultAttributeFactory extends AttributeFactory {
    private final WeakIdentityMap<Class<? extends Attribute>, Object> attClassImplMap =
      WeakIdentityMap.newConcurrentHashMap(false);
    private final ClassLoader myClassLoader = getClass().getClassLoader();
    private final boolean useMethodHandles;
    
    // this constructor is available for tests, to be able to test the pure-reflective case, too
    DefaultAttributeFactory(boolean useMethodHandles) {
      this.useMethodHandles = useMethodHandles;
    }
  
    @Override
    public AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
      // first lookup from cache:
      Object cached = attClassImplMap.get(attClass);
      if (cached instanceof MethodHandle) {
        return invokeMethodHandle((MethodHandle) cached);
      } else if (cached instanceof Reference) {
        @SuppressWarnings("unchecked") final Class<? extends AttributeImpl> clazz = 
            ((Reference<Class<? extends AttributeImpl>>) cached).get();
        if (clazz != null) {
          return invokeReflective(clazz);
        }
        cached = null;
        // fall-through
      }
      // No cache hit!
      // Please note: we have the slight chance that another thread may do the same, but who cares?
      assert cached == null;
      final Class<? extends AttributeImpl> implClazz = findImplClass(attClass);
      // if the attribute impl is from our own ClassLoader, we optimize to use pre-allocated MethodHandle to instantiate the object
      if (useMethodHandles && implClazz.getClassLoader() == myClassLoader) {
        final MethodHandle constr = findAttributeImplCtor(implClazz);
        attClassImplMap.put(attClass, constr);
        return invokeMethodHandle(constr);
      } else {
        // otherwise, to not refer to the class forever (because the MethodHandle strongly
        // references the class), so it can never be unloaded, we use slower reflection:
        attClassImplMap.put(attClass, new WeakReference<>(implClazz));
        return invokeReflective(implClazz);
      }
    }
    
    private Class<? extends AttributeImpl> findImplClass(Class<? extends Attribute> attClass) {
      try {
        return Class.forName(attClass.getName() + "Impl", true, attClass.getClassLoader()).asSubclass(AttributeImpl.class);
      } catch (ClassNotFoundException cnfe) {
        throw new IllegalArgumentException("Cannot find implementing class for: " + attClass.getName());
      }      
    }
    
    private AttributeImpl invokeMethodHandle(MethodHandle constr) {
      try {
        return (AttributeImpl) constr.invokeExact();
      } catch (Throwable t) {
        rethrow(t);
        throw new AssertionError();
      }
    }
    
    private AttributeImpl invokeReflective(Class<? extends AttributeImpl> implClass) {
      try {
        return implClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IllegalArgumentException("Cannot instantiate implementing class: " + implClass.getName(), e);
      }
    }
  }
  
  /** <b>Expert</b>: AttributeFactory returning an instance of the given {@code clazz} for the
   * attributes it implements. For all other attributes it calls the given delegate factory
   * as fallback. This class can be used to prefer a specific {@code AttributeImpl} which
   * combines multiple attributes over separate classes.
   * @lucene.internal
   */
  public abstract static class StaticImplementationAttributeFactory<A extends AttributeImpl> extends AttributeFactory {
    private final AttributeFactory delegate;
    private final Class<A> clazz;
    
    /** <b>Expert</b>: Creates an AttributeFactory returning {@code clazz} as instance for the
     * attributes it implements and for all other attributes calls the given delegate factory. */
    public StaticImplementationAttributeFactory(AttributeFactory delegate, Class<A> clazz) {
      this.delegate = delegate;
      this.clazz = clazz;
    }
    
    @Override
    public final AttributeImpl createAttributeInstance(Class<? extends Attribute> attClass) {
      return attClass.isAssignableFrom(clazz) ? createInstance() : delegate.createAttributeInstance(attClass);
    }
    
    /** Creates an instance of {@code A}. */
    protected abstract A createInstance();
    
    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (other == null || other.getClass() != this.getClass())
        return false;
      @SuppressWarnings("rawtypes")
      final StaticImplementationAttributeFactory af = (StaticImplementationAttributeFactory) other;
      return this.delegate.equals(af.delegate) && this.clazz == af.clazz;
    }
    
    @Override
    public int hashCode() {
      return 31 * delegate.hashCode() + clazz.hashCode();
    }
  }
  
  /** Returns an AttributeFactory returning an instance of the given {@code clazz} for the
   * attributes it implements. The given {@code clazz} must have a public no-arg constructor.
   * For all other attributes it calls the given delegate factory as fallback.
   * This method can be used to prefer a specific {@code AttributeImpl} which combines
   * multiple attributes over separate classes.
   * <p>Please save instances created by this method in a static final field, because
   * on each call, this does reflection for creating a {@link MethodHandle}.
   */
  public static <A extends AttributeImpl> AttributeFactory getStaticImplementation(AttributeFactory delegate, Class<A> clazz) {
    final MethodHandle constr = findAttributeImplCtor(clazz);
    return new StaticImplementationAttributeFactory<A>(delegate, clazz) {
      @Override
      protected A createInstance() {
        try {
          return (A) constr.invokeExact();
        } catch (Throwable t) {
          rethrow(t);
          throw new AssertionError();
        }
      }
    };
  }
  
  // Hack to rethrow unknown Exceptions from {@link MethodHandle#invoke}:
  // TODO: remove the impl in test-framework, this one is more elegant :-)
  static void rethrow(Throwable t) {
    AttributeFactory.<Error>rethrow0(t);
  }
  
  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void rethrow0(Throwable t) throws T {
    throw (T) t;
  }
  
}