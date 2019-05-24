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
package org.apache.lucene.analysis.util;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.ServiceConfigurationError;
import java.util.TreeMap;

import org.apache.lucene.util.SPIClassIterator;

/**
 * Helper class for loading named SPIs from classpath (e.g. Tokenizers, TokenStreams).
 * @lucene.internal
 */
public final class AnalysisSPILoader<S extends AbstractAnalysisFactory> {

  private volatile Map<String,Class<? extends S>> services = Collections.emptyMap();
  private final Class<S> clazz;

  public AnalysisSPILoader(Class<S> clazz) {
    this(clazz, null);
  }

  public AnalysisSPILoader(Class<S> clazz, ClassLoader classloader) {
    this.clazz = clazz;
    // if clazz' classloader is not a parent of the given one, we scan clazz's classloader, too:
    final ClassLoader clazzClassloader = clazz.getClassLoader();
    if (classloader == null) {
      classloader = clazzClassloader;
    }
    if (clazzClassloader != null && !SPIClassIterator.isParentClassLoader(clazzClassloader, classloader)) {
      reload(clazzClassloader);
    }
    reload(classloader);
  }

  /** 
   * Reloads the internal SPI list from the given {@link ClassLoader}.
   * Changes to the service list are visible after the method ends, all
   * iterators (e.g., from {@link #availableServices()},...) stay consistent. 
   * 
   * <p><b>NOTE:</b> Only new service providers are added, existing ones are
   * never removed or replaced.
   * 
   * <p><em>This method is expensive and should only be called for discovery
   * of new service providers on the given classpath/classloader!</em>
   */
  public synchronized void reload(ClassLoader classloader) {
    Objects.requireNonNull(classloader, "classloader");
    final LinkedHashMap<String,Class<? extends S>> services =
      new LinkedHashMap<>(this.services);
    final SPIClassIterator<S> loader = SPIClassIterator.get(clazz, classloader);
    while (loader.hasNext()) {
      final Class<? extends S> service = loader.next();
      String name = null;
      Throwable cause = null;
      try {
        // Lookup "NAME" field with appropriate modifiers.
        // Also it must be a String class and declared in the service class.
        final Field field = service.getField("NAME");
        int modifier = field.getModifiers();
        if (Modifier.isStatic(modifier) && Modifier.isFinal(modifier) &&
            field.getType().equals(String.class) &&
            Objects.equals(field.getDeclaringClass(), service)) {
          name = (String)field.get(null);
        }
      } catch (NoSuchFieldException | IllegalAccessException e) {
        cause = e;
      }
      if (name == null) {
        throw new ServiceConfigurationError("The class name " + service.getName() +
            " has no service name field: [public static final String NAME]", cause);
      }
      // only add the first one for each name, later services will be ignored
      // this allows to place services before others in classpath to make 
      // them used instead of others
      //
      // TODO: Should we disallow duplicate names here?
      // Allowing it may get confusing on collisions, as different packages
      // could contain same factory class, which is a naming bug!
      // When changing this be careful to allow reload()!
      if (!services.containsKey(name)) {
        services.put(name, service);
      }
    }
    this.services = Collections.unmodifiableMap(services);
  }
  
  public S newInstance(String name, Map<String,String> args) {
    final Class<? extends S> service = lookupClass(name);
    return newFactoryClassInstance(service, args);
  }
  
  public Class<? extends S> lookupClass(String name) {
    final Map<String,Class<? extends S>> services = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    services.putAll(this.services);
    final Class<? extends S> service = services.get(name);
    if (service != null) {
      return service;
    } else {
      throw new IllegalArgumentException("A SPI class of type "+clazz.getName()+" with name '"+name+"' does not exist. "+
          "You need to add the corresponding JAR file supporting this SPI to your classpath. "+
          "The current classpath supports the following names: "+availableServices());
    }
  }

  public Set<String> availableServices() {
    return services.keySet();
  }  
  
  /** Creates a new instance of the given {@link AbstractAnalysisFactory} by invoking the constructor, passing the given argument map. */
  public static <T extends AbstractAnalysisFactory> T newFactoryClassInstance(Class<T> clazz, Map<String,String> args) {
    try {
      return clazz.getConstructor(Map.class).newInstance(args);
    } catch (InvocationTargetException ite) {
      final Throwable cause = ite.getCause();
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException("Unexpected checked exception while calling constructor of "+clazz.getName(), cause);
    } catch (ReflectiveOperationException e) {
      throw new UnsupportedOperationException("Factory "+clazz.getName()+" cannot be instantiated. This is likely due to missing Map<String,String> constructor.", e);
    }
  }
}
