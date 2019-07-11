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


import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.ServiceConfigurationError;

import org.apache.lucene.util.SPIClassIterator;

/**
 * Helper class for loading named SPIs from classpath (e.g. Tokenizers, TokenStreams).
 * @lucene.internal
 */
public final class AnalysisSPILoader<S extends AbstractAnalysisFactory> {

  private volatile Map<String,Class<? extends S>> services = Collections.emptyMap();
  private final Class<S> clazz;
  private final String[] suffixes;
  
  public AnalysisSPILoader(Class<S> clazz) {
    this(clazz, new String[] { clazz.getSimpleName() });
  }

  public AnalysisSPILoader(Class<S> clazz, ClassLoader loader) {
    this(clazz, new String[] { clazz.getSimpleName() }, loader);
  }

  public AnalysisSPILoader(Class<S> clazz, String[] suffixes) {
    this(clazz, suffixes, null);
  }
  
  public AnalysisSPILoader(Class<S> clazz, String[] suffixes, ClassLoader classloader) {
    this.clazz = clazz;
    this.suffixes = suffixes;
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
      final String clazzName = service.getSimpleName();
      String name = null;
      for (String suffix : suffixes) {
        if (clazzName.endsWith(suffix)) {
          name = clazzName.substring(0, clazzName.length() - suffix.length()).toLowerCase(Locale.ROOT);
          break;
        }
      }
      if (name == null) {
        throw new ServiceConfigurationError("The class name " + service.getName() +
          " has wrong suffix, allowed are: " + Arrays.toString(suffixes));
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
    final Class<? extends S> service = services.get(name.toLowerCase(Locale.ROOT));
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
