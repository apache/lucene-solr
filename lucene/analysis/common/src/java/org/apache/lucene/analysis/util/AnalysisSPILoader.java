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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.util.ClassLoaderUtils;

/**
 * Helper class for loading named SPIs from classpath (e.g. Tokenizers, TokenStreams).
 * @lucene.internal
 */
public final class AnalysisSPILoader<S extends AbstractAnalysisFactory> {

  private volatile Map<String,Class<? extends S>> services = Collections.emptyMap();
  private volatile Set<String> originalNames = Collections.emptySet();
  private final Class<S> clazz;

  private static final Pattern SERVICE_NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]+$");

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
    if (clazzClassloader != null && !ClassLoaderUtils.isParentClassLoader(clazzClassloader, classloader)) {
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
    final LinkedHashMap<String,Class<? extends S>> services = new LinkedHashMap<>(this.services);
    final LinkedHashSet<String> originalNames = new LinkedHashSet<>(this.originalNames);
    ServiceLoader.load(clazz, classloader).stream().map(ServiceLoader.Provider::type).forEachOrdered(service -> {
      String name = null;
      String originalName = null;
      Throwable cause = null;
      try {
        originalName = lookupSPIName(service);
        name = originalName.toLowerCase(Locale.ROOT);
        if (!isValidName(originalName)) {
          throw new ServiceConfigurationError("The name " + originalName + " for " + service.getName() +
              " is invalid: Allowed characters are (English) alphabet, digits, and underscore. It should be started with an alphabet.");
        }
      } catch (NoSuchFieldException | IllegalAccessException | IllegalStateException e) {
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
        // preserve (case-sensitive) original name for reference
        originalNames.add(originalName);
      }
    });

    // make sure that the number of lookup keys is same to the number of original names.
    // in fact this constraint should be met in existence checks of the lookup map key,
    // so this is more like an assertion rather than a status check.
    if (services.keySet().size() != originalNames.size()) {
      throw new ServiceConfigurationError("Service lookup key set is inconsistent with original name set!");
    }

    this.services = Map.copyOf(services);
    this.originalNames = Set.copyOf(originalNames);
  }

  private boolean isValidName(String name) {
    return SERVICE_NAME_PATTERN.matcher(name).matches();
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
    return originalNames;
  }  

  /**
   * Looks up SPI name (static "NAME" field) with appropriate modifiers.
   * Also it must be a String class and declared in the concrete class.
   * @return the SPI name
   * @throws NoSuchFieldException - if the "NAME" field is not defined.
   * @throws IllegalAccessException - if the "NAME" field is inaccessible.
   * @throws IllegalStateException - if the "NAME" field does not have appropriate modifiers or isn't a String field.
   */
  public static String lookupSPIName(Class<? extends AbstractAnalysisFactory> service) throws NoSuchFieldException, IllegalAccessException, IllegalStateException {
    final Field field = service.getDeclaredField("NAME");
    int modifier = field.getModifiers();
    if (Modifier.isPublic(modifier) && Modifier.isStatic(modifier) &&
        Modifier.isFinal(modifier) && Objects.equals(field.getType(), String.class)) {
      return ((String) field.get(null));
    }
    throw new IllegalStateException("No SPI name defined.");
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
