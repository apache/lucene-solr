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

package org.apache.lucene.luke.util.reflection;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.util.LoggerFactory;

final class SubtypeCollector<T> implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Set<URL> urls = new HashSet<>();

  private final Class<T> superType;

  private final String packageName;

  private final ClassLoader[] classLoaders;

  private final Set<Class<? extends T>> types = new HashSet<>();

  SubtypeCollector(Class<T> superType, String packageName, ClassLoader... classLoaders) {
    this.superType = superType;
    this.packageName = packageName;
    this.classLoaders = classLoaders;
  }

  void addUrl(URL url) {
    urls.add(url);
  }

  Set<Class<? extends T>> getTypes() {
    return Collections.unmodifiableSet(types);
  }

  @Override
  public void run() {
    for (URL url : urls) {
      try (JarInputStream jis = new JarInputStream(url.openStream())) {
        // iterate all zip entry in the jar
        ZipEntry entry;
        while ((entry = jis.getNextEntry()) != null) {
          String name = entry.getName();
          if (name.endsWith(".class") && name.indexOf('$') < 0
              && !name.contains("package-info") && !name.startsWith("META-INF")) {
            String fqcn = convertToFQCN(name);
            if (!fqcn.startsWith(packageName)) {
              continue;
            }
            for (ClassLoader cl : classLoaders) {
              try {
                Class<?> clazz = Class.forName(fqcn, false, cl);
                if (superType.isAssignableFrom(clazz) && !Objects.equals(superType, clazz)) {
                  types.add(clazz.asSubclass(superType));
                }
                break;
              } catch (Throwable e) {
              }
            }
          }
        }
      } catch (IOException e) {
        log.error("Cannot load jar {}", url, e);
      }
    }
  }

  private static String convertToFQCN(String name) {
    if (name == null || name.equals("")) {
      return name;
    }
    int index = name.lastIndexOf(".class");
    return name.replace('/', '.').substring(0, index);
  }

}
