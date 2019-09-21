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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.luke.util.LoggerFactory;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Utility class for scanning class files in jars.
 */
public class ClassScanner {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String packageName;
  private final ClassLoader[] classLoaders;

  public ClassScanner(String packageName, ClassLoader... classLoaders) {
    this.packageName = packageName;
    this.classLoaders = classLoaders;
  }

  public <T> Set<Class<? extends T>> scanSubTypes(Class<T> superType) {
    final int numThreads = Runtime.getRuntime().availableProcessors();

    List<SubtypeCollector<T>> collectors = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      collectors.add(new SubtypeCollector<T>(superType, packageName, classLoaders));
    }

    try {
      List<URL> urls = getJarUrls();
      for (int i = 0; i < urls.size(); i++) {
        collectors.get(i % numThreads).addUrl(urls.get(i));
      }

      ExecutorService executorService = Executors.newFixedThreadPool(numThreads, new NamedThreadFactory("scanner-scan-subtypes"));
      for (SubtypeCollector<T> collector : collectors) {
        executorService.submit(collector);
      }

      try {
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      } finally {
        executorService.shutdownNow();
      }

      Set<Class<? extends T>> types = new HashSet<>();
      for (SubtypeCollector<T> collector : collectors) {
        types.addAll(collector.getTypes());
      }
      return types;
    } catch (IOException e) {
      log.error("Cannot load jar file entries", e);
    }
    return Collections.emptySet();
  }

  private List<URL> getJarUrls() throws IOException {
    List<URL> urls = new ArrayList<>();
    String resourceName = resourceName(packageName);
    for (ClassLoader loader : classLoaders) {
      for (Enumeration<URL> e = loader.getResources(resourceName); e.hasMoreElements(); ) {
        URL url = e.nextElement();
        // extract jar file path from the resource name
        int index = url.getPath().lastIndexOf(".jar");
        if (index > 0) {
          String path = url.getPath().substring(0, index + 4);
          urls.add(new URL(path));
        }
      }
    }
    return  urls;
  }

  private static String resourceName(String packageName) {
    if (packageName == null || packageName.equals("")) {
      return packageName;
    }
    return packageName.replace('.', '/');
  }
}
