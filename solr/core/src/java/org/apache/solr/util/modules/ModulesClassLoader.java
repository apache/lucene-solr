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
package org.apache.solr.util.modules;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class loader that has multiple loaders and uses them for loading classes and resources.
 */
public class ModulesClassLoader extends ClassLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<String, ClassLoader> loaderMap;

  public ModulesClassLoader(ClassLoader parent, Map<String, ClassLoader> loaders) {
    super(parent);
    this.loaderMap = loaders;
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    for (ClassLoader loader : loaderMap.values()) {
      try {
        return loader.loadClass(name);
      } catch (ClassNotFoundException e) {}
    }

    throw new ClassNotFoundException("Class " + name + " not found in any plugin. Tried: " + loaderMap.keySet());
  }

  @Override
  public URL findResource(String name) {
    for (ClassLoader loader : loaderMap.values()) {
      URL url = loader.getResource(name);
      if (url != null) {
        return url;
      }
    }

    return null;
  }

  @Override
  protected Enumeration<URL> findResources(String name) throws IOException {
    List<URL> resources = new ArrayList<URL>();
    for (ClassLoader loader : loaderMap.values()) {
      resources.addAll(Collections.list(loader.getResources(name)));
    }

    return Collections.enumeration(resources);
  }

}
