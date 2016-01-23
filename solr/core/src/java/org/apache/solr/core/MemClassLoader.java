package org.apache.solr.core;

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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MemClassLoader extends ClassLoader implements AutoCloseable, ResourceLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean allJarsLoaded = false;
  private final SolrResourceLoader parentLoader;
  private List<PluginBag.RuntimeLib> libs = new ArrayList<>();
  private Map<String, Class> classCache = new HashMap<>();


  public MemClassLoader(List<PluginBag.RuntimeLib> libs, SolrResourceLoader resourceLoader) {
    this.parentLoader = resourceLoader;
    this.libs = libs;
  }


  public synchronized void loadJars() {
    if (allJarsLoaded) return;

    for (PluginBag.RuntimeLib lib : libs) {
      try {
        lib.loadJar();
        lib.verify();
      } catch (Exception exception) {
        if (exception instanceof SolrException) throw (SolrException) exception;
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Atleast one runtimeLib could not be loaded", exception);
      }
    }
    allJarsLoaded = true;
  }


  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if(!allJarsLoaded ) loadJars();
    try {
      return parentLoader.findClass(name, Object.class);
    } catch (Exception e) {
      return loadFromRuntimeLibs(name);
    }
  }

  private synchronized  Class<?> loadFromRuntimeLibs(String name) throws ClassNotFoundException {
    Class result = classCache.get(name);
    if(result != null)
      return result;
    AtomicReference<String> jarName = new AtomicReference<>();
    ByteBuffer buf = null;
    try {
      buf = getByteBuffer(name, jarName);
    } catch (Exception e) {
      throw new ClassNotFoundException("class could not be loaded " + name, e);
    }
    if (buf == null) throw new ClassNotFoundException("Class not found :" + name);
    ProtectionDomain defaultDomain = null;
    //using the default protection domain, with no permissions
    try {
      defaultDomain = new ProtectionDomain(new CodeSource(new URL("http://localhost/.system/blob/" + jarName.get()), (Certificate[]) null),
          null);
    } catch (MalformedURLException mue) {
      throw new ClassNotFoundException("Unexpected exception ", mue);
      //should not happen
    }
    log.info("Defining_class {} from runtime jar {} ", name, jarName);

    result = defineClass(name, buf.array(), buf.arrayOffset(), buf.limit(), defaultDomain);
    classCache.put(name, result);
    return result;
  }

  private ByteBuffer getByteBuffer(String name, AtomicReference<String> jarName) throws Exception {
    if (!allJarsLoaded) {
      loadJars();

    }

    String path = name.replace('.', '/').concat(".class");
    ByteBuffer buf = null;
    for (PluginBag.RuntimeLib lib : libs) {
      try {
        buf = lib.getFileContent(path);
        if (buf != null) {
          jarName.set(lib.getName());
          break;
        }
      } catch (Exception exp) {
        throw new ClassNotFoundException("Unable to load class :" + name, exp);
      }
    }

    return buf;
  }

  @Override
  public void close() throws Exception {
    for (PluginBag.RuntimeLib lib : libs) {
      try {
        lib.close();
      } catch (Exception e) {
      }
    }
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    AtomicReference<String> jarName = new AtomicReference<>();
    try {
      ByteBuffer buf = getByteBuffer(resource, jarName);
      if (buf == null) throw new IOException("Resource could not be found " + resource);
    } catch (Exception e) {
      throw new IOException("Resource could not be found " + resource, e);
    }
    return null;
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    if(!allJarsLoaded ) loadJars();
    try {
      return findClass(cname).asSubclass(expectedType);
    } catch (Exception e) {
      if (e instanceof SolrException) {
        throw (SolrException) e;
      } else {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error loading class " + cname, e);
      }
    }

  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    try {
      return findClass(cname, expectedType).newInstance();
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "error instantiating class :" + cname, e);
    }
  }


}
