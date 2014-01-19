package org.apache.lucene.server.handlers;

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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.util.ResourceLoader;

/** Holds "files" in RAM, and falls back to delegate if the
 *  resource isn't in RAM.  This is used for analysis
 *  components that specify their "files" as strings in
 *  RAM. */

class RAMResourceLoaderWrapper implements ResourceLoader {

  private final Map<String,String> files = new HashMap<String,String>();
  private final ResourceLoader delegate;

  /** Sole constructor. */
  public RAMResourceLoaderWrapper(ResourceLoader delegate) {
    this.delegate = delegate;
  }

  public void add(String name, String contents) {
    if (files.containsKey(name)) {
      throw new IllegalArgumentException("name \"" + name + "\" was already added");
    }
    files.put(name, contents);
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    String value = files.get(resource);
    if (value != null) {
      return new ByteArrayInputStream(value.getBytes("UTF-8"));
    } else if (delegate == null) {
      throw new FileNotFoundException("resource \"" + resource + "\" is not found");
    } else {
      return delegate.openResource(resource);
    }
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    return delegate.newInstance(cname, expectedType);
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    return delegate.findClass(cname, expectedType);
  }
}
