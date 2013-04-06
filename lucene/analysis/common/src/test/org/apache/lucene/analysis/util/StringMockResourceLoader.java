package org.apache.lucene.analysis.util;

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
import java.io.IOException;
import java.io.InputStream;

/** Fake resource loader for tests: works if you want to fake reading a single file */
public class StringMockResourceLoader implements ResourceLoader {
  String text;

  public StringMockResourceLoader(String text) {
    this.text = text;
  }
  
  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    try {
      return Class.forName(cname).asSubclass(expectedType);
    } catch (Exception e) {
      throw new RuntimeException("Cannot load class: " + cname, e);
    }
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    Class<? extends T> clazz = findClass(cname, expectedType);
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Cannot create instance: " + cname, e);
    }
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    return new ByteArrayInputStream(text.getBytes("UTF-8"));
  }
}
