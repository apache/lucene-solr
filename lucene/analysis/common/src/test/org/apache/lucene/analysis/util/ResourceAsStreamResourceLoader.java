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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.IOUtils;

public class ResourceAsStreamResourceLoader implements ResourceLoader {
  Class<?> clazz;
  
  public ResourceAsStreamResourceLoader(Class<?> clazz) {
    this.clazz = clazz;
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    return clazz.getResourceAsStream(resource);
  }

  @Override
  public List<String> getLines(String resource) throws IOException {
    BufferedReader input = null;
    ArrayList<String> lines;
    try {
      input = new BufferedReader(new InputStreamReader(openResource(resource),
          IOUtils.CHARSET_UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)));

      lines = new ArrayList<String>();
      for (String word=null; (word=input.readLine())!=null;) {
        // skip initial bom marker
        if (lines.isEmpty() && word.length() > 0 && word.charAt(0) == '\uFEFF')
          word = word.substring(1);
        // skip comments
        if (word.startsWith("#")) continue;
        word=word.trim();
        // skip blank lines
        if (word.length()==0) continue;
        lines.add(word);
      }
    } catch (CharacterCodingException ex) {
      throw new RuntimeException("Error loading resource (wrong encoding?): " + resource, ex);
    } finally {
      if (input != null)
        input.close();
    }
    return lines;
  }

  // TODO: do this subpackages thing... wtf is that?
  @Override
  public <T> T newInstance(String cname, Class<T> expectedType, String... subpackages) {
    try {
      Class<? extends T> clazz = Class.forName(cname).asSubclass(expectedType);
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
