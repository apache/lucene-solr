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
package org.apache.lucene.dependencies;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse a properties file, performing non-recursive Ant-like
 * property value interpolation, and return the resulting Properties.
 */
public class InterpolatedProperties extends Properties {
  private static final Pattern PROPERTY_REFERENCE_PATTERN = Pattern.compile("\\$\\{([^}]+)\\}");

  /**
   * Loads the properties file via {@link Properties#load(InputStream)},
   * then performs non-recursive Ant-like property value interpolation.
   */
  @Override
  public void load(InputStream inStream) throws IOException {
    throw new UnsupportedOperationException("InterpolatedProperties.load(InputStream) is not supported.");
  }

  /**
   * Loads the properties file via {@link Properties#load(Reader)},
   * then performs non-recursive Ant-like property value interpolation.
   */
  @Override
  public void load(Reader reader) throws IOException {
    super.load(reader);
    interpolate();
  }

  /**
   * Perform non-recursive Ant-like property value interpolation
   */
  private void interpolate() {
    StringBuffer buffer = new StringBuffer();
    for (Map.Entry<?,?> entry : entrySet()) {
      buffer.setLength(0);
      Matcher matcher = PROPERTY_REFERENCE_PATTERN.matcher(entry.getValue().toString());
      while (matcher.find()) {
        String interpolatedValue = getProperty(matcher.group(1));
        if (null != interpolatedValue) {
          matcher.appendReplacement(buffer, interpolatedValue);
        }
      }
      matcher.appendTail(buffer);
      setProperty((String) entry.getKey(), buffer.toString());
    }
  }
}
