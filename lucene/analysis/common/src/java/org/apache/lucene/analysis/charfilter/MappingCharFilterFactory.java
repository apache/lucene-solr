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
package org.apache.lucene.analysis.charfilter;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;

/**
 * Factory for {@link MappingCharFilter}.
 *
 * <pre class="prettyprint">
 * &lt;fieldType name="text_map" class="solr.TextField" positionIncrementGap="100"&gt;
 *   &lt;analyzer&gt;
 *     &lt;charFilter class="solr.MappingCharFilterFactory" mapping="mapping.txt"/&gt;
 *     &lt;tokenizer class="solr.WhitespaceTokenizerFactory"/&gt;
 *   &lt;/analyzer&gt;
 * &lt;/fieldType&gt;</pre>
 *
 * @since Solr 1.4
 * @lucene.spi {@value #NAME}
 */
public class MappingCharFilterFactory extends CharFilterFactory implements ResourceLoaderAware {

  /** SPI name */
  public static final String NAME = "mapping";

  protected NormalizeCharMap normMap;
  private final String mapping;

  /** Creates a new MappingCharFilterFactory */
  public MappingCharFilterFactory(Map<String, String> args) {
    super(args);
    mapping = get(args, "mapping");
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  /** Default ctor for compatibility with SPI */
  public MappingCharFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    if (mapping != null) {
      List<String> wlist = null;
      List<String> files = splitFileNames(mapping);
      wlist = new ArrayList<>();
      for (String file : files) {
        List<String> lines = getLines(loader, file.trim());
        wlist.addAll(lines);
      }
      final NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
      parseRules(wlist, builder);
      normMap = builder.build();
      if (normMap.map == null) {
        // if the inner FST is null, it means it accepts nothing (e.g. the file is empty)
        // so just set the whole map to null
        normMap = null;
      }
    }
  }

  @Override
  public Reader create(Reader input) {
    // if the map is null, it means there's actually no mappings... just return the original stream
    // as there is nothing to do here.
    return normMap == null ? input : new MappingCharFilter(normMap, input);
  }

  @Override
  public Reader normalize(Reader input) {
    return create(input);
  }

  // "source" => "target"
  static Pattern p = Pattern.compile("\"(.*)\"\\s*=>\\s*\"(.*)\"\\s*$");

  protected void parseRules(List<String> rules, NormalizeCharMap.Builder builder) {
    for (String rule : rules) {
      Matcher m = p.matcher(rule);
      if (!m.find())
        throw new IllegalArgumentException(
            "Invalid Mapping Rule : [" + rule + "], file = " + mapping);
      builder.add(parseString(m.group(1)), parseString(m.group(2)));
    }
  }

  char[] out = new char[256];

  protected String parseString(String s) {
    int readPos = 0;
    int len = s.length();
    int writePos = 0;
    while (readPos < len) {
      char c = s.charAt(readPos++);
      if (c == '\\') {
        if (readPos >= len)
          throw new IllegalArgumentException("Invalid escaped char in [" + s + "]");
        c = s.charAt(readPos++);
        switch (c) {
          case '\\':
            c = '\\';
            break;
          case '"':
            c = '"';
            break;
          case 'n':
            c = '\n';
            break;
          case 't':
            c = '\t';
            break;
          case 'r':
            c = '\r';
            break;
          case 'b':
            c = '\b';
            break;
          case 'f':
            c = '\f';
            break;
          case 'u':
            if (readPos + 3 >= len)
              throw new IllegalArgumentException("Invalid escaped char in [" + s + "]");
            c = (char) Integer.parseInt(s.substring(readPos, readPos + 4), 16);
            readPos += 4;
            break;
        }
      }
      out[writePos++] = c;
    }
    return new String(out, 0, writePos);
  }
}
