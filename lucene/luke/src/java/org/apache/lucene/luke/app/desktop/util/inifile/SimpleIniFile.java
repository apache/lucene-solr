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

package org.apache.lucene.luke.app.desktop.util.inifile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Simple implementation of {@link IniFile} */
public class SimpleIniFile implements IniFile {

  private final Map<String, OptionMap> sections = new LinkedHashMap<>();

  private IniFileWriter writer = new SimpleIniFileWriter();

  private IniFileReader reader = new SimpleIniFileReader();

  @Override
  public synchronized void load(Path path) throws IOException {
    sections.putAll(reader.readSections(path));
  }

  @Override
  public synchronized void store(Path path) throws IOException {
    writer.writeSections(path, sections);
  }

  @Override
  public synchronized void put(String section, String option, Object value) {
    if (checkString(section) && checkString(option) && Objects.nonNull(value)) {
      sections.putIfAbsent(section, new OptionMap());
      sections
          .get(section)
          .put(option, (value instanceof String) ? (String) value : String.valueOf(value));
    }
  }

  @Override
  public String getString(String section, String option) {
    if (checkString(section) && checkString(option)) {
      OptionMap options = sections.get(section);
      if (options != null) {
        return options.getAsString(option);
      }
    }
    return null;
  }

  @Override
  public Boolean getBoolean(String section, String option) {
    if (checkString(section) && checkString(option)) {
      OptionMap options = sections.get(section);
      if (options != null) {
        return options.getAsBoolean(option);
      }
    }
    return false;
  }

  private boolean checkString(String s) {
    return Objects.nonNull(s) && !s.equals("");
  }

  Map<String, OptionMap> getSections() {
    return sections;
  }
}
