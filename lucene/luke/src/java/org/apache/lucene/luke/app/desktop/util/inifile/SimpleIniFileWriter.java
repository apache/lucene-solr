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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Simple implementation of {@link IniFileWriter} */
public class SimpleIniFileWriter implements IniFileWriter {

  @Override
  public void writeSections(Path path, Map<String, OptionMap> sections) throws IOException {
    try (BufferedWriter w = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      for (Map.Entry<String, OptionMap> section : sections.entrySet()) {
        w.write("[" + section.getKey() + "]");
        w.newLine();

        for (Map.Entry<String, String> option : section.getValue().entrySet()) {
          w.write(option.getKey() + " = " + option.getValue());
          w.newLine();
        }

        w.newLine();
      }
      w.flush();
    }
  }
}
