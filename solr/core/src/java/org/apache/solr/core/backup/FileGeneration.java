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

package org.apache.solr.core.backup;

import java.lang.invoke.MethodHandles;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finding and represent file with different generation.
 * i.e: backup-10.properties
 */
class FileGeneration {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final String prefix;
  private final String suffix;
  public final long gen;

  private FileGeneration(String prefix, String suffix, long gen) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.gen = gen;
  }

  FileGeneration nextGen() {
    return new FileGeneration(prefix, suffix, gen+1);
  }

  static FileGeneration zeroGen(String zeroGenFile) {
    int index = zeroGenFile.indexOf('.');
    if (index == -1) {
      return new FileGeneration(zeroGenFile, "", 0);
    }
    return new FileGeneration(zeroGenFile.substring(0, index), zeroGenFile.substring(index), 0);
  }

  static Optional<FileGeneration> findMostRecent(String zeroGenFile, String[] listFiles) {
    if (listFiles == null || listFiles.length == 0) {
      return Optional.empty();
    }

    int index = zeroGenFile.indexOf('.');
    if (index == -1) {
      return findMostRecent(zeroGenFile, "", listFiles);
    }
    return findMostRecent(zeroGenFile.substring(0, index), zeroGenFile.substring(index), listFiles);
  }

  private static Optional<FileGeneration> findMostRecent(String prefix, String suffix, String[] listFiles) {
    long gen = -1;
    for (String file : listFiles) {
      if (file.startsWith(prefix) && file.endsWith(suffix)) {
        if (prefix.length() + suffix.length() == file.length()) {
          gen = Math.max(gen, 0);
        } else {
          if (file.charAt(prefix.length()) != '-') {
            log.info("This file {} does not match with the file generation", file);
            continue;
          }

          try {
            gen = Math.max(gen, Long.parseLong(file.substring(prefix.length() + 1, file.length() - suffix.length())));
          } catch (NumberFormatException e) {
            log.info("This file {} does not match with the file generation", file);
          }
        }
      }
    }

    if (gen == -1)
      return Optional.empty();

    return Optional.of(new FileGeneration(prefix, suffix, gen));
  }

  public String getFileName() {
    if (gen == 0) {
      return prefix + suffix;
    }
    else return String.format("%s-%d%s", prefix, gen, suffix);
  }
}
