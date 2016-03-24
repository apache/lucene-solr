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
package org.apache.solr.core.backup.repository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class LocalFileSystemRepository implements BackupRepository {
  private final URI baseLocation;

  public LocalFileSystemRepository(URI baseLocation) {
    this.baseLocation = baseLocation;
  }

  @Override
  public URI createURI(String... pathComponents) {
    Path result = Paths.get(baseLocation);
    for(String path : pathComponents) {
      result = result.resolve(path);
    }
    return result.toUri();
  }

  @Override
  public void createDirectory(URI path) throws IOException {
    Files.createDirectory(Paths.get(path));
  }

  @Override
  public void deleteDirectory(URI path) throws IOException {
    Files.walkFileTree(Paths.get(path), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc)
          throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Override
  public boolean exists(URI path) throws IOException {
    return Files.exists(Paths.get(path));
  }

  @Override
  public OutputStream createOutput(URI path) throws IOException {
    return Files.newOutputStream(Paths.get(path));
  }
}
