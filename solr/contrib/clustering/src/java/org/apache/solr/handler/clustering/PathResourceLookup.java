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
package org.apache.solr.handler.clustering;

import org.carrot2.util.ResourceLookup;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Carrot2 resource provider from the provided list of filesystem paths.
 */
final class PathResourceLookup implements ResourceLookup {
  private final List<Path> locations;

  PathResourceLookup(List<Path> locations) {
    if (locations == null || locations.isEmpty()) {
      throw new RuntimeException("At least one resource location is required.");
    }
    this.locations = locations;
  }

  @Override
  public InputStream open(String resource) throws IOException {
    Path p = locate(resource);
    if (p == null) {
      throw new IOException(
          "Resource "
              + p
              + " not found relative to: "
              + locations.stream()
                  .map(path -> path.toAbsolutePath().toString())
                  .collect(Collectors.joining(", ")));
    }
    return new BufferedInputStream(Files.newInputStream(p));
  }

  @Override
  public boolean exists(String resource) {
    return locate(resource) != null;
  }

  @Override
  public String pathOf(String resource) {
    return "["
        + locations.stream()
            .map(path -> path.resolve(resource).toAbsolutePath().toString())
            .collect(Collectors.joining(" | "))
        + "]";
  }

  private Path locate(String resource) {
    for (Path base : locations) {
      Path p = base.resolve(resource);
      if (Files.exists(p)) {
        return p;
      }
    }
    return null;
  }
}
