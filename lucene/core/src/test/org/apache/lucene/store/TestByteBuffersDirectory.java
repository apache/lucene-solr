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
package org.apache.lucene.store;


import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Supplier;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

public class TestByteBuffersDirectory extends BaseDirectoryTestCase {
  
  private Supplier<ByteBuffersDirectory> implSupplier;

  public TestByteBuffersDirectory(Supplier<ByteBuffersDirectory> implSupplier, String name) {
    this.implSupplier = implSupplier;
  }
  
  @Override
  protected Directory getDirectory(Path path) throws IOException {
    return implSupplier.get();
  }
  
  @ParametersFactory(argumentFormatting = "impl=%2$s")
  public static Iterable<Object[]> parametersWithCustomName() {
    return Arrays.asList(new Object [][] {
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), ByteBuffersDirectory.OUTPUT_AS_MANY_BUFFERS), "many buffers"},
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), ByteBuffersDirectory.OUTPUT_AS_ONE_BUFFER), "one buffer"},
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), ByteBuffersDirectory.OUTPUT_AS_MANY_BUFFERS_LUCENE), "lucene's buffers"},
      {(Supplier<ByteBuffersDirectory>) () -> new ByteBuffersDirectory(
          new SingleInstanceLockFactory(), ByteBuffersDirectory.OUTPUT_AS_BYTE_ARRAY), "byte array"},
    });
  }
}
