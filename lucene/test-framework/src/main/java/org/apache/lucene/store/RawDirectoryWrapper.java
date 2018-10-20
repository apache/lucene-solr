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

/**
 * Delegates all operations, even optional ones, to the wrapped directory.
 * <p>
 * This class is used if you want the most realistic testing, but still
 * with a checkindex on close. If you want asserts and evil things,
 * use MockDirectoryWrapper instead.
 */
public final class RawDirectoryWrapper extends BaseDirectoryWrapper {

  public RawDirectoryWrapper(Directory delegate) {
    super(delegate);
  }
  
  @Override
  public void copyFrom(Directory from, String src, String dest, IOContext context) throws IOException {
    in.copyFrom(from, src, dest, context);
  }
  
  @Override
  public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
    return in.openChecksumInput(name, context);
  }

  @Override
  protected void ensureOpen() throws AlreadyClosedException {
    in.ensureOpen();
  }
}
