package org.apache.lucene.util;

/**
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

import java.io.IOException;

/**
 * Prints nothing. Just to make sure tests pass w/ and without enabled InfoStream
 * without actually making noise.
 * @lucene.experimental
 */
public class NullInfoStream extends InfoStream {

  @Override
  public void message(String component, String message) {
    assert component != null;
    assert message != null;
  }
  
  @Override
  public boolean isEnabled(String component) {
    assert component != null;
    return true; // to actually enable logging, we just ignore on message()
  }

  @Override
  public void close() throws IOException {   
  }
}
