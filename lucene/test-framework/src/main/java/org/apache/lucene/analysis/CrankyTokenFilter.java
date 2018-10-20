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
package org.apache.lucene.analysis;

import java.io.IOException;
import java.util.Random;

/** 
 * Throws IOException from random Tokenstream methods.
 * <p>
 * This can be used to simulate a buggy analyzer in IndexWriter,
 * where we must delete the document but not abort everything in the buffer.
 */
public final class CrankyTokenFilter extends TokenFilter {
  final Random random;
  int thingToDo;
  
  /** Creates a new CrankyTokenFilter */
  public CrankyTokenFilter(TokenStream input, Random random) {
    super(input);
    this.random = random;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (thingToDo == 0 && random.nextBoolean()) {
      throw new IOException("Fake IOException from TokenStream.incrementToken()");
    }
    return input.incrementToken();
  }
  
  @Override
  public void end() throws IOException {
    super.end();
    if (thingToDo == 1 && random.nextBoolean()) {
      throw new IOException("Fake IOException from TokenStream.end()");
    }
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    thingToDo = random.nextInt(100);
    if (thingToDo == 2 && random.nextBoolean()) {
      throw new IOException("Fake IOException from TokenStream.reset()");
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (thingToDo == 3 && random.nextBoolean()) {
      throw new IOException("Fake IOException from TokenStream.close()");
    }
  }
}
