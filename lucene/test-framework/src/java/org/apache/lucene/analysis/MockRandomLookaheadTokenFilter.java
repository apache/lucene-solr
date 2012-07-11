package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.util.Random;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * Uses {@link LookaheadTokenFilter} to randomly peek at future tokens.
 */

public final class MockRandomLookaheadTokenFilter extends LookaheadTokenFilter<LookaheadTokenFilter.Position> {
  private final static boolean DEBUG = false;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final Random random;
  private final long seed;

  public MockRandomLookaheadTokenFilter(Random random, TokenStream in) {
    super(in);
    this.seed = random.nextLong();
    this.random = new Random(seed);
  }

  @Override
  public Position newPosition() {
    return new Position();
  }

  @Override
  protected void afterPosition() throws IOException {
    if (!end && random.nextInt(4) == 2) {
      peekToken();
    }
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (DEBUG) {
      System.out.println("\n" + Thread.currentThread().getName() + ": MRLTF.incrToken");
    }

    if (!end) {
      while (true) {
        if (random.nextInt(3) == 1) {
          if (!peekToken()) {
            if (DEBUG) {
              System.out.println("  peek; inputPos=" + inputPos + " END");
            }
            break;
          }
          if (DEBUG) {
            System.out.println("  peek; inputPos=" + inputPos + " token=" + termAtt);
          }
        } else {
          if (DEBUG) {
            System.out.println("  done peek");
          }
          break;
        }
      }
    }

    final boolean result = nextToken();
    if (result) {
      if (DEBUG) {
        System.out.println("  return nextToken token=" + termAtt);
      }
    } else {
      if (DEBUG) {
        System.out.println("  return nextToken END");
      }
    }
    return result;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    random.setSeed(seed);
  }
}
