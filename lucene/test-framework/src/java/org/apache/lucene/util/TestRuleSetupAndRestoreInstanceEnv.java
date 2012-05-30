package org.apache.lucene.util;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.RandomCodec;
import org.apache.lucene.search.BooleanQuery;
import org.junit.internal.AssumptionViolatedException;

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

/**
 * Prepares and restores {@link LuceneTestCase} at instance level 
 * (fine grained junk that doesn't fit anywhere else).
 */
final class TestRuleSetupAndRestoreInstanceEnv extends AbstractBeforeAfterRule {
  private int savedBoolMaxClauseCount;

  protected void before() {
    savedBoolMaxClauseCount = BooleanQuery.getMaxClauseCount();

    Codec codec = Codec.getDefault();
    if (LuceneTestCase.shouldAvoidCodec(codec.getName())) {
      throw new AssumptionViolatedException(
          "Method not allowed to use codec: " + codec.getName() + ".");
    }
    // TODO: make this more efficient
    if (codec instanceof RandomCodec) {
      for (String name : ((RandomCodec)codec).formatNames) {
        if (LuceneTestCase.shouldAvoidCodec(name)) {
          throw new AssumptionViolatedException(
              "Method not allowed to use postings format: " + name + ".");
        }
      }
    }
    PostingsFormat pf = codec.postingsFormat();
    if (LuceneTestCase.shouldAvoidCodec(pf.getName())) {
      throw new AssumptionViolatedException(
          "Method not allowed to use postings format: " + pf.getName() + ".");
    }
    
  }

  protected void after() {
    BooleanQuery.setMaxClauseCount(savedBoolMaxClauseCount);
  }
}
