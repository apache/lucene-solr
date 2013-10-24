package org.apache.lucene.search.grouping;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Base class for grouping related tests.
 */
// TODO (MvG) : The grouping tests contain a lot of code duplication. Try to move the common code to this class..
public abstract class AbstractGroupingTestCase extends LuceneTestCase {

  protected String generateRandomNonEmptyString() {
    String randomValue;
    do {
      // B/c of DV based impl we can't see the difference between an empty string and a null value.
      // For that reason we don't generate empty string
      // groups.
      randomValue = _TestUtil.randomRealisticUnicodeString(random());
      //randomValue = _TestUtil.randomSimpleString(random());
    } while ("".equals(randomValue));
    return randomValue;
  }
}
