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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.util.LuceneTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link MergePolicy} with a no-arg constructor that proxies to an
 * instance retrieved from {@link LuceneTestCase#newMergePolicy}.
 * Solr tests utilizing the Lucene randomized test framework can refer 
 * to this class in solrconfig.xml to get a fully randomized merge policy.
 */
public class RandomMergePolicy extends FilterMergePolicy {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public RandomMergePolicy() {
    this(LuceneTestCase.newMergePolicy());
  }

  protected RandomMergePolicy(MergePolicy inner) {
    super(inner);
    if (log.isInfoEnabled()) {
      log.info("RandomMergePolicy wrapping {}: {}", inner.getClass(), inner);
    }
  }

}
