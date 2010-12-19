package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util._TestUtil;

/**
 * Unit tests for the ParallelMultiSearcher 
 */
public class TestParallelMultiSearcher extends TestMultiSearcher {
  List<ExecutorService> pools = new ArrayList<ExecutorService>();

  @Override
  public void tearDown() throws Exception {
    for (ExecutorService exec : pools)
      exec.awaitTermination(1000, TimeUnit.MILLISECONDS);
    pools.clear();
    super.tearDown();
  }

  @Override
  protected MultiSearcher getMultiSearcherInstance(Searcher[] searchers)
    throws IOException {
    ExecutorService exec = Executors.newFixedThreadPool(_TestUtil.nextInt(random, 2, 8));
    pools.add(exec);
    return new ParallelMultiSearcher(exec, searchers);
  }

}
