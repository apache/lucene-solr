package org.apache.solr.handler.component;
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

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.common.params.CommonParams;


/**
 *
 *
 **/
public class DistributedDebugComponentTest extends BaseDistributedSearchTestCase {

  @Override
  public void doTest() throws Exception {
    index(id, "1", "title", "this is a title");
    index(id, "2", "title", "this is another title.");
    index(id, "3", "title", "Mary had a little lamb.");
    commit();
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    handle.put("maxScore", SKIPVAL);
    handle.put("time", SKIPVAL);
    
    // we care only about the spellcheck results
    handle.put("response", SKIP);
    handle.put("explain", UNORDERED);    
    handle.put("debug", UNORDERED);
    flags |= UNORDERED;
    query("q", "*:*", CommonParams.DEBUG_QUERY, "true");
    query("q", "*:*", CommonParams.DEBUG, CommonParams.TIMING);
    query("q", "*:*", CommonParams.DEBUG, CommonParams.RESULTS);
    query("q", "*:*", CommonParams.DEBUG, CommonParams.QUERY);

  }
}
