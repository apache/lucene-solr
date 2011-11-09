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

import org.apache.lucene.search.NRTManager; // javadocs

/** Pass an implementation of this to {@link NRTManager} or
 *  {@link SearcherManager} to warm a new {@link
 *  IndexSearcher} before it's put into production.
 *
 * @lucene.experimental */

public interface SearcherWarmer {
  // TODO: can we somehow merge this w/ IW's
  // IndexReaderWarmer.... should IW switch to this?    
  public void warm(IndexSearcher s) throws IOException;
}
