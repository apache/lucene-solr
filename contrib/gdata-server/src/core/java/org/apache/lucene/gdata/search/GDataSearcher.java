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
package org.apache.lucene.gdata.search;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.search.Query;

/**
 * @author Simon Willnauer
 * @param <T> 
 *
 */
public interface GDataSearcher <T>{

    /**
     * executes an Query and returns a list of defined return values of type T
     * @param query - the query to apply to the searcher
     * @param hitcount - the amount of hits returned by this search
     * @param offset - the hit count offset 
     * @param feedId 
     * @return List of T
     * @throws IOException - if the underlying lucene searcher throws an IO Exception 
     */
    public List<T> search(Query query,int hitcount, int offset, String feedId)throws IOException;
    /**
     * Destroys this Searcher
     */
    public abstract void close();
}
