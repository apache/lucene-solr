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

package org.apache.solr.cluster.api;

import org.apache.solr.common.SolrException;

import java.io.IOException;
import java.io.InputStream;

/**A binary resource. The impl is agnostic of the content type */
public interface Resource {
    /**
     * This is a full path. e.g schema.xml , solrconfig.xml , lang/stopwords.txt etc
     */
    String name();
    /** read a file/resource.
     * The caller should consume the stream completely and should not hold a reference to this stream.
     * This method closes the stream soon after the method returns
     * @param resourceConsumer This should be a full path. e.g schema.xml , solrconfig.xml , lang/stopwords.txt etc
     */
    void get(Consumer resourceConsumer) throws SolrException;

    interface Consumer {
        void read(InputStream is) throws IOException;
    }
}
