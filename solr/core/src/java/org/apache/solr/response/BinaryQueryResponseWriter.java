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
package org.apache.solr.response;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.solr.request.SolrQueryRequest;

/**
 * Implementations of <code>BinaryQueryResponseWriter</code> are used to
 * write response in binary format.
 * <p>
 * Functionality is exactly same as its parent class <code>QueryResponseWriter</code>
 * But it may not implement the <code>write(Writer writer, SolrQueryRequest request, SolrQueryResponse response)</code>
 * method  
 *
 */
public interface BinaryQueryResponseWriter extends QueryResponseWriter{

    /**Use it to write the response in a binary format
     */
    public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response) throws IOException;
}
