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
package org.apache.solr.update.processor;

import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Factory which creates RegexBoostProcessors
 * <p>
 * The factory initializes a shared object cache which is passed to the processor
 * and this way reduces rules file parsing to the first time the UpdateChain
 * is initialized.
 * @since 4.2.0
 */
public class RegexpBoostProcessorFactory extends UpdateRequestProcessorFactory {

    private SolrParams params;
    private final Map<Object, Object> sharedObjectCache = new HashMap<>();

    @Override
    public void init(@SuppressWarnings("rawtypes") final NamedList args) {
        if (args != null) {
          this.params = args.toSolrParams();
        }
    }

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest request,
            SolrQueryResponse response,
            UpdateRequestProcessor nextProcessor) {

        return new RegexpBoostProcessor(this.params, request, response, nextProcessor, this.sharedObjectCache);
    }
}
