package org.apache.solr.rest.schema;
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

import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.GETable;
import org.apache.solr.schema.IndexSchema;
import org.restlet.representation.Representation;
import org.restlet.resource.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class responds to requests at /solr/(corename)/schema/solrqueryparser/defaultoperator
 */
public class SolrQueryParserDefaultOperatorResource extends BaseSolrResource implements GETable {
  private static final Logger log = LoggerFactory.getLogger(SolrQueryParserDefaultOperatorResource.class);

  public SolrQueryParserDefaultOperatorResource() {
    super();
  }

  @Override
  public void doInit() throws ResourceException {
    super.doInit();
  }

  @Override
  public Representation get() {
    try {
      getSolrResponse().add(IndexSchema.DEFAULT_OPERATOR, getSchema().getQueryParserDefaultOperator());
    } catch (Exception e) {
      getSolrResponse().setException(e);
    }
    handlePostExecution(log);

    return new SolrOutputRepresentation();
  }
}
