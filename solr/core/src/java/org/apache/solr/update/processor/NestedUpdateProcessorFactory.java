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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

public class NestedUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  private EnumSet<NestedFlag> fields;
  private static final List<String> allowedConfFields = Arrays.stream(NestedFlag.values()).map(e -> e.toString().toLowerCase(Locale.ROOT)).collect(Collectors.toList());

  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next ) {
    return new NestedUpdateProcessor(req, rsp, fields, next);
  }

  @Override
  public void init( NamedList args )
  {
    Object tmp = args.remove("fields");
    if (null == tmp) {
      throw new SolrException(SERVER_ERROR,
          "'fields' must be configured");
    }
    if (! (tmp instanceof String) ) {
      throw new SolrException(SERVER_ERROR,
          "'fields' must be configured as a <str>");
    }
    List<String> fields = StrUtils.splitSmart((String)tmp, ',');
    if(!allowedConfFields.containsAll(fields)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Nested URP may only contain: " + StringUtils.join(allowedConfFields, ", ") +
      " got: " + StringUtils.join(fields, ", ") + " instead");
    }
    this.fields = generateNestedFlags(fields);
  }

  private static EnumSet<NestedFlag> generateNestedFlags(List<String> fields) {
    return EnumSet.copyOf(fields.stream().map(e -> NestedFlag.valueOf(e.toUpperCase(Locale.ROOT))).collect(Collectors.toList()));
  }

  public enum NestedFlag {
    PATH,
    PARENT
  }
}
