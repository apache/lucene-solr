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
package org.apache.solr.handler.component;

import java.io.IOException;

import org.apache.solr.common.util.NamedList;

/**
 * A search component that supports the configuration and selective use of custom search components.
<p>
 * Usage illustration:
 * <ul>
 * <li> for <code>/select?q=*:*</code> the default case i.e. <code>QueryComponent</code> is used
 * <li> for <code>/select?q=*:*&amp;foo=true</code> the <code>FooQueryComponent</code> is used
 * <li> for <code>/select?q=*:*&amp;bar=true</code> the <code>BarQueryComponent</code> is used
 * </ul>
<p>
 * Configuration illustration:
<pre class="prettyprint">
&lt;searchComponent name="queryDefault" class="org.apache.solr.handler.component.QueryComponent"/&gt;
&lt;searchComponent name="queryFoo" class="com.company.team.FooQueryComponent"/&gt;
&lt;searchComponent name="queryBar" class="com.company.team.BarQueryComponent"/&gt;

&lt;searchComponent name="query" class="org.apache.solr.handler.component.DelegatingSearchComponent"&gt;
  &lt;str name="mappings.default"&gt;queryDefault&lt;/str&gt;
  &lt;lst name="mappings"&gt;
    &lt;str name="foo"&gt;queryFoo&lt;str&gt;
    &lt;str name="bar"&gt;queryBar&lt;str&gt;
  &lt;/lst&gt;
  &lt;str name="category"&gt;QUERY&lt;/str&gt;
&lt;/searchComponent&gt;
</pre>
 */
public class DelegatingSearchComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "delegate";

  private NamedList mappings;
  private String defaultMapping;
  private Category category = Category.OTHER;

  @Override
  public void init( NamedList args ) {
    super.init(args);

    this.mappings = (NamedList)args.get("mappings");
    this.defaultMapping = (String)args.get("mappings.default");

    final Object categoryObj = args.get("category");
    if (categoryObj != null) {
      this.category = Category.valueOf((String)categoryObj);
    }
  }

  private SearchComponent getDelegate(ResponseBuilder rb) {
    String component = this.defaultMapping;
    for (int ii = 0; ii < mappings.size(); ++ii) {
      if (rb.req.getParams().getBool(mappings.getName(ii), false)) {
        component = (String)mappings.getVal(ii);
        break;
      }
    }
    if (component != null) {
      return rb.req.getCore().getSearchComponent(component);
    } else {
      return null;
    }
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    getDelegate(rb).prepare(rb);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    getDelegate(rb).process(rb);
  }

  public int distributedProcess(ResponseBuilder rb) throws IOException {
    return getDelegate(rb).distributedProcess(rb);
  }

  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    getDelegate(rb).modifyRequest(rb, who, sreq);
  }

  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    getDelegate(rb).handleResponses(rb, sreq);
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    getDelegate(rb).finishStage(rb);
  }

  @Override
  public String getDescription() {
    return "delegate";
  }

  @Override
  public Category getCategory() {
    return this.category;
  }

}
