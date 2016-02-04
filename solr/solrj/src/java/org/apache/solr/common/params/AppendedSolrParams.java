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
package org.apache.solr.common.params;

/**
 * SolrParams wrapper which acts similar to DefaultSolrParams except that
 * it "appends" the values of multi-value params from both sub instances, so
 * that all of the values are returned. 
 */
public class AppendedSolrParams extends DefaultSolrParams {

  public static AppendedSolrParams wrapAppended(SolrParams params, SolrParams extra) {
    return new AppendedSolrParams(params, extra);
  }

  private AppendedSolrParams(SolrParams main, SolrParams extra) {
    super(main, extra);
  }

  @Override
  public String[] getParams(String param) {
    String[] main = params.getParams(param);
    String[] extra = defaults.getParams(param);
    if (null == extra || 0 == extra.length) {
      return main;
    }
    if (null == main || 0 == main.length) {
      return extra;
    }
    String[] result = new String[main.length + extra.length];
    System.arraycopy(main,0,result,0,main.length);
    System.arraycopy(extra,0,result,main.length,extra.length);
    return result;
  }

  @Override
  public String toString() {
    return "{main("+params+"),extra("+defaults+")}";
  }
}
