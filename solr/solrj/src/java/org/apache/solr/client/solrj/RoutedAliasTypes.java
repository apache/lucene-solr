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

package org.apache.solr.client.solrj;


/**
 * Types of Routed Alias supported.
 *
 * Routed Alias collections have a naming pattern of XYZ where X is the alias name, Y is the separator prefix and
 * Z is the data driven value distinguishing the bucket.
 */
public enum RoutedAliasTypes {
  TIME {
    @Override
    public String getSeparatorPrefix() {
      return "__TRA__";
    }
  },
  CATEGORY {
    @Override
    public String getSeparatorPrefix() {
      return "__CRA__";
    }
  },
  DIMENSIONAL {
    public String getSeparatorPrefix() {
      throw new UnsupportedOperationException("dimensions within dimensions are not allowed");
    }
  };
  public abstract String getSeparatorPrefix();

}
