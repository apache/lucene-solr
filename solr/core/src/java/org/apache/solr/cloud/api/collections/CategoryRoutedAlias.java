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

package org.apache.solr.cloud.api.collections;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.update.AddUpdateCommand;

public class CategoryRoutedAlias implements RoutedAlias {
  private final String aliasName;
  private final Map<String, String> aliasProperties;

  public CategoryRoutedAlias(String aliasName, Map<String, String> aliasProperties) {

    this.aliasName = aliasName;
    this.aliasProperties = aliasProperties;
  }

  @Override
  public boolean updateParsedCollectionAliases(ZkController zkController) {
    return false;
  }

  @Override
  public String getAliasName() {
    return aliasName;
  }

  @Override
  public List<Map.Entry<Instant, String>> parseCollections(Aliases aliases) {
    return null;
  }

  @Override
  public void validateRouteValue(AddUpdateCommand cmd) {

  }

  @Override
  public String createCollectionsIfRequired(AddUpdateCommand cmd) {
    return null;
  }

  @Override
  public String computeInitialCollectionName(String collection) {
    return null;
  }
}
