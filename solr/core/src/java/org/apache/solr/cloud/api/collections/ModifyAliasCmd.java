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

import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.*;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CommonParams.NAME;

public class ModifyAliasCmd implements Cmd {

  public static final String META_DATA = "metadata";
  public static final String ALLOW_WHITESPACE_VALUES =  "allowWhitespaceValues";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler messageHandler;

  ModifyAliasCmd(OverseerCollectionMessageHandler messageHandler) {
    this.messageHandler = messageHandler;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    String aliasName = message.getStr(NAME);
    boolean whitespaceValues = message.getBool(ALLOW_WHITESPACE_VALUES, false);


    ZkStateReader zkStateReader = messageHandler.zkStateReader;
    if (zkStateReader.getAliases().getCollectionAliasMap().get(aliasName) == null) {
      // nicer than letting aliases object throw later on...
      throw new SolrException(BAD_REQUEST,
          String.format(Locale.ROOT,  "Can't modify non-existent alias %s", aliasName));
    }

    @SuppressWarnings("unchecked")
    Map<String, String> metadata = (Map<String, String>) message.get(META_DATA);

    zkStateReader.aliasesHolder.applyModificationAndExportToZk(aliases1 -> {
      for (String key : metadata.keySet()) {
        if ("".equals(key.trim())) {
          throw new SolrException(BAD_REQUEST, "metadata keys must not be pure whitespace");
        }
        if (!key.equals(key.trim())) {
          throw new SolrException(BAD_REQUEST, "metadat keys should not begin or end with whitespace");
        }
        String value = metadata.get(key);
        if (value != null && !"".equals(value)) {
          if (!whitespaceValues && "".equals(value.trim()))  {
            value = value.trim();
            log.warn("Pure white space value for alias metadata interpreted as null, set allowWhitespaceValue to true if you wish to store a whitespace value, or send null or empty string to avoid this message");
          }
        }
        if ("".equals(value)) {
          value = null;
        }
        aliases1 = aliases1.cloneWithCollectionAliasMetadata(aliasName, key, value);
      }
      return aliases1;
    });
  }
}
