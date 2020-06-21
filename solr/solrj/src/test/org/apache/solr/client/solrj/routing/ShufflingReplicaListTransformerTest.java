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
package org.apache.solr.client.solrj.routing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.Replica;
import org.junit.Test;

public class ShufflingReplicaListTransformerTest extends SolrTestCase {

  private final ShufflingReplicaListTransformer transformer = new ShufflingReplicaListTransformer(random());

  @Test
  public void testTransformReplicas() throws Exception {
    final List<Replica> replicas = new ArrayList<>();
    int counter = 0;
    for (final String url : createRandomUrls()) {
      Map<String, Object> propMap = new HashMap<>();
      propMap.put("core", "core" + counter);
      propMap.put("type", "NRT");
      propMap.put("node_name", "node" + counter);
      counter++;
      replicas.add(new Replica(url, propMap, "c1", "s1"));
    }
    implTestTransform(replicas);
  }

  @Test
  public void testTransformUrls() throws Exception {
    final List<String> urls = createRandomUrls();
    implTestTransform(urls);
  }

  private <TYPE> void implTestTransform(List<TYPE> inputs) throws Exception {
    final List<TYPE> transformedInputs = new ArrayList<>(inputs);
    transformer.transform(transformedInputs);

    final Set<TYPE> inputSet = new HashSet<>(inputs);
    final Set<TYPE> transformedSet = new HashSet<>(transformedInputs);

    assertTrue(inputSet.equals(transformedSet));
  }

  private final List<String> createRandomUrls() throws Exception {
    final List<String> urls = new ArrayList<>();
    maybeAddUrl(urls, "a"+random().nextDouble());
    maybeAddUrl(urls, "bb"+random().nextFloat());
    maybeAddUrl(urls, "ccc"+random().nextGaussian());
    maybeAddUrl(urls, "dddd"+random().nextInt());
    maybeAddUrl(urls, "eeeee"+random().nextLong());
    Collections.shuffle(urls, random());
    return urls;
  }

  private final void maybeAddUrl(final List<String> urls, final String url) {
    if (random().nextBoolean()) {
      urls.add(url);
    }
  }

}
