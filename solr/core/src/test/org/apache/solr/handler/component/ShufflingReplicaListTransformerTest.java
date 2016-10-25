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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.cloud.Replica;
import org.junit.Test;

public class ShufflingReplicaListTransformerTest extends LuceneTestCase {

  private final ShufflingReplicaListTransformer transformer = new ShufflingReplicaListTransformer(random());

  @Test
  public void testTransform() throws Exception {

    final List<Replica> replicas = new ArrayList<>();
    for (final String url : createRandomUrls()) {
      replicas.add(new Replica(url, new HashMap<String,Object>()));
    }

    final List<Replica> transformedReplicas = new ArrayList<>(replicas);
    transformer.transform(transformedReplicas);

    final Set<Replica> replicaSet = new HashSet<>(replicas);
    final Set<Replica> transformedReplicaSet = new HashSet<>(transformedReplicas);

    assertTrue(replicaSet.equals(transformedReplicaSet));
  }

  @Test
  public void testTransformUrls() throws Exception {

    final List<String> urls = createRandomUrls();

    final List<String> transformedUrls = new ArrayList<>(urls);
    transformer.transformUrls(transformedUrls);

    final Set<String> urlSet = new HashSet<>(urls);
    final Set<String> transformedUrlSet = new HashSet<>(transformedUrls);

    assertTrue(urlSet.equals(transformedUrlSet));
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
