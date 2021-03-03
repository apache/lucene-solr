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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaListTransformerTest extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // A transformer that keeps only matching choices
  private static class ToyMatchingReplicaListTransformer implements ReplicaListTransformer, Closeable {

    private final String regex;

    public ToyMatchingReplicaListTransformer(String regex)
    {
      this.regex = regex;
    }

    public void transform(List<?> choices)
    {
      log.info("regex transform input: {}", choices);
      Iterator<?> it = choices.iterator();
      while (it.hasNext()) {
        Object choice = it.next();
        final String url;
        if (choice instanceof String) {
          url = (String)choice;
        }
        else if (choice instanceof Replica) {
          url = ((Replica)choice).getCoreUrl();
        } else {
          url = null;
        }
        log.info("considering: {} w/url={}", choice, url);
        if (url == null || !url.matches(regex)) {
          log.info("removing {}", choice);
          it.remove();
        }
      }
    }

    @Override
    public void close() {

    }

  }

  // A transformer that makes no transformation
  private static class ToyNoOpReplicaListTransformer implements ReplicaListTransformer, Closeable {

    public ToyNoOpReplicaListTransformer()
    {
    }

    public void transform(List<?> choices)
    {
      // no-op
      log.info("No-Op transform ignoring input: {}", choices);
    }

    @Override
    public void close() {

    }

  }

  @Test
  public void testTransform() throws Exception {

    final String regex = ".*?r_n\\d+.*";

    AtomicReference<ReplicaListTransformer> transformer = new AtomicReference<>();
    try {
      if (random().nextBoolean()) {
        log.info("Using ToyMatching Transfomer");
        transformer.set(new ToyMatchingReplicaListTransformer(regex));

      } else {
        log.info("Using conditional Transfomer");
        try (HttpShardHandlerFactory hsh = new HttpShardHandlerFactory() {
          {
            transformer.set(getReplicaListTransformer(
                    new LocalSolrQueryRequest(null,
                            new ModifiableSolrParams().add("toyRegEx", regex))));
          }
          @Override
          protected ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req) {
            final SolrParams params = req.getParams();

            if (params.getBool("toyNoTransform", false)) {
              return new ToyNoOpReplicaListTransformer();
            }

            final String regex = params.get("toyRegEx");
            if (regex != null) {
              return new ToyMatchingReplicaListTransformer(regex);
            }

            return super.getReplicaListTransformer(req);
          }

        };) {

        }

      }

      final List<Replica> inputs = new ArrayList<>();
      final List<Replica> expectedTransformed = new ArrayList<>();

      final List<String> urls = createRandomUrls();
      for (int ii = 0; ii < urls.size(); ++ii) {

        final String name = "coll1_s1_r_n" + (ii + 1);
        final String url = urls.get(ii);
        final Map<String, Object> propMap = new HashMap<String, Object>();
        propMap.put(ZkStateReader.NODE_NAME_PROP, url);
        propMap.put("type", "NRT");
        propMap.put("id", String.valueOf(ii));
        // a skeleton replica, good enough for this test's purposes
        final Replica replica = new Replica(name, propMap, "c1",-1l,"s1", new Replica.NodeNameToBaseUrl() {
          @Override
          public String getBaseUrlForNodeName(String nodeName) {
            return Utils.getBaseUrlForNodeName(name, "http");
          }
        });

        inputs.add(replica);
        final String coreUrl = replica.getCoreUrl();
        if (coreUrl.matches(regex)) {
          log.info("adding replica=[{}] to expected due to core url ({}) regex match on {} ",
                  replica, coreUrl, regex);
          expectedTransformed.add(replica);
        } else {
          log.info("NOT expecting replica=[{}] due to core url ({}) regex mismatch ({})",
                  replica, coreUrl, regex);
        }

      }

      final List<Replica> actualTransformed = new ArrayList<>(inputs);
      transformer.get().transform(actualTransformed);

      assertEquals(expectedTransformed, actualTransformed);
    } finally {
      if (transformer != null) {
        transformer.get().close();
      }
    }
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
