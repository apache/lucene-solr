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

package org.apache.solr.spelling.suggest;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for a dictionary with an iterator over bounded-length random strings (with fixed
 * weight of 1 and null payloads) that only operates when RandomDictionary.enabledSysProp
 * is set; this will be true from the time a RandomDictionary has been constructed until
 * its enabledSysProp has been cleared.
 */
public class RandomTestDictionaryFactory extends DictionaryFactory {
  public static final String RAND_DICT_MAX_ITEMS = "randDictMaxItems";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final long DEFAULT_MAX_ITEMS = 100_000_000;

  @Override
  public RandomTestDictionary create(SolrCore core, SolrIndexSearcher searcher) {
    if(params == null) {
      // should not happen; implies setParams was not called
      throw new IllegalStateException("Value of params not set");
    }
    String name = (String)params.get(CommonParams.NAME);
    if (name == null) { // Shouldn't happen since this is the component name
      throw new IllegalArgumentException(CommonParams.NAME + " is a mandatory parameter");
    }
    long maxItems = DEFAULT_MAX_ITEMS;
    Object specifiedMaxItems = params.get(RAND_DICT_MAX_ITEMS);
    if (specifiedMaxItems != null) {
      maxItems = Long.parseLong(specifiedMaxItems.toString());
    }
    return new RandomTestDictionary(name, maxItems);
  }

  public static class RandomTestDictionary implements Dictionary {
    private static final String SYS_PROP_PREFIX = RandomTestDictionary.class.getName() + ".enabled.";
    private final String enabledSysProp; // Clear this property to stop the input iterator
    private final long maxItems;
    private long emittedItems = 0L;

    RandomTestDictionary(String name, long maxItems) {
      enabledSysProp = getEnabledSysProp(name);
      this.maxItems = maxItems;
      synchronized (RandomTestDictionary.class) {
        if (System.getProperty(enabledSysProp) != null) {
          throw new RuntimeException("System property '" + enabledSysProp + "' is already in use.");
        }
        System.setProperty(enabledSysProp, "true");
      }
    }

    public static String getEnabledSysProp(String suggesterName) {
      return SYS_PROP_PREFIX + suggesterName;
    }

    @Override
    public InputIterator getEntryIterator() throws IOException {
      return new InputIterator.InputIteratorWrapper(new RandomByteRefIterator());
    }

    private class RandomByteRefIterator implements BytesRefIterator {
      private static final int MAX_LENGTH = 100;

      @Override
      public BytesRef next() throws IOException {
        BytesRef next = null;
        if (System.getProperty(enabledSysProp) != null) {
          if (emittedItems < maxItems) {
            ++emittedItems;
            next = new BytesRef(TestUtil.randomUnicodeString(LuceneTestCase.random(), MAX_LENGTH));
            if (emittedItems % 1000 == 0) {
              log.info("{} emitted {} items", enabledSysProp, emittedItems);
            }
          } else {
            log.info("{} disabled after emitting {} items", enabledSysProp, emittedItems);
            System.clearProperty(enabledSysProp); // disable once maxItems has been reached
            emittedItems = 0L;
          }
        } else {
          log.warn("{} invoked when disabled", enabledSysProp);
          emittedItems = 0L;
        }
        return next;
      }
    }
  }
}
