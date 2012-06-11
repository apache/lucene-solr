package org.apache.lucene.facet.enhancements.params;

import java.util.List;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.enhancements.CategoryEnhancement;
import org.apache.lucene.facet.enhancements.CategoryEnhancementDummy1;
import org.apache.lucene.facet.enhancements.CategoryEnhancementDummy2;
import org.apache.lucene.facet.enhancements.params.DefaultEnhancementsIndexingParams;
import org.apache.lucene.facet.enhancements.params.EnhancementsIndexingParams;
import org.apache.lucene.facet.index.DummyProperty;
import org.apache.lucene.facet.index.attributes.CategoryProperty;

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

public class DefaultEnhancementsIndexingParamsTest extends LuceneTestCase {

  @Test
  public void testCategoryEnhancements() {
    EnhancementsIndexingParams params = 
      new DefaultEnhancementsIndexingParams(
          new CategoryEnhancementDummy1());

    // check retainable properties 
    List<Class<? extends CategoryProperty>> retainableProps = params
        .getRetainableProperties();
    assertNull("Unexpected content in retainable list", retainableProps);

    params.addCategoryEnhancements(new CategoryEnhancementDummy2());

    List<CategoryEnhancement> enhancements = params
        .getCategoryEnhancements();

    assertEquals("Wrong number of enhancements", 2, enhancements.size());

    assertTrue("Wrong first enhancement",
        enhancements.get(0) instanceof CategoryEnhancementDummy1);
    assertTrue("Wrong second enhancement",
        enhancements.get(1) instanceof CategoryEnhancementDummy2);

    // re-check retainable properties 
    retainableProps = params.getRetainableProperties();
    assertNotNull("Unexpected empty retainable list", retainableProps);
    assertEquals("Unexpected size of retainable list", 1, retainableProps
        .size());
    assertEquals("Wrong property in retainable list", DummyProperty.class,
        retainableProps.get(0));

  }
}
