package org.apache.lucene.facet.index;

import org.junit.Before;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.index.CategoryContainer;
import org.apache.lucene.facet.taxonomy.CategoryPath;

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

public abstract class CategoryContainerTestBase extends LuceneTestCase {
  
  protected CategoryContainer categoryContainer;
  protected CategoryPath[] initialCatgeories;

  @Before
  public void setCategoryContainer() {
    initialCatgeories = new CategoryPath[3];
    initialCatgeories[0] = new CategoryPath("one", "two", "three");
    initialCatgeories[1] = new CategoryPath("four");
    initialCatgeories[2] = new CategoryPath("five", "six");

    categoryContainer = new CategoryContainer();

    for (int i = 0; i < initialCatgeories.length; i++) {
      categoryContainer.addCategory(initialCatgeories[i]);
    }
  }
  
}
