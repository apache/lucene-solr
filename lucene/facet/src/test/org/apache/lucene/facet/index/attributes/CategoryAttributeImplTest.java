package org.apache.lucene.facet.index.attributes;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.FacetException;
import org.apache.lucene.facet.index.DummyProperty;
import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryAttributeImpl;
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

public class CategoryAttributeImplTest extends LuceneTestCase {

  @Test
  public void testCategoryPath() {
    CategoryAttribute ca = new CategoryAttributeImpl();

    assertNull("Category Path should be null", ca.getCategoryPath());

    CategoryPath cp = new CategoryPath("a", "b");
    ca.setCategoryPath(cp);

    assertEquals("Wrong Category Path", cp, ca.getCategoryPath());

    ca.setCategoryPath(null);
    assertNull("Category Path should be null", ca.getCategoryPath());

    ca = new CategoryAttributeImpl(cp);
    assertEquals("Wrong Category Path", cp, ca.getCategoryPath());
  }

  @Test
  public void testProperties() throws FacetException {
    CategoryAttribute ca = new CategoryAttributeImpl();

    assertNull("Attribute should be null", ca
        .getProperty(DummyProperty.class));
    assertNull("Attribute classes should be null", ca.getPropertyClasses());

    ca.addProperty(new DummyProperty());
    assertEquals("DummyProperty should be in properties",
        new DummyProperty(), ca.getProperty(DummyProperty.class));
    assertEquals("Attribute classes should contain 1 element", 1, ca
        .getPropertyClasses().size());

    boolean failed = false;
    try {
      ca.addProperty(new DummyProperty());
    } catch (UnsupportedOperationException e) {
      failed = true;
    }

    if (!failed) {
      fail("Two DummyAttributes added to the same CategoryAttribute");
    }

    ca.clearProperties();
    assertNull("Attribute classes should be null", ca.getPropertyClasses());

    ca.addProperty(new DummyProperty());
    assertEquals("DummyProperty should be in properties",
        new DummyProperty(), ca.getProperty(DummyProperty.class));
    ca.remove(DummyProperty.class);
    assertEquals("DummyProperty should not be in properties", null, ca
        .getProperty(DummyProperty.class));
    assertNull("Attribute classes should be null", ca.getPropertyClasses());

    ca.addProperty(new DummyProperty());
    List<Class<? extends CategoryProperty>> propertyClasses = new ArrayList<Class<? extends CategoryProperty>>();
    assertEquals("No property expected when no classes given", null, ca
        .getProperty(propertyClasses));
    propertyClasses.add(DummyProperty.class);
    assertEquals("DummyProperty should be in properties",
        new DummyProperty(), ca.getProperty(propertyClasses));
    propertyClasses.add(OrdinalProperty.class);
    assertEquals("DummyProperty should be in properties",
        new DummyProperty(), ca.getProperty(propertyClasses));
    propertyClasses.clear();
    propertyClasses.add(OrdinalProperty.class);
    assertEquals("No ordinal property expected", null, ca
        .getProperty(propertyClasses));
  }

  @Test
  public void testCloneCopyToAndSet() throws FacetException {
    CategoryAttributeImpl ca1 = new CategoryAttributeImpl();

    CategoryPath cp = new CategoryPath("a", "b");
    ca1.setCategoryPath(cp);
    ca1.addProperty(new DummyProperty());

    CategoryAttribute ca2 = ca1.clone();
    assertEquals("Error in cloning", ca1, ca2);

    CategoryAttributeImpl ca3 = new CategoryAttributeImpl();
    assertNotSame("Should not be the same", ca1, ca3);
    ca1.copyTo(ca3);
    assertEquals("Error in cloning", ca1, ca3);

    ca2.setCategoryPath(null);
    assertNotSame("Should not be the same", ca1, ca2);
    ca2.set(ca3);
    assertEquals("Error in cloning", ca1, ca2);
  }
}
