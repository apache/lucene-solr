package org.apache.lucene.facet.index;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;

import org.junit.Test;

import org.apache.lucene.facet.FacetException;
import org.apache.lucene.facet.enhancements.association.AssociationIntProperty;
import org.apache.lucene.facet.enhancements.association.AssociationProperty;
import org.apache.lucene.facet.index.CategoryContainer;
import org.apache.lucene.facet.index.attributes.CategoryAttribute;
import org.apache.lucene.facet.index.attributes.CategoryAttributeImpl;
import org.apache.lucene.facet.index.streaming.CategoryAttributesStream;
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

public class CategoryContainerTest extends CategoryContainerTestBase {

  @Test
  public void basicTest() {
    assertEquals("Wrong number of categories in the container", 3,
        categoryContainer.size());

    categoryContainer.clear();
    assertEquals("Container should not contain categories after clear", 0,
        categoryContainer.size());
  }

  @Test
  public void testIterator() throws FacetException {
    Iterator<CategoryAttribute> iterator = categoryContainer.iterator();

    // count the number of tokens
    int nCategories;
    for (nCategories = 0; iterator.hasNext(); nCategories++) {
      iterator.next();
    }
    assertEquals("Wrong number of tokens", 3, nCategories);
  }

  @Test
  public void testExistingNewCategoryWithProperty() throws FacetException {
    categoryContainer.addCategory(new CategoryPath("five", "six"),
        new DummyProperty());
    Iterator<CategoryAttribute> iterator = categoryContainer.iterator();

    // count the number of tokens, and check there is one DummyAttribute
    int nCategories;
    int nProperties = 0;
    for (nCategories = 0; iterator.hasNext(); nCategories++) {
      CategoryAttribute attribute = iterator.next();
      if (attribute.getProperty(DummyProperty.class) != null) {
        nProperties++;
      }
    }
    assertEquals("Wrong number of tokens", 3, nCategories);
    assertEquals("Wrong number of tokens with properties", 1, nProperties);
  }

  @Test
  public void testMultipleCategoriesWithProperties() throws FacetException {
    AssociationProperty associationProperty = new AssociationIntProperty(
        49);
    categoryContainer.addCategory(new CategoryPath("five", "six"),
        new DummyProperty(), associationProperty);
    categoryContainer.addCategory(new CategoryPath("seven", "eight"),
        new DummyProperty());
    associationProperty = new AssociationIntProperty(123);
    categoryContainer.addCategory(new CategoryPath("nine"),
        associationProperty, new DummyProperty());
    Iterator<CategoryAttribute> iterator = categoryContainer.iterator();

    // count the number of tokens, and check there is one DummyAttribute
    int nCategories;
    int nDummyAttributes = 0;
    int nAssocAttributes = 0;
    for (nCategories = 0; iterator.hasNext(); nCategories++) {
      CategoryAttribute attribute = iterator.next();
      if (attribute.getProperty(DummyProperty.class) != null) {
        nDummyAttributes++;
      }
      if (attribute.getProperty(AssociationIntProperty.class) != null) {
        nAssocAttributes++;
      }
    }
    assertEquals("Wrong number of tokens", 5, nCategories);
    assertEquals("Wrong number of tokens with dummy properties", 3,
        nDummyAttributes);
    assertEquals("Wrong number of tokens with association properties", 2,
        nAssocAttributes);
  }

  @Test
  public void testAddNewCategoryWithProperty() throws FacetException {
    categoryContainer.addCategory(new CategoryPath("seven", "eight"),
        new DummyProperty());
    Iterator<CategoryAttribute> iterator = categoryContainer.iterator();

    // count the number of tokens, and check there is one DummyAttribute
    int nCategories;
    int nProperties = 0;
    for (nCategories = 0; iterator.hasNext(); nCategories++) {
      CategoryAttribute attribute = iterator.next();
      if (attribute.getProperty(DummyProperty.class) != null) {
        nProperties++;
      }
    }
    assertEquals("Wrong number of tokens", 4, nCategories);
    assertEquals("Wrong number of tokens with properties", 1, nProperties);
  }

  /**
   * Test addition of {@link CategoryAttribute} object without properties to a
   * {@link CategoryContainer}.
   */
  @Test
  public void testAddCategoryAttributeWithoutProperties()
      throws FacetException {
    CategoryAttribute newCA = new CategoryAttributeImpl(new CategoryPath(
        "seven", "eight"));
    categoryContainer.addCategory(newCA);
  }

  /**
   * Test addition of {@link CategoryAttribute} object with property to a
   * {@link CategoryContainer}.
   */
  @Test
  public void testAddCategoryAttributeWithProperty() throws FacetException {
    CategoryAttribute newCA = new CategoryAttributeImpl(new CategoryPath(
        "seven", "eight"));
    newCA.addProperty(new DummyProperty());
    categoryContainer.addCategory(newCA);
    Iterator<CategoryAttribute> iterator = categoryContainer.iterator();

    // count the number of tokens, and check there is one DummyAttribute
    int nCategories;
    int nProperties = 0;
    for (nCategories = 0; iterator.hasNext(); nCategories++) {
      CategoryAttribute attribute = iterator.next();
      if (attribute.getProperty(DummyProperty.class) != null) {
        nProperties++;
      }
    }
    assertEquals("Wrong number of tokens", 4, nCategories);
    assertEquals("Wrong number of tokens with properties", 1, nProperties);
  }

  /**
   * Verifies that a {@link CategoryAttributesStream} can be constructed from
   * {@link CategoryContainer} and produce the correct number of tokens.
   */
  @Test
  public void testCategoryAttributesStream() throws IOException {
    CategoryAttributesStream stream = new CategoryAttributesStream(
        categoryContainer);
    // count the number of tokens
    int nTokens;
    for (nTokens = 0; stream.incrementToken(); nTokens++) {
    }
    assertEquals("Wrong number of tokens", 3, nTokens);
  }

  /**
   * Test that {@link CategoryContainer} merges properties.
   */
  @Test
  public void testCategoryAttributeMerge() throws FacetException {
    categoryContainer.addCategory(initialCatgeories[0],
        new AssociationIntProperty(2));
    categoryContainer.addCategory(initialCatgeories[0],
        new AssociationIntProperty(15));

    Iterator<CategoryAttribute> iterator = categoryContainer.iterator();

    int nCategories;
    int nAssociations = 0;
    for (nCategories = 0; iterator.hasNext(); nCategories++) {
      CategoryAttribute ca = iterator.next();
      AssociationProperty aa = (AssociationProperty) ca
          .getProperty(AssociationIntProperty.class);
      if (aa != null) {
        assertEquals("Wrong association value", 17, aa.getAssociation());
        nAssociations++;
      }
    }
    assertEquals("Wrong number of tokens", 3, nCategories);
    assertEquals("Wrong number of tokens with associations", 1,
        nAssociations);
  }
  
  @Test
  public void testSerialization() throws Exception {
    AssociationProperty associationProperty = new AssociationIntProperty(
        49);
    categoryContainer.addCategory(new CategoryPath("five", "six"),
        new DummyProperty(), associationProperty);
    categoryContainer.addCategory(new CategoryPath("seven", "eight"),
        new DummyProperty());
    associationProperty = new AssociationIntProperty(123);
    categoryContainer.addCategory(new CategoryPath("nine"),
        associationProperty, new DummyProperty());
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    ObjectOutputStream out = new ObjectOutputStream(baos);
    out.writeObject(categoryContainer);
    out.close();
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    ObjectInputStream in = new ObjectInputStream(bais);
    assertEquals(
        "Original and deserialized CategoryContainer are different",
        categoryContainer, in.readObject());
  }
}