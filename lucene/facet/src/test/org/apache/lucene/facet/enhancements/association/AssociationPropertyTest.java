package org.apache.lucene.facet.enhancements.association;

import org.junit.Test;

import org.apache.lucene.facet.FacetException;
import org.apache.lucene.facet.enhancements.association.AssociationFloatProperty;
import org.apache.lucene.facet.enhancements.association.AssociationIntProperty;
import org.apache.lucene.facet.enhancements.association.AssociationProperty;
import org.apache.lucene.util.LuceneTestCase;

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

/** Test {@link AssociationProperty}-ies. */
public class AssociationPropertyTest extends LuceneTestCase {

  @Test
  public void testAssociationCountProperty() throws FacetException {
    AssociationProperty aa1 = new AssociationIntProperty(5);
    AssociationProperty aa2 = new AssociationIntProperty(3);
    assertEquals("Wrong association for property", 5, aa1.getAssociation());
    assertEquals("Wrong association for property", 3, aa2.getAssociation());
    aa1.merge(aa2);
    assertEquals("Wrong association for property", 8, aa1.getAssociation());
  }

  @Test
  public void testAssociationFloatProperty() throws FacetException {
    AssociationFloatProperty aa1 = new AssociationFloatProperty(5);
    AssociationFloatProperty aa2 = new AssociationFloatProperty(3);
    assertEquals("Wrong association for property", 5.0, aa1.getFloatAssociation(), 0.00001);
    assertEquals("Wrong association for property", 3.0, aa2.getFloatAssociation(), 0.00001);
    aa1.merge(aa2);
    assertEquals("Wrong association for property", 8.0, aa1.getFloatAssociation(), 0.00001);
  }

  @Test
  public void testEquals() {
    AssociationProperty aa1 = new AssociationIntProperty(5);
    AssociationProperty aa2 = new AssociationIntProperty(5);
    AssociationProperty aa3 = new AssociationFloatProperty(5);
    AssociationProperty aa4 = new AssociationFloatProperty(5);

    assertTrue("Should be equal", aa1.equals(aa1));
    assertTrue("Should be equal", aa1.equals(aa2));
    assertFalse("Should not be equal", aa1.equals(aa3));
    assertTrue("Should be equal", aa3.equals(aa3));
    assertTrue("Should be equal", aa3.equals(aa4));
  }
  
}
