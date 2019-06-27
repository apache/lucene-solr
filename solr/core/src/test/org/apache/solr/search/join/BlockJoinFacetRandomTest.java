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
package org.apache.solr.search.join;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlockJoinFacetRandomTest extends SolrTestCaseJ4 {
  private static String handler;
  private static final int NUMBER_OF_PARENTS = 10;
  private static final int NUMBER_OF_VALUES = 5;
  private static final int NUMBER_OF_CHILDREN = 5;
  private static final String[] facetFields = {"brand", "category", "color", "size", "type"};
  private static final String[] otherValues = {"x_", "y_", "z_"};
  public static final String PARENT_VALUE_PREFIX = "prn_";
  public static final String CHILD_VALUE_PREFIX = "chd_";


  private static Facet[] facets;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-blockjoinfacetcomponent.xml", "schema-blockjoinfacetcomponent.xml");
    handler = random().nextBoolean() ? "/blockJoinDocSetFacetRH":"/blockJoinFacetRH";
    facets = createFacets();
    createIndex();
  }

  public static void createIndex() throws Exception {
    int i = 0;
    List<List<List<String>>> blocks = createBlocks();
    for (List<List<String>> block : blocks) {
      List<XmlDoc> updBlock = new ArrayList<>();
      for (List<String> blockFields : block) {
        blockFields.add("id");
        blockFields.add(Integer.toString(i));
        updBlock.add(doc(blockFields.toArray(new String[blockFields.size()])));
        i++;
      }
      //got xmls for every doc. now nest all into the last one
      XmlDoc parentDoc = updBlock.get(updBlock.size() - 1);
      parentDoc.xml = parentDoc.xml.replace("</doc>",
          updBlock.subList(0, updBlock.size() - 1).toString().replaceAll("[\\[\\]]", "") + "</doc>");
      assertU(add(parentDoc));

      if (random().nextBoolean()) {
        assertU(commit());
        // force empty segment (actually, this will no longer create an empty segment, only a new segments_n)
        if (random().nextBoolean()) {
          assertU(commit());
        }
      }
    }
    assertU(commit());
    assertQ(req("q", "*:*"), "//*[@numFound='" + i + "']");
  }

  private static List<List<List<String>>> createBlocks() {
    List<List<List<String>>> blocks = new ArrayList<>();
    for (int i = 0; i < NUMBER_OF_PARENTS; i++) {
      List<List<String>> block = createChildrenBlock(i, facets);
      List<String> fieldsList = new LinkedList<>();
      fieldsList.add("parent_s");
      fieldsList.add(parent(i));
      for (Facet facet : facets) {
        for (RandomFacetValue facetValue : facet.facetValues) {
          RandomParentPosting posting = facetValue.postings[i];
          if (posting.parentHasOwnValue) {
            fieldsList.add(facet.getFieldNameForIndex());
            fieldsList.add(facetValue.facetValue);
          } else if (facet.multiValued && random().nextBoolean()) {
            fieldsList.add(facet.getFieldNameForIndex());
            fieldsList.add(someOtherValue(facet.fieldType));
          }
        }
        if (facet.additionalValueIsAllowedForParent(i)&&random().nextBoolean()) {
          fieldsList.add(facet.getFieldNameForIndex());
          fieldsList.add(someOtherValue(facet.fieldType));
        }
      }
      block.add(fieldsList);
      blocks.add(block);
    }
    Collections.shuffle(blocks, random());
    return blocks;
  }

  private static List<List<String>> createChildrenBlock(int parentIndex, Facet[] facets) {
    List<List<String>> block = new ArrayList<>();
    for (int i = 0; i < NUMBER_OF_CHILDREN; i++) {
      List<String> fieldsList = new LinkedList<>();

      fieldsList.add("child_s");
      fieldsList.add(child(i));
      fieldsList.add("parentchild_s");
      fieldsList.add(parentChild(parentIndex, i));
      for (Facet facet : facets) {
        for (RandomFacetValue facetValue : facet.facetValues) {
          RandomParentPosting posting = facetValue.postings[parentIndex];
          if (posting.childrenHaveValue[i]) {
            fieldsList.add(facet.getFieldNameForIndex());
            fieldsList.add(facetValue.facetValue);
          } else if (facet.multiValued && random().nextBoolean()) {
            fieldsList.add(facet.getFieldNameForIndex());
            fieldsList.add(someOtherValue(facet.fieldType));
          }
        }
        if (facet.additionalValueIsAllowedForChild(parentIndex,i)&&random().nextBoolean()) {
          fieldsList.add(facet.getFieldNameForIndex());
          fieldsList.add(someOtherValue(facet.fieldType));
        }
      }
      block.add(fieldsList);
    }
    Collections.shuffle(block, random());
    return block;
  }

  private static String parent(int docNumber) {
    return fieldValue(PARENT_VALUE_PREFIX, docNumber);
  }

  private static String child(int docNumber) {
    return fieldValue(CHILD_VALUE_PREFIX, docNumber);
  }

  private static String someOtherValue(FieldType fieldType) {
    int randomValue = random().nextInt(NUMBER_OF_VALUES) + NUMBER_OF_VALUES;
    switch (fieldType) {
      case String :
        int index = random().nextInt(otherValues.length);
        return otherValues[index]+randomValue;
      case Float:
        return createFloatValue(randomValue);
      default:
        return String.valueOf(randomValue);

    }

  }

  private static String createFloatValue(int intValue) {
    return intValue + ".01";
  }

  private static String fieldValue(String valuePrefix, int docNumber) {
    return valuePrefix + docNumber;
  }

  private static String parentChild(int parentIndex, int childIndex) {
    return parent(parentIndex) + "_" + child(childIndex);
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    if (null != h) {
      assertU(delQ("*:*"));
      optimize();
      assertU((commit()));
    }
  }

  @Test
  public void testValidation() throws Exception {
    assertQ("Component is ignored",
        req("q", "+parent_s:(prn_1 prn_2)", "qt", handler)
        , "//*[@numFound='2']"
        , "//doc/str[@name=\"parent_s\"]='prn_1'"
        , "//doc/str[@name=\"parent_s\"]='prn_2'"
    );

    assertQEx("Validation exception is expected because query is not ToParentBlockJoinQuery",
        BlockJoinFacetComponent.NO_TO_PARENT_BJQ_MESSAGE,
        req(
            "q", "t",
            "df", "name",
            "qt", handler,
            BlockJoinFacetComponent.CHILD_FACET_FIELD_PARAMETER, facetFields[0]
        ),
        SolrException.ErrorCode.BAD_REQUEST
    );

    assertQEx("Validation exception is expected because facet field is not defined in schema",
        req(
            "q", "{!parent which=\"parent_s:[* TO *]\"}child_s:chd_1",
            "qt", handler,
            BlockJoinFacetComponent.CHILD_FACET_FIELD_PARAMETER, "undefinedField"
        ),
        SolrException.ErrorCode.BAD_REQUEST
    );
  }

  @Test
  public void testAllDocs() throws Exception {
    int[] randomFacets = getRandomArray(facets.length);
    assertQ("Random facets for all docs should be calculated",
        req(randomFacetsRequest(null, null, null, null, null, randomFacets)),
        expectedResponse(null, null, randomFacets));
  }

  @Test
  public void testRandomParentsAllChildren() throws Exception {
    int[] randomParents = getRandomArray(NUMBER_OF_PARENTS);
    int[] randomFacets = getRandomArray(facets.length);
    assertQ("Random facets for random parents should be calculated",
        req(randomFacetsRequest(randomParents, null, null, null, null, randomFacets)),
        expectedResponse(randomParents, null, randomFacets));
  }

  @Test
  public void testRandomChildrenAllParents() throws Exception {
    int[] randomChildren = getRandomArray(NUMBER_OF_CHILDREN);
    int[] randomFacets = getRandomArray(facets.length);
    assertQ("Random facets for all parent docs should be calculated",
        req(randomFacetsRequest(null, randomChildren, null, null, null, randomFacets)),
        expectedResponse(null, randomChildren, randomFacets));
  }

  @Test
  public void testRandomChildrenRandomParents() throws Exception {
    int[] randomParents = getRandomArray(NUMBER_OF_PARENTS);
    int[] randomChildren = getRandomArray(NUMBER_OF_CHILDREN);
    int[] randomFacets = getRandomArray(facets.length);
    assertQ("Random facets for all parent docs should be calculated",
        req(randomFacetsRequest(randomParents, randomChildren, null, null, null, randomFacets)),
        expectedResponse(randomParents, randomChildren, randomFacets));
  }

  @Test
  public void testRandomChildrenRandomParentsRandomRelations() throws Exception {
    int[] randomParents = getRandomArray(NUMBER_OF_PARENTS);
    int[] randomChildren = getRandomArray(NUMBER_OF_CHILDREN);
    int[] parentRelations = getRandomArray(NUMBER_OF_PARENTS);
    int[] childRelations = getRandomArray(NUMBER_OF_CHILDREN);
    int[] randomFacets = getRandomArray(facets.length);
    assertQ("Random facets for all parent docs should be calculated",
        req(randomFacetsRequest(randomParents, randomChildren, parentRelations, childRelations, null, randomFacets)),
        expectedResponse(intersection(randomParents, parentRelations),
            intersection(randomChildren, childRelations), randomFacets));
  }

  @Test
  public void testRandomFilters() throws Exception {
    int[] randomParents = getRandomArray(NUMBER_OF_PARENTS);
    int[] randomChildren = getRandomArray(NUMBER_OF_CHILDREN);
    int[] parentRelations = getRandomArray(NUMBER_OF_PARENTS);
    int[] childRelations = getRandomArray(NUMBER_OF_CHILDREN);
    int[] randomParentFilters = getRandomArray(NUMBER_OF_PARENTS);
    int[] randomFacets = getRandomArray(facets.length);
    assertQ("Random facets for all parent docs should be calculated",
        req(randomFacetsRequest(randomParents, randomChildren, parentRelations, childRelations, randomParentFilters, randomFacets)),
        expectedResponse(intersection(intersection(randomParents, parentRelations), randomParentFilters),
            intersection(randomChildren, childRelations), randomFacets));
  }

  private int[] intersection(int[] firstArray, int[] secondArray) {
    Set<Integer> firstSet = new HashSet<>();
    for (int i : firstArray) {
      firstSet.add(i);
    }
    Set<Integer> secondSet = new HashSet<>();
    for (int i : secondArray) {
      secondSet.add(i);
    }
    firstSet.retainAll(secondSet);
    int[] result = new int[firstSet.size()];
    int i = 0;
    for (Integer integer : firstSet) {
      result[i++] = integer;
    }
    return result;
  }

  private String[] randomFacetsRequest(int[] parents, int[] children,
                                       int[] parentRelations, int[] childRelations,
                                       int[] parentFilters, int[] facetNumbers) {
    List<String> params = new ArrayList<>(Arrays.asList(
        "q", parentsQuery(parents),
        "qt",handler,
        "pq","parent_s:[* TO *]",
        "chq", childrenQuery(children, parentRelations, childRelations),
        "fq", flatQuery(parentFilters, "parent_s", PARENT_VALUE_PREFIX)
        ));
      for (int facetNumber : facetNumbers) {
        params .add(BlockJoinFacetComponent.CHILD_FACET_FIELD_PARAMETER);
        params .add(facets[facetNumber].getFieldNameForIndex());
      }
    return params.toArray(new String[params.size()]);
  }

  private String parentsQuery(int[] parents) {
    String result;
    if (parents == null) {
      result = "{!parent which=$pq v=$chq}";
    } else {
      result = flatQuery(parents, "parent_s", PARENT_VALUE_PREFIX) + " +_query_:\"{!parent which=$pq v=$chq}\"";
    }
    return result;
  }

  private String flatQuery(int[] docNumbers, final String fieldName, String fieldValuePrefix) {
    String result;
    if (docNumbers == null) {
      result = "+" + fieldName + ":[* TO *]";
    } else {
      StringBuilder builder = new StringBuilder("+" + fieldName +":(");
      if (docNumbers.length == 0) {
        builder.append("match_nothing_value");
      } else {
        for (int docNumber : docNumbers) {
          builder.append(fieldValue(fieldValuePrefix, docNumber));
          builder.append(" ");
        }
        builder.deleteCharAt(builder.length() - 1);
      }
      builder.append(")");
      result = builder.toString();
    }
    return result;
  }

  private String childrenQuery(int[] children, int[] parentRelations, int[] childRelations) {
    StringBuilder builder = new StringBuilder();
    builder.append(flatQuery(children, "child_s", CHILD_VALUE_PREFIX));
    if (parentRelations == null) {
      if (childRelations == null) {
        builder.append(" +parentchild_s:[* TO *]");
      } else {
        builder.append(" +parentchild_s:(");
        if (childRelations.length == 0) {
          builder.append("match_nothing_value");
        } else {
          for (int childRelation : childRelations) {
            for (int i = 0; i < NUMBER_OF_PARENTS; i++) {
              builder.append(parentChild(i, childRelation));
              builder.append(" ");
            }
          }
          builder.deleteCharAt(builder.length() - 1);
        }
        builder.append(")");
      }
    } else {
      builder.append(" +parentchild_s:(");
      if (parentRelations.length == 0) {
        builder.append("match_nothing_value");
      } else {
        if (childRelations == null) {
          for (int parentRelation : parentRelations) {
              for (int i = 0; i < NUMBER_OF_CHILDREN; i++) {
                builder.append(parentChild(parentRelation, i));
                builder.append(" ");
              }
          }
        } else if (childRelations.length == 0) {
          builder.append("match_nothing_value");
        } else {
          for (int parentRelation : parentRelations) {

              for (int childRelation : childRelations) {
                builder.append(parentChild(parentRelation, childRelation));
                builder.append(" ");
              }
          }
          builder.deleteCharAt(builder.length() - 1);
        }
      }
      builder.append(")");
    }
    return builder.toString();
  }

  private String[] expectedResponse(int[] parents, int[] children, int[] facetNumbers) {
    List<String> result = new LinkedList<>();
    if (children != null && children.length == 0) {
      result.add("//*[@numFound='" + 0 + "']");
    } else {
      if (parents == null) {
        result.add("//*[@numFound='" + NUMBER_OF_PARENTS + "']");
        for (int i = 0; i < NUMBER_OF_PARENTS; i++) {
          result.add("//doc/str[@name=\"parent_s\"]='" + parent(i) + "'");
        }
      } else {
        result.add("//*[@numFound='" + parents.length + "']");
        for (int parent : parents) {
          result.add("//doc/str[@name=\"parent_s\"]='" + parent(parent) + "'");
        }
      }
    }
    if (facetNumbers != null) {
      for (int facetNumber : facetNumbers) {
        result.add("//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" + facets[facetNumber].getFieldNameForIndex() + "']");
        RandomFacetValue[] facetValues = facets[facetNumber].facetValues;
        for (RandomFacetValue facetValue : facetValues) {
          int expectedFacetCount = facetValue.getFacetCount(parents, children);
          if (expectedFacetCount > 0) {
            result.add("//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='" +
            facets[facetNumber].getFieldNameForIndex() + "']/int[@name='" + 
                facetValue.facetValue + "' and text()='" + expectedFacetCount + "']");
          }
        }
      }
    }
    return result.toArray(new String[result.size()]);
  }

  private static Facet[] createFacets() {
    int[] facetsToCreate = getRandomArray(facetFields.length);
    Facet[] facets = new Facet[facetsToCreate.length];
    int i = 0;
    for (int facetNumber : facetsToCreate) {
      facets[i++] = new Facet(facetFields[facetNumber]);
    }
    return facets;
  }

  private static int[] getRandomArray(int maxNumber) {
    int[] buffer = new int[maxNumber];
    int count = 0;
    for (int i = 0; i < maxNumber; i++) {
      if (random().nextBoolean()) {
        buffer[count++] = i;
      }
    }
    int[] result = new int[count];
    System.arraycopy(buffer, 0, result, 0, count);
    return result;
  }

  private static class Facet {
    private String fieldName;
    private boolean multiValued = true;
    FieldType fieldType;
    RandomFacetValue[] facetValues;

    Facet(String fieldName) {
      this.fieldName = fieldName;
      fieldType = FieldType.values()[random().nextInt(FieldType.values().length)];
      if ( FieldType.String.equals(fieldType)) {
        // sortedDocValues are supported for string fields only
        multiValued = random().nextBoolean();
      }

      fieldType = FieldType.String;
      facetValues = new RandomFacetValue[NUMBER_OF_VALUES];
      for (int i = 0; i < NUMBER_OF_VALUES; i++) {
        String value = createRandomValue(i);
        facetValues[i] = new RandomFacetValue(value);
      }
      if (!multiValued) {
        makeValuesSingle();
      }
    }

    private String createRandomValue(int i) {
      switch( fieldType ) {
        case String:
          return fieldName.substring(0, 2) + "_" + i;
        case Float:
          return createFloatValue(i);
        default:
          return String.valueOf(i);
      }
    }

    String getFieldNameForIndex() {
      String multiValuedPostfix = multiValued ? "_multi" : "_single";
      return fieldName + fieldType.fieldPostfix + multiValuedPostfix;
    }

    private void makeValuesSingle() {
      for ( int i = 0; i < NUMBER_OF_PARENTS; i++) {
        List<Integer> values = getValuesForParent(i);
        if ( values.size() > 0) {
          int singleValueOrd = values.get(random().nextInt(values.size()));
          setSingleValueForParent(i,singleValueOrd);
        }
        for ( int j=0; j < NUMBER_OF_CHILDREN; j++) {
          values = getValuesForChild(i,j);
          if ( values.size() > 0 ) {
            int singleValueOrd = values.get(random().nextInt(values.size()));
            setSingleValueForChild(i, j, singleValueOrd);
          }
        }
      }
    }

    private List<Integer> getValuesForParent(int parentNumber) {
      List<Integer> result = new ArrayList<>();
      for (int i = 0; i<NUMBER_OF_VALUES; i++) {
        if (facetValues[i].postings[parentNumber].parentHasOwnValue) {
          result.add(i);
        }
      }
      return result;
    }

    private void setSingleValueForParent(int parentNumber, int valueOrd) {
      for (int i = 0; i<NUMBER_OF_VALUES; i++) {
        facetValues[i].postings[parentNumber].parentHasOwnValue = (i == valueOrd);
      }
    }

    boolean additionalValueIsAllowedForParent(int parentNumber) {
      return multiValued || getValuesForParent(parentNumber).size() == 0;
    }

    private List<Integer> getValuesForChild(int parentNumber, int childNumber) {
      List<Integer> result = new ArrayList<>();
      for (int i = 0; i<NUMBER_OF_VALUES; i++) {
        if (facetValues[i].postings[parentNumber].childrenHaveValue[childNumber]) {
          result.add(i);
        }
      }
      return result;
    }

    private void setSingleValueForChild(int parentNumber, int childNumber, int valueOrd) {
      for (int i = 0; i<NUMBER_OF_VALUES; i++) {
        facetValues[i].postings[parentNumber].childrenHaveValue[childNumber] = (i == valueOrd);
      }
    }

    boolean additionalValueIsAllowedForChild(int parentNumber, int childNumber) {
      return multiValued || getValuesForChild(parentNumber,childNumber).size() == 0;
    }
  }

  private static class RandomFacetValue {
    final String facetValue;
    // rootDoc, level, docsOnLevel
    RandomParentPosting[] postings;


    public RandomFacetValue(String facetValue) {
      this.facetValue = facetValue;
      postings = new RandomParentPosting[NUMBER_OF_PARENTS];
      for (int i = 0; i < NUMBER_OF_PARENTS; i++) {
        postings[i] = new RandomParentPosting(random().nextBoolean());
      }
    }

    int getFacetCount(int[] parentNumbers, int[] childNumbers) {
      int result = 0;
      if (parentNumbers != null) {
        for (int parentNumber : parentNumbers) {
          if (postings[parentNumber].isMatched(childNumbers)) {
            result++;
          }
        }
      } else {
        for (int i = 0; i < NUMBER_OF_PARENTS; i++) {
          if (postings[i].isMatched(childNumbers)) {
            result++;
          }
        }
      }
      return result;
    }
  }

  private enum  FieldType {
    Integer("_i"),
    Float("_f"),
    String("_s");
    private final String fieldPostfix;

    FieldType(String fieldPostfix) {
      this.fieldPostfix = fieldPostfix;
    }
  }

  private static class RandomParentPosting {
    boolean parentHasOwnValue;
    boolean[] childrenHaveValue;

    RandomParentPosting(boolean expected) {
      childrenHaveValue = new boolean[NUMBER_OF_CHILDREN];
      if (expected) {
        // don't count parents
        parentHasOwnValue = false;// random().nextBoolean();
        if (random().nextBoolean()) {
          for (int i = 0; i < NUMBER_OF_CHILDREN; i++) {
            childrenHaveValue[i] = random().nextBoolean();
          }
        }
      }
    }

    boolean isMatched(int[] childNumbers) {
      boolean result = parentHasOwnValue && (childNumbers == null || childNumbers.length > 0);
      if (!result) {
        if (childNumbers == null) {
          for (boolean childHasValue : childrenHaveValue) {
            result = childHasValue;
            if (result) {
              break;
            }
          }
        } else {
          for (int child : childNumbers) {
            result = childrenHaveValue[child];
            if (result) {
              break;
            }
          }
        }
      }
      return result;
    }
  }
}
