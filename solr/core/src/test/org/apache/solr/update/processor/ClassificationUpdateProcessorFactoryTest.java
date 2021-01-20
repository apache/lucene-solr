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
package org.apache.solr.update.processor;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link ClassificationUpdateProcessorFactory}
 */
public class ClassificationUpdateProcessorFactoryTest extends SolrTestCaseJ4 {
  private ClassificationUpdateProcessorFactory cFactoryToTest = new ClassificationUpdateProcessorFactory();
  @SuppressWarnings({"rawtypes"})
  private NamedList args = new NamedList<String>();

  @Before
  @SuppressWarnings({"unchecked"})
  public void initArgs() {
    args.add("inputFields", "inputField1,inputField2");
    args.add("classField", "classField1");
    args.add("predictedClassField", "classFieldX");
    args.add("algorithm", "bayes");
    args.add("knn.k", "9");
    args.add("knn.minDf", "8");
    args.add("knn.minTf", "10");
  }

  @Test
  public void init_fullArgs_shouldInitFullClassificationParams() {
    cFactoryToTest.init(args);
    ClassificationUpdateProcessorParams classificationParams = cFactoryToTest.getClassificationParams();

    String[] inputFieldNames = classificationParams.getInputFieldNames();
    assertEquals("inputField1", inputFieldNames[0]);
    assertEquals("inputField2", inputFieldNames[1]);
    assertEquals("classField1", classificationParams.getTrainingClassField());
    assertEquals("classFieldX", classificationParams.getPredictedClassField());
    assertEquals(ClassificationUpdateProcessorFactory.Algorithm.BAYES, classificationParams.getAlgorithm());
    assertEquals(8, classificationParams.getMinDf());
    assertEquals(10, classificationParams.getMinTf());
    assertEquals(9, classificationParams.getK());
  }

  @Test
  public void init_emptyInputFields_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("inputFields");
    try {
      cFactoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor 'inputFields' can not be null", e.getMessage());
    }
  }

  @Test
  public void init_emptyClassField_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("classField");
    try {
      cFactoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor 'classField' can not be null", e.getMessage());
    }
  }

  @Test
  public void init_emptyPredictedClassField_shouldDefaultToTrainingClassField() {
    args.removeAll("predictedClassField");

    cFactoryToTest.init(args);

    ClassificationUpdateProcessorParams classificationParams = cFactoryToTest.getClassificationParams();
    assertThat(classificationParams.getPredictedClassField(), is("classField1"));
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void init_unsupportedAlgorithm_shouldThrowExceptionWithDetailedMessage() {
    args.removeAll("algorithm");
    args.add("algorithm", "unsupported");
    try {
      cFactoryToTest.init(args);
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor Algorithm: 'unsupported' not supported", e.getMessage());
    }
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void init_unsupportedFilterQuery_shouldThrowExceptionWithDetailedMessage() {
    assumeWorkingMockito();
    
    UpdateRequestProcessor mockProcessor = mock(UpdateRequestProcessor.class);
    SolrQueryRequest mockRequest = mock(SolrQueryRequest.class);
    SolrQueryResponse mockResponse = mock(SolrQueryResponse.class);
    args.add("knn.filterQuery", "not supported query");
    try {
      cFactoryToTest.init(args);
      /* parsing failure happens because of the mocks, fine enough to check a proper exception propagation */
      cFactoryToTest.getInstance(mockRequest, mockResponse, mockProcessor);
    } catch (SolrException e) {
      assertEquals("Classification UpdateProcessor Training Filter Query: 'not supported query' is not supported", e.getMessage());
    }
  }

  @Test
  public void init_emptyArgs_shouldDefaultClassificationParams() {
    args.removeAll("algorithm");
    args.removeAll("knn.k");
    args.removeAll("knn.minDf");
    args.removeAll("knn.minTf");
    cFactoryToTest.init(args);
    ClassificationUpdateProcessorParams classificationParams = cFactoryToTest.getClassificationParams();

    assertEquals(ClassificationUpdateProcessorFactory.Algorithm.KNN, classificationParams.getAlgorithm());
    assertEquals(1, classificationParams.getMinDf());
    assertEquals(1, classificationParams.getMinTf());
    assertEquals(10, classificationParams.getK());
  }
}
