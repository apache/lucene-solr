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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.SkipExistingDocumentsProcessorFactory.SkipExistingDocumentsUpdateProcessor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class SkipExistingDocumentsProcessorFactoryTest {

  private BytesRef docId = new BytesRef();
  @SuppressWarnings({"rawtypes"})
  private SolrQueryRequest defaultRequest = new LocalSolrQueryRequest(null, new NamedList());
  
  @BeforeClass
  public static void beforeClass() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  // Tests for logic in the factory

  @Test(expected=SolrException.class)
  public void testExceptionIfSkipInsertParamNonBoolean() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipInsertIfExists", "false");
    factory.init(initArgs);
  }

  @Test(expected=SolrException.class)
  public void testExceptionIfSkipUpdateParamNonBoolean() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipUpdateIfMissing", 0);
    factory.init(initArgs);
  }

  @Test(expected=SolrException.class)
  public void testExceptionIfNextProcessorIsNull() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    factory.init(initArgs);

    factory.getInstance(defaultRequest, new SolrQueryResponse(), null);
  }

  @Test(expected=SolrException.class)
  public void testExceptionIfNextProcessorNotDistributed() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    factory.init(initArgs);
    UpdateRequestProcessor next = new BufferingRequestProcessor(null);

    factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
  }

  @Test
  public void testNoExceptionIfNextProcessorIsDistributed() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    factory.init(initArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
  }

  @Test
  public void testNoExceptionIfNextNextProcessorIsDistributed() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    factory.init(initArgs);
    UpdateRequestProcessor distProcessor = Mockito.mock(DistributedUpdateProcessor.class);
    UpdateRequestProcessor next = new BufferingRequestProcessor(distProcessor);

    factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
  }

  @Test
  public void testSkipInsertsAndUpdatesDefaultToTrueIfNotConfigured() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    factory.init(initArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
    assertTrue("Expected skipInsertIfExists to be true", processor.isSkipInsertIfExists());
    assertTrue("Expected skipUpdateIfMissing to be true", processor.isSkipUpdateIfMissing());
  }

  @Test
  public void testSkipInsertsFalseIfInInitArgs() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipInsertIfExists", false);
    factory.init(initArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
    assertFalse("Expected skipInsertIfExists to be false", processor.isSkipInsertIfExists());
    assertTrue("Expected skipUpdateIfMissing to be true", processor.isSkipUpdateIfMissing());
  }

  @Test
  public void testSkipUpdatesFalseIfInInitArgs() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipUpdateIfMissing", false);
    factory.init(initArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
    assertTrue("Expected skipInsertIfExists to be true", processor.isSkipInsertIfExists());
    assertFalse("Expected skipUpdateIfMissing to be false", processor.isSkipUpdateIfMissing());
  }

  @Test
  public void testSkipBothFalseIfInInitArgs() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipInsertIfExists", false);
    initArgs.add("skipUpdateIfMissing", false);
    factory.init(initArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(defaultRequest, new SolrQueryResponse(), next);
    assertFalse("Expected skipInsertIfExists to be false", processor.isSkipInsertIfExists());
    assertFalse("Expected skipUpdateIfMissing to be false", processor.isSkipUpdateIfMissing());
  }

  @Test
  public void testSkipInsertsFalseIfInitArgsTrueButFalseStringInRequest() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipInsertIfExists", true);
    factory.init(initArgs);
    NamedList<String> requestArgs = new NamedList<>();
    requestArgs.add("skipInsertIfExists", "false");
    SolrQueryRequest req = new LocalSolrQueryRequest(null, requestArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(req, new SolrQueryResponse(), next);
    assertFalse("Expected skipInsertIfExists to be false", processor.isSkipInsertIfExists());
    assertTrue("Expected skipUpdateIfMissing to be true", processor.isSkipUpdateIfMissing());
  }

  @Test
  public void testSkipUpdatesFalseIfInitArgsTrueButFalseBooleanInRequest() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipUpdateIfMissing", true);
    factory.init(initArgs);
    NamedList<Object> requestArgs = new NamedList<>();
    requestArgs.add("skipUpdateIfMissing", false);
    SolrQueryRequest req = new LocalSolrQueryRequest(null, requestArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(req, new SolrQueryResponse(), next);
    assertTrue("Expected skipInsertIfExists to be true", processor.isSkipInsertIfExists());
    assertFalse("Expected skipUpdateIfMissing to be false", processor.isSkipUpdateIfMissing());
  }

  @Test
  public void testSkipUpdatesTrueIfInitArgsFalseButTrueStringInRequest() {
    SkipExistingDocumentsProcessorFactory factory = new SkipExistingDocumentsProcessorFactory();
    NamedList<Object> initArgs = new NamedList<>();
    initArgs.add("skipInsertIfExists", true);
    initArgs.add("skipUpdateIfMissing", false);
    factory.init(initArgs);
    NamedList<Object> requestArgs = new NamedList<>();
    requestArgs.add("skipUpdateIfMissing", "true");
    SolrQueryRequest req = new LocalSolrQueryRequest(null, requestArgs);
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);

    SkipExistingDocumentsUpdateProcessor processor = factory.getInstance(req, new SolrQueryResponse(), next);
    assertTrue("Expected skipInsertIfExists to be true", processor.isSkipInsertIfExists());
    assertTrue("Expected skipUpdateIfMissing to be true", processor.isSkipUpdateIfMissing());
  }


  // Tests for logic in the processor

  @Test
  public void testSkippableInsertIsNotSkippedIfNotLeader() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, true, true));

    AddUpdateCommand cmd = createInsertUpdateCmd(defaultRequest);
    doReturn(false).when(processor).isLeader(cmd);
    doReturn(true).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next).processAdd(cmd);
  }

  @Test
  public void testSkippableInsertIsNotSkippedIfSkipInsertsFalse() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, false, false));

    AddUpdateCommand cmd = createInsertUpdateCmd(defaultRequest);
    doReturn(true).when(processor).isLeader(cmd);
    doReturn(true).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next).processAdd(cmd);
  }

  @Test
  public void testSkippableInsertIsSkippedIfSkipInsertsTrue() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, true, false));

    AddUpdateCommand cmd = createInsertUpdateCmd(defaultRequest);
    doReturn(true).when(processor).isLeader(cmd);
    doReturn(true).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next, never()).processAdd(cmd);
  }

  @Test
  public void testNonSkippableInsertIsNotSkippedIfSkipInsertsTrue() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, true, false));

    AddUpdateCommand cmd = createInsertUpdateCmd(defaultRequest);
    doReturn(true).when(processor).isLeader(cmd);
    doReturn(false).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next).processAdd(cmd);
  }

  @Test
  public void testSkippableUpdateIsNotSkippedIfNotLeader() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, true, true));

    AddUpdateCommand cmd = createAtomicUpdateCmd(defaultRequest);
    doReturn(false).when(processor).isLeader(cmd);
    doReturn(false).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next).processAdd(cmd);
  }

  @Test
  public void testSkippableUpdateIsNotSkippedIfSkipUpdatesFalse() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, false, false));

    AddUpdateCommand cmd = createAtomicUpdateCmd(defaultRequest);
    doReturn(true).when(processor).isLeader(cmd);
    doReturn(false).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next).processAdd(cmd);
  }

  @Test
  public void testSkippableUpdateIsSkippedIfSkipUpdatesTrue() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, false, true));

    AddUpdateCommand cmd = createAtomicUpdateCmd(defaultRequest);
    doReturn(true).when(processor).isLeader(cmd);
    doReturn(false).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next, never()).processAdd(cmd);
  }

  @Test
  public void testNonSkippableUpdateIsNotSkippedIfSkipUpdatesTrue() throws IOException {
    UpdateRequestProcessor next = Mockito.mock(DistributedUpdateProcessor.class);
    SkipExistingDocumentsUpdateProcessor processor
            = Mockito.spy(new SkipExistingDocumentsUpdateProcessor(defaultRequest, next, false, true));

    AddUpdateCommand cmd = createAtomicUpdateCmd(defaultRequest);
    doReturn(true).when(processor).isLeader(cmd);
    doReturn(true).when(processor).doesDocumentExist(docId);

    processor.processAdd(cmd);
    verify(next).processAdd(cmd);
  }

  private AddUpdateCommand createInsertUpdateCmd(SolrQueryRequest req) {
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.setIndexedId(docId);
    cmd.solrDoc = new SolrInputDocument();
    assertFalse(AtomicUpdateDocumentMerger.isAtomicUpdate(cmd));
    return cmd;
  }

  private AddUpdateCommand createAtomicUpdateCmd(SolrQueryRequest req) {
    AddUpdateCommand cmd = new AddUpdateCommand(req);
    cmd.setIndexedId(docId);
    cmd.solrDoc = new SolrInputDocument();
    cmd.solrDoc.addField("last_name", ImmutableMap.of("set", "Smith"));
    assertTrue(AtomicUpdateDocumentMerger.isAtomicUpdate(cmd));
    return cmd;
  }
}
