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
package org.apache.solr.client.solrj;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.impl.StreamingBinaryResponseParser;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;

/**
 * Abstraction through which all communication with a Solr server may be routed
 *
 * @since 5.0, replaced {@code SolrServer}
 */
public abstract class SolrClient implements Serializable, Closeable {

  private static final long serialVersionUID = 1L;

  private DocumentObjectBinder binder;

  /**
   * Adds a collection of documents
   *
   * @param collection the Solr collection to add documents to
   * @param docs  the collection of documents
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since Solr 5.1
   */
  public UpdateResponse add(String collection, Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(collection, docs, -1);
  }

  /**
   * Adds a collection of documents
   *
   * @param docs  the collection of documents
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    return add(null, docs);
  }

  /**
   * Adds a collection of documents, specifying max time before they become committed
   *
   * @param collection the Solr collection to add documents to
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since Solr 5.1
   */
  public UpdateResponse add(String collection, Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(docs);
    req.setCommitWithin(commitWithinMs);
    return req.process(this, collection);
  }

  /**
   * Adds a collection of documents, specifying max time before they become committed
   *
   * @param docs  the collection of documents
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since Solr 3.5
   */
  public UpdateResponse add(Collection<SolrInputDocument> docs, int commitWithinMs) throws SolrServerException, IOException {
    return add(null, docs, commitWithinMs);
  }

  /**
   * Adds a single document
   *
   * @param collection the Solr collection to add the document to
   * @param doc  the input document
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(String collection, SolrInputDocument doc) throws SolrServerException, IOException {
    return add(collection, doc, -1);
  }

  /**
   * Adds a single document
   *
   * Many {@link SolrClient} implementations have drastically slower indexing performance when documents are added
   * individually.  Document batching generally leads to better indexing performance and should be used whenever
   * possible.
   *
   * @param doc  the input document
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(SolrInputDocument doc) throws SolrServerException, IOException {
    return add(null, doc);
  }

  /**
   * Adds a single document specifying max time before it becomes committed
   *
   * @param collection the Solr collection to add the document to
   * @param doc  the input document
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 5.1
   */
  public UpdateResponse add(String collection, SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(doc);
    req.setCommitWithin(commitWithinMs);
    return req.process(this, collection);
  }

  /**
   * Adds a single document specifying max time before it becomes committed
   *
   * @param doc  the input document
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 3.5
   */
  public UpdateResponse add(SolrInputDocument doc, int commitWithinMs) throws SolrServerException, IOException {
    return add(null, doc, commitWithinMs);
  }

  /**
   * Adds the documents supplied by the given iterator.
   *
   * @param collection the Solr collection to add the documents to
   * @param docIterator
   *          the iterator which returns SolrInputDocument instances
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(String collection, Iterator<SolrInputDocument> docIterator)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(docIterator);
    return req.process(this, collection);
  }

  /**
   * Adds the documents supplied by the given iterator.
   *
   * @param docIterator
   *          the iterator which returns SolrInputDocument instances
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse add(Iterator<SolrInputDocument> docIterator) throws SolrServerException, IOException {
    return add(null, docIterator);
  }

  /**
   * Adds a single bean
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   * <p>
   * Many {@link SolrClient} implementations have drastically slower indexing performance when documents are added
   * individually.  Document batching generally leads to better indexing performance and should be used whenever
   * possible.
   *
   * @param collection to Solr collection to add documents to
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(String collection, Object obj) throws IOException, SolrServerException {
    return addBean(collection, obj, -1);
  }

  /**
   * Adds a single bean
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(Object obj) throws IOException, SolrServerException {
    return addBean(null, obj, -1);
  }

  /**
   * Adds a single bean specifying max time before it becomes committed
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection to Solr collection to add documents to
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(String collection, Object obj, int commitWithinMs) throws IOException, SolrServerException {
    return add(collection, getBinder().toSolrInputDocument(obj), commitWithinMs);
  }

  /**
   * Adds a single bean specifying max time before it becomes committed
   *
   * The bean is converted to a {@link SolrInputDocument} by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param obj  the input bean
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBean(Object obj, int commitWithinMs) throws IOException, SolrServerException {
    return add(null, getBinder().toSolrInputDocument(obj), commitWithinMs);
  }

  /**
   * Adds a collection of beans
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection the Solr collection to add documents to
   * @param beans  the collection of beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(String collection, Collection<?> beans) throws SolrServerException, IOException {
    return addBeans(collection, beans, -1);
  }

  /**
   * Adds a collection of beans
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param beans  the collection of beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(Collection<?> beans) throws SolrServerException, IOException {
    return addBeans(null, beans, -1);
  }

  /**
   * Adds a collection of beans specifying max time before they become committed
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param collection the Solr collection to add documents to
   * @param beans  the collection of beans
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @see SolrClient#getBinder()
   *
   * @since solr 5.1
   */
  public UpdateResponse addBeans(String collection, Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    DocumentObjectBinder binder = this.getBinder();
    ArrayList<SolrInputDocument> docs =  new ArrayList<>(beans.size());
    for (Object bean : beans) {
      docs.add(binder.toSolrInputDocument(bean));
    }
    return add(collection, docs, commitWithinMs);
  }

  /**
   * Adds a collection of beans specifying max time before they become committed
   *
   * The beans are converted to {@link SolrInputDocument}s by the client's
   * {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder}
   *
   * @param beans  the collection of beans
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   *
   * @see SolrClient#getBinder()
   *
   * @since solr 3.5
   */
  public UpdateResponse addBeans(Collection<?> beans, int commitWithinMs) throws SolrServerException, IOException {
    return addBeans(null, beans, commitWithinMs);
  }

  /**
   * Adds the beans supplied by the given iterator.
   *
   * @param collection the Solr collection to add the documents to
   * @param beanIterator
   *          the iterator which returns Beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(String collection, final Iterator<?> beanIterator)
      throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.setDocIterator(new Iterator<SolrInputDocument>() {

      @Override
      public boolean hasNext() {
        return beanIterator.hasNext();
      }

      @Override
      public SolrInputDocument next() {
        Object o = beanIterator.next();
        if (o == null) return null;
        return getBinder().toSolrInputDocument(o);
      }

      @Override
      public void remove() {
        beanIterator.remove();
      }
    });
    return req.process(this, collection);
  }

  /**
   * Adds the beans supplied by the given iterator.
   *
   * @param beanIterator
   *          the iterator which returns Beans
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} from the server
   *
   * @throws IOException         if there is a communication error with the server
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse addBeans(final Iterator<?> beanIterator) throws SolrServerException, IOException {
    return addBeans(null, beanIterator);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * <p>
   * Be very careful when triggering commits from the client side.  Commits are heavy operations and WILL impact Solr
   * performance when executed too often or too close together.  Instead, consider using 'commitWithin' when adding documents
   * or rely on your core's/collection's 'autoCommit' settings.
   *
   * @param collection the Solr collection to send the commit to
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(String collection) throws SolrServerException, IOException {
    return commit(collection, true, true);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   * <p>
   * Be very careful when triggering commits from the client side.  Commits are heavy operations and WILL impact Solr
   * performance when executed too often or too close together.  Instead, consider using 'commitWithin' when adding documents
   * or rely on your core's/collection's 'autoCommit' settings.
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit() throws SolrServerException, IOException {
    return commit(null, true, true);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * Be very careful when triggering commits from the client side.  Commits are heavy operations and WILL impact Solr
   * performance when executed too often or too close together.  Instead, consider using 'commitWithin' when adding documents
   * or rely on your core's/collection's 'autoCommit' settings.
   *
   * @param collection the Solr collection to send the commit to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher)
      throws SolrServerException, IOException {
    return new UpdateRequest()
        .setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher)
        .process(this, collection);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * Be very careful when triggering commits from the client side.  Commits are heavy operations and WILL impact Solr
   * performance when executed too often or too close together.  Instead, consider using 'commitWithin' when adding documents
   * or rely on your core's/collection's 'autoCommit' settings.
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return commit(null, waitFlush, waitSearcher);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * Be very careful when triggering commits from the client side.  Commits are heavy operations and WILL impact Solr
   * performance when executed too often or too close together.  Instead, consider using 'commitWithin' when adding documents
   * or rely on your core's/collection's 'autoCommit' settings.
   *
   * @param collection the Solr collection to send the commit to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   * @param softCommit makes index changes visible while neither fsync-ing index files
   *                   nor writing a new index descriptor
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(String collection, boolean waitFlush, boolean waitSearcher, boolean softCommit)
      throws SolrServerException, IOException {
    return new UpdateRequest()
        .setAction(UpdateRequest.ACTION.COMMIT, waitFlush, waitSearcher, softCommit)
        .process(this, collection);
  }

  /**
   * Performs an explicit commit, causing pending documents to be committed for indexing
   *
   * Be very careful when triggering commits from the client side.  Commits are heavy operations and WILL impact Solr
   * performance when executed too often or too close together.  Instead, consider using 'commitWithin' when adding documents
   * or rely on your core's/collection's 'autoCommit' settings.
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as the
   *                      main query searcher, making the changes visible
   * @param softCommit makes index changes visible while neither fsync-ing index files
   *                   nor writing a new index descriptor
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse commit(boolean waitFlush, boolean waitSearcher, boolean softCommit)
      throws SolrServerException, IOException {
    return commit(null, waitFlush, waitSearcher, softCommit);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param collection the Solr collection to send the optimize to
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(String collection) throws SolrServerException, IOException {
    return optimize(collection, true, true, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * waitFlush=true and waitSearcher=true to be inline with the defaults for plain HTTP access
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize() throws SolrServerException, IOException {
    return optimize(null, true, true, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param collection the Solr collection to send the optimize to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return optimize(collection, waitFlush, waitSearcher, 1);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   * <p>
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher) throws SolrServerException, IOException {
    return optimize(null, waitFlush, waitSearcher);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param collection the Solr collection to send the optimize to
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   * @param maxSegments  optimizes down to at most this number of segments
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(String collection, boolean waitFlush, boolean waitSearcher, int maxSegments)
      throws SolrServerException, IOException {
    return new UpdateRequest()
        .setAction(UpdateRequest.ACTION.OPTIMIZE, waitFlush, waitSearcher, maxSegments)
        .process(this, collection);
  }

  /**
   * Performs an explicit optimize, causing a merge of all segments to one.
   *
   * Note: In most cases it is not required to do explicit optimize
   *
   * @param waitFlush  block until index changes are flushed to disk
   * @param waitSearcher  block until a new searcher is opened and registered as
   *                      the main query searcher, making the changes visible
   * @param maxSegments  optimizes down to at most this number of segments
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse optimize(boolean waitFlush, boolean waitSearcher, int maxSegments)
      throws SolrServerException, IOException {
    return optimize(null, waitFlush, waitSearcher, maxSegments);
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   *
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   * <p>
   * Also note that rollbacks reset changes made by <i>all</i> clients.  Use this method carefully when multiple clients,
   * or multithreaded clients are in use.
   *
   * @param collection the Solr collection to send the rollback to
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse rollback(String collection) throws SolrServerException, IOException {
    return new UpdateRequest().rollback().process(this, collection);
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   *
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have been committed due to autoCommit, buffer full, other client performing
   * a commit etc.
   * <p>
   * Also note that rollbacks reset changes made by <i>all</i> clients.  Use this method carefully when multiple clients,
   * or multithreaded clients are in use.
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse rollback() throws SolrServerException, IOException {
    return rollback(null);
  }

  /**
   * Deletes a single document by unique ID.  Doesn't work for child/nested docs.
   *
   * @param collection the Solr collection to delete the document from
   * @param id  the ID of the document to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(String collection, String id) throws SolrServerException, IOException {
    return deleteById(collection, id, -1);
  }

  /**
   * Deletes a single document by unique ID.  Doesn't work for child/nested docs.
   *
   * @param id  the ID of the document to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(String id) throws SolrServerException, IOException {
    return deleteById(null, id);
  }

  /**
   * Deletes a single document by unique ID, specifying max time before commit.
   * Doesn't work for child/nested docs.
   *
   * @param collection the Solr collection to delete the document from
   * @param id  the ID of the document to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 5.1
   */
  public UpdateResponse deleteById(String collection, String id, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteById(id);
    req.setCommitWithin(commitWithinMs);
    return req.process(this, collection);
  }

  /**
   * Deletes a single document by unique ID, specifying max time before commit.
   * Doesn't work for child/nested docs.
   *
   * @param id  the ID of the document to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 3.6
   */
  public UpdateResponse deleteById(String id, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(null, id, commitWithinMs);
  }

  /**
   * Deletes a list of documents by unique ID.  Doesn't work for child/nested docs.
   *
   * @param collection the Solr collection to delete the documents from
   * @param ids  the list of document IDs to delete; must be non-null and contain elements
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(String collection, List<String> ids) throws SolrServerException, IOException {
    return deleteById(collection, ids, -1);
  }

  /**
   * Deletes a list of documents by unique ID.  Doesn't work for child/nested docs.
   *
   * @param ids  the list of document IDs to delete; must be non-null and contain elements
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteById(List<String> ids) throws SolrServerException, IOException {
    return deleteById(null, ids);
  }

  /**
   * Deletes a list of documents by unique ID, specifying max time before commit.
   * Doesn't work for child/nested docs.
   *
   * @param collection the Solr collection to delete the documents from
   * @param ids  the list of document IDs to delete; must be non-null and contain elements
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 5.1
   */
  public UpdateResponse deleteById(String collection, List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    if (ids == null) throw new IllegalArgumentException("'ids' parameter must be non-null");
    if (ids.isEmpty()) throw new IllegalArgumentException("'ids' parameter must not be empty; should contain IDs to delete");

    UpdateRequest req = new UpdateRequest();
    req.deleteById(ids);
    req.setCommitWithin(commitWithinMs);
    return req.process(this, collection);
  }

  /**
   * Deletes a list of documents by unique ID, specifying max time before commit.
   * Doesn't work for child/nested docs.
   *
   * @param ids  the list of document IDs to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 3.6
   */
  public UpdateResponse deleteById(List<String> ids, int commitWithinMs) throws SolrServerException, IOException {
    return deleteById(null, ids, commitWithinMs);
  }

  /**
   * Deletes documents from the index based on a query
   *
   * @param collection the Solr collection to delete the documents from
   * @param query  the query expressing what documents to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteByQuery(String collection, String query) throws SolrServerException, IOException {
    return deleteByQuery(collection, query, -1);
  }

  /**
   * Deletes documents from the index based on a query
   *
   * @param query  the query expressing what documents to delete
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public UpdateResponse deleteByQuery(String query) throws SolrServerException, IOException {
    return deleteByQuery(null, query);
  }

  /**
   * Deletes documents from the index based on a query, specifying max time before commit
   *
   * @param collection the Solr collection to delete the documents from
   * @param query  the query expressing what documents to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 5.1
   */
  public UpdateResponse deleteByQuery(String collection, String query, int commitWithinMs) throws SolrServerException, IOException {
    UpdateRequest req = new UpdateRequest();
    req.deleteByQuery(query);
    req.setCommitWithin(commitWithinMs);
    return req.process(this, collection);
  }

  /**
   * Deletes documents from the index based on a query, specifying max time before commit
   *
   * @param query  the query expressing what documents to delete
   * @param commitWithinMs  max time (in ms) before a commit will happen
   *
   * @return an {@link org.apache.solr.client.solrj.response.UpdateResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since 3.6
   */
  public UpdateResponse deleteByQuery(String query, int commitWithinMs) throws SolrServerException, IOException {
    return deleteByQuery(null, query, commitWithinMs);
  }

  /**
   * Issues a ping request to check if the collection's replicas are alive
   *
   * @param collection collection to ping
   *
   * @return a {@link org.apache.solr.client.solrj.response.SolrPingResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrPingResponse ping(String collection) throws SolrServerException, IOException {
    return new SolrPing().process(this, collection);
  }

  /**
   * Issues a ping request to check if the server is alive
   *
   * @return a {@link org.apache.solr.client.solrj.response.SolrPingResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrPingResponse ping() throws SolrServerException, IOException {
    return new SolrPing().process(this, null);
  }


  /**
   * Performs a query to the Solr server
   *
   * @param collection the Solr collection to query
   * @param params  an object holding all key/value parameters to send along the request
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(String collection, SolrParams params) throws SolrServerException, IOException {
    return new QueryRequest(params).process(this, collection);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param params  an object holding all key/value parameters to send along the request
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(SolrParams params) throws SolrServerException, IOException {
    return query(null, params);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param collection the Solr collection to query
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(String collection, SolrParams params, METHOD method) throws SolrServerException, IOException {
    return new QueryRequest(params, method).process(this, collection);
  }

  /**
   * Performs a query to the Solr server
   *
   * @param params  an object holding all key/value parameters to send along the request
   * @param method  specifies the HTTP method to use for the request, such as GET or POST
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public QueryResponse query(SolrParams params, METHOD method) throws SolrServerException, IOException {
    return query(null, params, method);
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will 
   * send events for each Document rather then add them to the QueryResponse.
   *
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return 
   * the results in the QueryResponse.
   *
   * @param collection the Solr collection to query
   * @param params  an object holding all key/value parameters to send along the request
   * @param callback the callback to stream results to
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 5.1
   */
  public QueryResponse queryAndStreamResponse(String collection, SolrParams params, StreamingResponseCallback callback)
      throws SolrServerException, IOException {
    return getQueryResponse(collection, params,  new StreamingBinaryResponseParser(callback));
  }

  public QueryResponse queryAndStreamResponse(String collection, SolrParams params, FastStreamingDocsCallback callback)
      throws SolrServerException, IOException {
    return getQueryResponse(collection, params, new StreamingBinaryResponseParser(callback));
  }

  private QueryResponse getQueryResponse(String collection, SolrParams params, ResponseParser parser) throws SolrServerException, IOException {
    QueryRequest req = new QueryRequest(params);
    if (parser instanceof StreamingBinaryResponseParser) {
      req.setStreamingResponseCallback(((StreamingBinaryResponseParser) parser).callback);
    }
    req.setResponseParser(parser);
    return req.process(this, collection);
  }

  /**
   * Query solr, and stream the results.  Unlike the standard query, this will
   * send events for each Document rather then add them to the QueryResponse.
   *
   * Although this function returns a 'QueryResponse' it should be used with care
   * since it excludes anything that was passed to callback.  Also note that
   * future version may pass even more info to the callback and may not return
   * the results in the QueryResponse.
   *
   * @param params  an object holding all key/value parameters to send along the request
   * @param callback the callback to stream results to
   *
   * @return a {@link org.apache.solr.client.solrj.response.QueryResponse} containing the response
   *         from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   *
   * @since solr 4.0
   */
  public QueryResponse queryAndStreamResponse(SolrParams params, StreamingResponseCallback callback)
      throws SolrServerException, IOException {
    return queryAndStreamResponse(null, params, callback);
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier.
   *
   * @param collection the Solr collection to query
   * @param id the id
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String collection, String id) throws SolrServerException, IOException {
    return getById(collection, id, null);
  }
  /**
   * Retrieves the SolrDocument associated with the given identifier.
   *
   * @param id the id
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String id) throws SolrServerException, IOException {
    return getById(null, id, null);
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier and uses
   * the SolrParams to execute the request.
   *
   * @param collection the Solr collection to query
   * @param id the id
   * @param params additional parameters to add to the query
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String collection, String id, SolrParams params) throws SolrServerException, IOException {
    SolrDocumentList docs = getById(collection, Collections.singletonList(id), params);
    if (!docs.isEmpty()) {
      return docs.get(0);
    }
    return null;
  }

  /**
   * Retrieves the SolrDocument associated with the given identifier and uses
   * the SolrParams to execute the request.
   *
   * @param id the id
   * @param params additional parameters to add to the query
   *
   * @return retrieved SolrDocument, or null if no document is found.
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocument getById(String id, SolrParams params) throws SolrServerException, IOException {
    return getById(null, id, params);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param collection the Solr collection to query
   * @param ids the ids
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(String collection, Collection<String> ids) throws SolrServerException, IOException {
    return getById(collection, ids, null);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param ids the ids
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   *  @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(Collection<String> ids) throws SolrServerException, IOException {
    return getById(null, ids);
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers and uses
   * the SolrParams to execute the request.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param collection the Solr collection to query
   * @param ids the ids
   * @param params additional parameters to add to the query
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(String collection, Collection<String> ids, SolrParams params)
      throws SolrServerException, IOException {
    if (ids == null || ids.isEmpty()) {
      throw new IllegalArgumentException("Must provide an identifier of a document to retrieve.");
    }

    ModifiableSolrParams reqParams = new ModifiableSolrParams(params);
    if (StringUtils.isEmpty(reqParams.get(CommonParams.QT))) {
      reqParams.set(CommonParams.QT, "/get");
    }
    reqParams.set("ids", ids.toArray(new String[ids.size()]));

    return query(collection, reqParams).getResults();
  }

  /**
   * Retrieves the SolrDocuments associated with the given identifiers and uses
   * the SolrParams to execute the request.
   *
   * If a document was not found, it will not be added to the SolrDocumentList.
   *
   * @param ids the ids
   * @param params additional parameters to add to the query
   *
   * @return a SolrDocumentList, or null if no documents were found
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public SolrDocumentList getById(Collection<String> ids, SolrParams params) throws SolrServerException, IOException {
    return getById(null, ids, params);
  }

  /**
   * Execute a request against a Solr server for a given collection
   *
   * @param request the request to execute
   * @param collection the collection to execute the request against
   *
   * @return a {@link NamedList} containing the response from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public abstract NamedList<Object> request(@SuppressWarnings({"rawtypes"})final SolrRequest request, String collection)
      throws SolrServerException, IOException;

  /**
   * Execute a request against a Solr server
   *
   * @param request the request to execute
   *
   * @return a {@link NamedList} containing the response from the server
   *
   * @throws IOException If there is a low-level I/O error.
   * @throws SolrServerException if there is an error on the server
   */
  public final NamedList<Object> request(@SuppressWarnings({"rawtypes"})final SolrRequest request) throws SolrServerException, IOException {
    return request(request, null);
  }

  /**
   * Get the {@link org.apache.solr.client.solrj.beans.DocumentObjectBinder} for this client.
   *
   * @return a DocumentObjectBinder
   *
   * @see SolrClient#addBean
   * @see SolrClient#addBeans
   */
  public DocumentObjectBinder getBinder() {
    if(binder == null){
      binder = new DocumentObjectBinder();
    }
    return binder;
  }

}
