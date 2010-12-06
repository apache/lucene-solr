/**
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

package org.apache.solr.response;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocIterator;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

/**
 * 
 * 
 * This class serves as a basis from which {@link QueryResponseWriter}s can be
 * developed. The class provides a single method
 * {@link #write(SingleResponseWriter, SolrQueryRequest, SolrQueryResponse)}
 * that allows users to implement a {@link SingleResponseWriter} sub-class which
 * defines how to output {@link SolrInputDocument}s or a
 * {@link SolrDocumentList}.
 * 
 * @version $Id$
 * @since 1.5
 * 
 */
public abstract class BaseResponseWriter {

  private static final Logger LOG = LoggerFactory
      .getLogger(BaseResponseWriter.class);

  private static final String SCORE_FIELD = "score";

  /**
   * 
   * The main method that allows users to write {@link SingleResponseWriter}s
   * and provide them as the initial parameter <code>responseWriter</code> to
   * this method which defines how output should be generated.
   * 
   * @param responseWriter
   *          The user-provided {@link SingleResponseWriter} implementation.
   * @param request
   *          The provided {@link SolrQueryRequest}.
   * @param response
   *          The provided {@link SolrQueryResponse}.
   * @throws IOException
   *           If any error occurs.
   */
  public void write(SingleResponseWriter responseWriter,
      SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    responseWriter.start();
    NamedList nl = response.getValues();
    for (int i = 0; i < nl.size(); i++) {
      String name = nl.getName(i);
      Object val = nl.getVal(i);
      if ("responseHeader".equals(name)) {
        Boolean omitHeader = request.getParams().getBool(CommonParams.OMIT_HEADER);
        if (omitHeader == null || !omitHeader) responseWriter.writeResponseHeader((NamedList) val);
      } else if (val instanceof SolrDocumentList) {
        SolrDocumentList list = (SolrDocumentList) val;
        DocListInfo info = new DocListInfo((int)list.getNumFound(), list.size(), (int)list.getStart(), list.getMaxScore());
        if (responseWriter.isStreamingDocs()) {
          responseWriter.startDocumentList(name,info);
          for (SolrDocument solrDocument : list)
            responseWriter.writeDoc(solrDocument);
          responseWriter.endDocumentList();
        } else {
          responseWriter.writeAllDocs(info, list);
        }
      } else if (val instanceof DocList) {
        DocList docList = (DocList) val;
        int sz = docList.size();
        IdxInfo idxInfo = new IdxInfo(request.getSchema(), request
            .getSearcher(), response.getReturnFields());
        DocListInfo info = new DocListInfo(docList.matches(), docList.size(),docList.offset(),
            docList.maxScore());
        DocIterator iterator = docList.iterator();
        if (responseWriter.isStreamingDocs()) {
          responseWriter.startDocumentList(name,info);
          for (int j = 0; j < sz; j++) {
            SolrDocument sdoc = getDoc(iterator.nextDoc(), idxInfo);
            if (idxInfo.includeScore && docList.hasScores()) {
              sdoc.addField(SCORE_FIELD, iterator.score());
            }
            responseWriter.writeDoc(sdoc);
          }
        } else {
          ArrayList<SolrDocument> list = new ArrayList<SolrDocument>(docList
              .size());
          for (int j = 0; j < sz; j++) {
            SolrDocument sdoc = getDoc(iterator.nextDoc(), idxInfo);
            if (idxInfo.includeScore && docList.hasScores()) {
              sdoc.addField(SCORE_FIELD, iterator.score());
            }
            list.add(sdoc);
          }
          responseWriter.writeAllDocs(info, list);
        }

      } else {
        responseWriter.writeOther(name, val);

      }
    }
    responseWriter.end();

  }

  /**No ops implementation so that the implementing classes do not have to do it
   */
  public void init(NamedList args){}

  private static class IdxInfo {
    IndexSchema schema;
    SolrIndexSearcher searcher;
    Set<String> returnFields;
    boolean includeScore;

    private IdxInfo(IndexSchema schema, SolrIndexSearcher searcher,
        Set<String> returnFields) {
      this.schema = schema;
      this.searcher = searcher;
      this.includeScore = returnFields != null
              && returnFields.contains(SCORE_FIELD);
      if (returnFields != null) {
        if (returnFields.size() == 0 || (returnFields.size() == 1 && includeScore) || returnFields.contains("*")) {
          returnFields = null;  // null means return all stored fields
        }
      }
      this.returnFields = returnFields;

    }
  }

  private static SolrDocument getDoc(int id, IdxInfo info) throws IOException {
    Document doc = info.searcher.doc(id);
    SolrDocument solrDoc = new SolrDocument();
    for (Fieldable f : doc.getFields()) {
      String fieldName = f.name();
      if (info.returnFields != null && !info.returnFields.contains(fieldName))
        continue;
      SchemaField sf = info.schema.getFieldOrNull(fieldName);
      FieldType ft = null;
      if (sf != null) ft = sf.getType();
      Object val = null;
      if (ft == null) { // handle fields not in the schema
        if (f.isBinary())
          val = f.getBinaryValue();
        else
          val = f.stringValue();
      } else {
        try {
          if (BinaryResponseWriter.KNOWN_TYPES.contains(ft.getClass())) {
            val = ft.toObject(f);
          } else {
            val = ft.toExternal(f);
          }
        } catch (Exception e) {
          // There is a chance of the underlying field not really matching the
          // actual field type . So ,it can throw exception
          LOG.warn("Error reading a field from document : " + solrDoc, e);
          // if it happens log it and continue
          continue;
        }
      }
      if (sf != null && sf.multiValued() && !solrDoc.containsKey(fieldName)) {
        ArrayList l = new ArrayList();
        l.add(val);
        solrDoc.addField(fieldName, l);
      } else {
        solrDoc.addField(fieldName, val);
      }
    }

    return solrDoc;
  }

  public static class DocListInfo {
    public final int numFound;
    public final int start ;
    public Float maxScore = null;
    public final int size;

    public DocListInfo(int numFound, int sz,int start, Float maxScore) {
      this.numFound = numFound;
      size = sz;
      this.start = start;
      this.maxScore = maxScore;
    }
  }

  /**
   * 
   * Users wanting to define custom {@link QueryResponseWriter}s that deal with
   * {@link SolrInputDocument}s and {@link SolrDocumentList} should override the
   * methods for this class. All the methods are w/o body because the user is left
   * to choose which all methods are required for his purpose
   */
  public static abstract class SingleResponseWriter {

    /**
     * This method is called at the start of the {@link QueryResponseWriter}
     * output. Override this method if you want to provide a header for your
     * output, e.g., XML headers, etc.
     * 
     * @throws IOException
     *           if any error occurs.
     */
    public void start() throws IOException { }

    /**
     * This method is called at the start of processing a
     * {@link SolrDocumentList}. Those that override this method are provided
     * with {@link DocListInfo} object to use to inspect the output
     * {@link SolrDocumentList}.
     * 
     * @param info Information about the {@link SolrDocumentList} to output.
     */
    public void startDocumentList(String name, DocListInfo info) throws IOException { }

    /**
     * This method writes out a {@link SolrDocument}, on a doc-by-doc basis.
     * This method is only called when {@link #isStreamingDocs()} returns true.
     * 
     * @param solrDocument
     *          The doc-by-doc {@link SolrDocument} to transform into output as
     *          part of this {@link QueryResponseWriter}.
     */
    public void writeDoc(SolrDocument solrDocument) throws IOException { }

    /**
     * This method is called at the end of outputting a {@link SolrDocumentList}
     * or on a doc-by-doc {@link SolrDocument} basis.
     */
    public void endDocumentList() throws IOException { } 
    /**
     * This method defines how to output the {@link SolrQueryResponse} header
     * which is provided as a {@link NamedList} parameter.
     * 
     * @param responseHeader
     *          The response header to output.
     */
    public void writeResponseHeader(NamedList responseHeader) throws IOException { }

    /**
     * This method is called at the end of the {@link QueryResponseWriter}
     * lifecycle. Implement this method to add a footer to your output, e.g., in
     * the case of XML, the outer tag for your tag set, etc.
     * 
     * @throws IOException
     *           If any error occurs.
     */
    public void end() throws IOException { }

    /**
     * Define this method to control how output is written by this
     * {@link QueryResponseWriter} if the output is not a
     * {@link SolrInputDocument} or a {@link SolrDocumentList}.
     * 
     * @param name
     *          The name of the object to output.
     * @param other
     *          The object to output.
     * @throws IOException
     *           If any error occurs.
     */
    public void writeOther(String name, Object other) throws IOException { }

    /**
     * Overriding this method to return false forces all
     * {@link SolrInputDocument}s to be spit out as a {@link SolrDocumentList}
     * so they can be processed as a whole, rather than on a doc-by-doc basis.
     * If set to false, this method calls
     * {@link #writeAllDocs(DocListInfo, List)}, else if set to true, then this
     * method forces calling {@link #writeDoc(SolrDocument)} on a doc-by-doc
     * basis. one
     * 
     * @return True to force {@link #writeDoc(SolrDocument)} to be called, False
     *         to force {@link #writeAllDocs(DocListInfo, List)} to be called.
     */
    public boolean isStreamingDocs() { return true; }

    /**
     * Writes out all {@link SolrInputDocument}s . This is invoked only if
     * {@link #isStreamingDocs()} returns false.
     * 
     * @param info
     *          Information about the {@link List} of {@link SolrDocument}s to
     *          output.
     * @param allDocs
     *          A {@link List} of {@link SolrDocument}s to output.
     * @throws IOException
     *           If any error occurs.
     */
    public void writeAllDocs(DocListInfo info, List<SolrDocument> allDocs) throws IOException { }

  }

}
