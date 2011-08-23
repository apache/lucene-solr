package org.apache.solr.handler.clustering.carrot2;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.clustering.SearchClusteringEngine;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;
import org.carrot2.core.Cluster;
import org.carrot2.core.Controller;
import org.carrot2.core.ControllerFactory;
import org.carrot2.core.Document;
import org.carrot2.core.IClusteringAlgorithm;
import org.carrot2.core.attribute.AttributeNames;
import org.carrot2.text.linguistic.DefaultLexicalDataFactoryDescriptor;
import org.carrot2.text.preprocessing.pipeline.BasicPreprocessingPipelineDescriptor;
import org.carrot2.util.resource.ClassLoaderLocator;
import org.carrot2.util.resource.IResource;
import org.carrot2.util.resource.IResourceLocator;
import org.carrot2.util.resource.ResourceLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Search results clustering engine based on Carrot2 clustering algorithms.
 * <p/>
 * Output from this class is subject to change.
 *
 * @link http://project.carrot2.org
 */
public class CarrotClusteringEngine extends SearchClusteringEngine {
	private transient static Logger log = LoggerFactory
          .getLogger(CarrotClusteringEngine.class);

	/**
	 * The subdirectory in Solr config dir to read customized Carrot2 resources from.
	 */
	private static final String CARROT_RESOURCES_PREFIX = "clustering/carrot2";

  /**
   * Name of Carrot2 document's field containing Solr document's identifier.
   */
  private static final String SOLR_DOCUMENT_ID = "solrId";

  /**
   * Name of Solr document's field containing the document's identifier. To avoid
   * repeating the content of documents in clusters on output, each cluster contains
   * identifiers of documents it contains.
   */
  private String idFieldName;

  /**
   * Carrot2 controller that manages instances of clustering algorithms
   */
  private Controller controller = ControllerFactory.createPooling();
  private Class<? extends IClusteringAlgorithm> clusteringAlgorithmClass;

  @Override
  @Deprecated
  public Object cluster(Query query, DocList docList, SolrQueryRequest sreq) {
    SolrIndexSearcher searcher = sreq.getSearcher();
    SolrDocumentList solrDocList;
    try {
      Map<SolrDocument,Integer> docIds = new HashMap<SolrDocument, Integer>(docList.size());
      solrDocList = SolrPluginUtils.docListToSolrDocumentList( docList, searcher, getFieldsToLoad(sreq), docIds );
      return cluster(query, solrDocList, docIds, sreq);
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public Object cluster(Query query, SolrDocumentList solrDocList,
      Map<SolrDocument, Integer> docIds, SolrQueryRequest sreq) {
    try {
      // Prepare attributes for Carrot2 clustering call
      Map<String, Object> attributes = new HashMap<String, Object>();
      List<Document> documents = getDocuments(solrDocList, docIds, query, sreq);
      attributes.put(AttributeNames.DOCUMENTS, documents);
      attributes.put(AttributeNames.QUERY, query.toString());

      // Pass the fields on which clustering runs to the
      // SolrStopwordsCarrot2LexicalDataFactory
      attributes.put("solrFieldNames", getFieldsForClustering(sreq));

      // Pass extra overriding attributes from the request, if any
      extractCarrotAttributes(sreq.getParams(), attributes);

      // Perform clustering and convert to named list
      return clustersToNamedList(controller.process(attributes,
              clusteringAlgorithmClass).getClusters(), sreq.getParams());
    } catch (Exception e) {
      log.error("Carrot2 clustering failed", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Carrot2 clustering failed", e);
    }
  }

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
  public String init(NamedList config, final SolrCore core) {
    String result = super.init(config, core);
    final SolrParams initParams = SolrParams.toSolrParams(config);

    // Initialize Carrot2 controller. Pass initialization attributes, if any.
    HashMap<String, Object> initAttributes = new HashMap<String, Object>();
    extractCarrotAttributes(initParams, initAttributes);

    // Customize the stemmer and tokenizer factories. The implementations we provide here
    // are included in the code base of Solr, so that it's possible to refactor
    // the Lucene APIs the factories rely on if needed.
    // Additionally, we set a custom lexical resource factory for Carrot2 that
    // will use both Carrot2 default stop words as well as stop words from
    // the StopFilter defined on the field.
		BasicPreprocessingPipelineDescriptor.attributeBuilder(initAttributes)
				.stemmerFactory(LuceneCarrot2StemmerFactory.class)
				.tokenizerFactory(LuceneCarrot2TokenizerFactory.class)
				.lexicalDataFactory(SolrStopwordsCarrot2LexicalDataFactory.class);

		// Pass the schema to SolrStopwordsCarrot2LexicalDataFactory.
		initAttributes.put("solrIndexSchema", core.getSchema());

    // Customize Carrot2's resource lookup to first look for resources
    // using Solr's resource loader. If that fails, try loading from the classpath.
    DefaultLexicalDataFactoryDescriptor.attributeBuilder(initAttributes)
        .resourceLookup(new ResourceLookup(new IResourceLocator() {
          @Override
          public IResource[] getAll(final String resource) {
            final SolrResourceLoader resourceLoader = core.getResourceLoader();
            final String carrot2ResourcesDir = initParams.get(
                CarrotParams.LEXICAL_RESOURCES_DIR, CARROT_RESOURCES_PREFIX);
            try {
              log.debug("Looking for " + resource + " in "
                  + carrot2ResourcesDir);
              final InputStream resourceStream = resourceLoader
                  .openResource(carrot2ResourcesDir + "/" + resource);

              log.info(resource + " loaded from " + carrot2ResourcesDir);
              final IResource foundResource = new IResource() {
                @Override
                public InputStream open() throws IOException {
                  return resourceStream;
                }
              };
              return new IResource[] { foundResource };
            } catch (RuntimeException e) {
              // No way to distinguish if the resource was found but failed
              // to load or wasn't found at all, so we simply fall back
              // to Carrot2 defaults here by returning an empty locations array.
              log.debug(resource + " not found in " + carrot2ResourcesDir
                  + ". Using the default " + resource + " from Carrot JAR.");
              return new IResource[] {};
            }
          }
        },

        // Using the class loader directly because this time we want to omit the prefix
        new ClassLoaderLocator(core.getResourceLoader().getClassLoader())));

    this.controller.init(initAttributes);
    this.idFieldName = core.getSchema().getUniqueKeyField().getName();

    // Make sure the requested Carrot2 clustering algorithm class is available
    String carrotAlgorithmClassName = initParams.get(CarrotParams.ALGORITHM);
    Class<?> algorithmClass = core.getResourceLoader().findClass(carrotAlgorithmClassName);
    if (!IClusteringAlgorithm.class.isAssignableFrom(algorithmClass)) {
      throw new IllegalArgumentException("Class provided as "
              + CarrotParams.ALGORITHM + " must implement "
              + IClusteringAlgorithm.class.getName());
    }
    this.clusteringAlgorithmClass = (Class<? extends IClusteringAlgorithm>) algorithmClass;

    return result;
  }

  @Override
  protected Set<String> getFieldsToLoad(SolrQueryRequest sreq){
    SolrParams solrParams = sreq.getParams();

    HashSet<String> fields = Sets.newHashSet(getFieldsForClustering(sreq));
    fields.add(idFieldName);
    fields.add(solrParams.get(CarrotParams.URL_FIELD_NAME, "url"));
		return fields;
  }

	/**
	 * Returns the names of fields that will be delivering the actual
	 * content for clustering. Currently, there are two such fields: document
	 * title and document content.
	 */
	private Set<String> getFieldsForClustering(SolrQueryRequest sreq) {
    SolrParams solrParams = sreq.getParams();

    String titleField = solrParams.get(CarrotParams.TITLE_FIELD_NAME, "title");
    String snippetField = solrParams.get(CarrotParams.SNIPPET_FIELD_NAME, titleField);
    if (StringUtils.isBlank(snippetField)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, CarrotParams.SNIPPET_FIELD_NAME
              + " must not be blank.");
    }
    return Sets.newHashSet(titleField, snippetField);
	}

  /**
   * Prepares Carrot2 documents for clustering.
   */
  private List<Document> getDocuments(SolrDocumentList solrDocList, Map<SolrDocument, Integer> docIds,
                                      Query query, final SolrQueryRequest sreq) throws IOException {
    SolrHighlighter highlighter = null;
    SolrParams solrParams = sreq.getParams();
    SolrCore core = sreq.getCore();

    String urlField = solrParams.get(CarrotParams.URL_FIELD_NAME, "url");
    String titleField = solrParams.get(CarrotParams.TITLE_FIELD_NAME, "title");
    String snippetField = solrParams.get(CarrotParams.SNIPPET_FIELD_NAME, titleField);
    
    // Get the documents
    boolean produceSummary = solrParams.getBool(CarrotParams.PRODUCE_SUMMARY, false);

    SolrQueryRequest req = null;
    String[] snippetFieldAry = null;
    if (produceSummary) {
      highlighter = HighlightComponent.getHighlighter(core);
      if (highlighter != null){
        Map<String, Object> args = Maps.newHashMap();
        snippetFieldAry = new String[]{snippetField};
        args.put(HighlightParams.FIELDS, snippetFieldAry);
        args.put(HighlightParams.HIGHLIGHT, "true");
        args.put(HighlightParams.SIMPLE_PRE, ""); //we don't care about actually highlighting the area
        args.put(HighlightParams.SIMPLE_POST, "");
        args.put(HighlightParams.FRAGSIZE, solrParams.getInt(CarrotParams.SUMMARY_FRAGSIZE, solrParams.getInt(HighlightParams.FRAGSIZE, 100)));
        req = new LocalSolrQueryRequest(core, query.toString(), "", 0, 1, args) {
          @Override
          public SolrIndexSearcher getSearcher() {
            return sreq.getSearcher();
          }
        };
      } else {
        log.warn("No highlighter configured, cannot produce summary");
        produceSummary = false;
      }
    }

    Iterator<SolrDocument> docsIter = solrDocList.iterator();
    List<Document> result = new ArrayList<Document>(solrDocList.size());

    float[] scores = {1.0f};
    int[] docsHolder = new int[1];
    Query theQuery = query;

    while (docsIter.hasNext()) {
      SolrDocument sdoc = docsIter.next();
      String snippet = getValue(sdoc, snippetField);
      // TODO: docIds will be null when running distributed search.
      // See comment in ClusteringComponent#finishStage().
      if (produceSummary && docIds != null) {
        docsHolder[0] = docIds.get(sdoc).intValue();
        DocList docAsList = new DocSlice(0, 1, docsHolder, scores, 1, 1.0f);
        NamedList<Object> highlights = highlighter.doHighlighting(docAsList, theQuery, req, snippetFieldAry);
        if (highlights != null && highlights.size() == 1) {//should only be one value given our setup
          //should only be one document with one field
          @SuppressWarnings("unchecked")
					NamedList<String []> tmp = (NamedList<String[]>) highlights.getVal(0);
          String [] highlt = tmp.get(snippetField);
          if (highlt != null && highlt.length == 1) {
            snippet = highlt[0];
          }
        }
      }
      Document carrotDocument = new Document(getValue(sdoc, titleField),
              snippet, (String)sdoc.getFieldValue(urlField));
      carrotDocument.setField(SOLR_DOCUMENT_ID, sdoc.getFieldValue(idFieldName));
      result.add(carrotDocument);
    }

    return result;
  }

  protected String getValue(SolrDocument sdoc, String field) {
    StringBuilder result = new StringBuilder();
    Collection<Object> vals = sdoc.getFieldValues(field);
    if(vals == null) return "";
    Iterator<Object> ite = vals.iterator();
    while(ite.hasNext()){
      // Join multiple values with a period so that Carrot2 does not pick up
      // phrases that cross field value boundaries (in most cases it would
      // create useless phrases).
      result.append((String)ite.next()).append(" . ");
    }
    return result.toString().trim();
  }

  private List<NamedList<Object>> clustersToNamedList(List<Cluster> carrotClusters,
                                   SolrParams solrParams) {
    List<NamedList<Object>> result = Lists.newArrayList();
    clustersToNamedList(carrotClusters, result, solrParams.getBool(
            CarrotParams.OUTPUT_SUB_CLUSTERS, true), solrParams.getInt(
            CarrotParams.NUM_DESCRIPTIONS, Integer.MAX_VALUE));
    return result;
  }

  private void clustersToNamedList(List<Cluster> outputClusters,
                                   List<NamedList<Object>> parent, boolean outputSubClusters, int maxLabels) {
    for (Cluster outCluster : outputClusters) {
      NamedList<Object> cluster = new SimpleOrderedMap<Object>();
      parent.add(cluster);

      // Add labels
      List<String> labels = outCluster.getPhrases();
      if (labels.size() > maxLabels) {
        labels = labels.subList(0, maxLabels);
      }
      cluster.add("labels", labels);

      // Add cluster score
      final Double score = outCluster.getScore();
      if (score != null) {
        cluster.add("score", score);
      }

      // Add other topics marker
      if (outCluster.isOtherTopics()) {
        cluster.add("other-topics", outCluster.isOtherTopics());
      }

      // Add documents
      List<Document> docs = outputSubClusters ? outCluster.getDocuments() : outCluster.getAllDocuments();
      List<Object> docList = Lists.newArrayList();
      cluster.add("docs", docList);
      for (Document doc : docs) {
        docList.add(doc.getField(SOLR_DOCUMENT_ID));
      }

      // Add subclusters
      if (outputSubClusters && !outCluster.getSubclusters().isEmpty()) {
        List<NamedList<Object>> subclusters = Lists.newArrayList();
        cluster.add("clusters", subclusters);
        clustersToNamedList(outCluster.getSubclusters(), subclusters,
                outputSubClusters, maxLabels);
      }
    }
  }

  /**
   * Extracts parameters that can possibly match some attributes of Carrot2 algorithms.
   */
  private void extractCarrotAttributes(SolrParams solrParams,
                                       Map<String, Object> attributes) {
    // Extract all non-predefined parameters. This way, we'll be able to set all
    // parameters of Carrot2 algorithms without defining their names as constants.
    for (Iterator<String> paramNames = solrParams.getParameterNamesIterator(); paramNames
            .hasNext();) {
      String paramName = paramNames.next();
      if (!CarrotParams.CARROT_PARAM_NAMES.contains(paramName)) {
        attributes.put(paramName, solrParams.get(paramName));
      }
    }
  }
}
