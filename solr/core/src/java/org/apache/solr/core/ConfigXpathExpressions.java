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
package org.apache.solr.core;

import net.sf.saxon.Configuration;
import net.sf.saxon.xpath.XPathFactoryImpl;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

public class ConfigXpathExpressions implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final XPathFactoryImpl xpathFactory;
  static String luceneMatchVersionPath = "/config/" + IndexSchema.LUCENE_MATCH_VERSION_PARAM;
  static String indexDefaultsPath = "/config/indexDefaults";
  static String mainIndexPath = "/config/mainIndex";
  static String nrtModePath = "/config/indexConfig/nrtmode";
  static String unlockOnStartupPath = "/config/indexConfig/unlockOnStartup";

  static String shardHandlerFactoryPath = "/solr/shardHandlerFactory";
  static String counterExpPath = "/solr/metrics/suppliers/counter";
  static String meterPath = "/solr/metrics/suppliers/meter";
  static String timerPath = "/solr/metrics/suppliers/timer";
  static String histoPath = "/solr/metrics/suppliers/histogram";
  static String historyPath = "/solr/metrics/history";
  static String  transientCoreCacheFactoryPath =  "/solr/transientCoreCacheFactory";
  static String  tracerConfigPath = "/solr/tracerConfig";

  static String  coreLoadThreadsPath = "/solr/@coreLoadThreads";
  static String  persistentPath = "/solr/@persistent";
  static String  sharedLibPath = "/solr/@sharedLib";
  static String  zkHostPath = "/solr/@zkHost";
  static String  coresPath = "/solr/cores";

  static String  metricsReporterPath = "/solr/metrics/reporter";


  public static String schemaNamePath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.AT + IndexSchema.NAME);
  public static String schemaVersionPath = "/schema/@version";

  public static String copyFieldPath = "//" + IndexSchema.COPY_FIELD;

  public static String fieldTypeXPathExpressions = IndexSchema.getFieldTypeXPathExpressions();

  public static String schemaSimPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.SIMILARITY); //   /schema/similarity

  public static String defaultSearchFieldPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, "defaultSearchField", IndexSchema.TEXT_FUNCTION);

  public static String solrQueryParserDefaultOpPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, "solrQueryParser", IndexSchema.AT + "defaultOperator");

  public static String schemaUniqueKeyPath = IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.UNIQUE_KEY, IndexSchema.TEXT_FUNCTION);

  public static String filterCachePath = "/config/query/filterCache";
  public static String queryResultCachePath = "/config/query/queryResultCache";
  public static String documentCachePath = "/config/query/documentCache";
  public static String fieldValueCachePath = "/config/query/fieldValueCache";

  public static String useFilterForSortedQueryPath = "/config/query/useFilterForSortedQuery";
  public static String queryResultWindowSizePath = "/config/query/queryResultWindowSize";
  public static String enableLazyFieldLoadingPath = "/config/query/enableLazyFieldLoading";
  public static String queryResultMaxDocsCachedPath = "/config/query/queryResultMaxDocsCached";
  public static String useRangeVersionsPath = "/config/peerSync/useRangeVersions";
  public static String maxBooleanClausesPath = "/config/query/maxBooleanClauses";
  public static String useColdSearcherPath = "/config/query/useColdSearcher";

  public static String  multipartUploadLimitInKBPath = "/config/requestDispatcher/requestParsers/@multipartUploadLimitInKB";
  public static String  formdataUploadLimitInKBPath = "/config/requestDispatcher/requestParsers/@formdataUploadLimitInKB";
  public static String  enableRemoteStreamingPath = "/config/requestDispatcher/requestParsers/@enableRemoteStreaming";
  public static String  enableStreamBodyPath = "/config/requestDispatcher/requestParsers/@enableStreamBody";

  public static String  handleSelectPath = "/config/requestDispatcher/@handleSelect";
  public static String  addHttpRequestToContextPath = "/config/requestDispatcher/requestParsers/@addHttpRequestToContext";

  public static String  maxWarmingSearchersPath = "/config/query/maxWarmingSearchers";
  public static String  slowQueryThresholdMillisPath = "/config/query/slowQueryThresholdMillis";

  public static String  never304Path = "/config/requestDispatcher/httpCaching/@never304";

  public static String  updateHandlerClassPath = "/config/updateHandler/@class";
  public static String  autoCommitMaxDocsPath = "/config/updateHandler/autoCommit/maxDocs";
  public static String  autoCommitMaxSizePath = "/config/updateHandler/autoCommit/maxSize";
  public static String  autoCommitMaxTimePath = "/config/updateHandler/autoCommit/maxTime";
  public static String  indexWriterCloseWaitsForMergesPath = "/config/updateHandler/indexWriter/closeWaitsForMerge";
  public static String  autoCommitOpenSearcherPath = "/config/updateHandler/autoCommit/openSearcher";
  public static String  autoSoftCommitMaxTimePath = "/config/updateHandler/autoSoftCommit/maxTime";
  public static String  autoSoftCommitMaxDocsPath = "/config/updateHandler/autoSoftCommit/maxDocs";
  public static String  commitWithinSoftCommitPath = "/config/updateHandler/commitWithin/softCommit";

  public static String  useCompoundFilePath = "/config/indexConfig/useCompoundFile";
  public static String  maxBufferedDocsPath = "/config/indexConfig/maxBufferedDocs";
  public static String  ramPerThreadHardLimitMBPath = "/config/indexConfig/ramPerThreadHardLimitMB";
  public static String  writeLockTimeoutPath = "/config/indexConfig/writeLockTimeout";

  public static String  maxMergeDocPath = "/config/indexConfig/maxMergeDocs";
  public static String  mergeFactorPath = "/config/indexConfig/mergeFactor";
  public static String  infoStreamPath = "/config/indexConfig/infoStream";

  public static String indexConfigPath = "/config/indexConfig";
  public static String mergeSchedulerPath = indexConfigPath + "/mergeScheduler";
  public static String mergePolicyPath = indexConfigPath + "/mergePolicy";
  public static String ramBufferSizeMBPath = indexConfigPath + "/ramBufferSizeMB";
  public static String checkIntegrityAtMergePath = indexConfigPath + "/checkIntegrityAtMerge";

  public static String  versionBucketLockTimeoutMsPath = "/config/updateHandler/versionBucketLockTimeoutMs";

  public XPathExpression shardHandlerFactoryExp;
  public XPathExpression counterExp;
  public XPathExpression meterExp;
  public XPathExpression timerExp;
  public XPathExpression histoExp;
  public XPathExpression historyExp;
  public XPathExpression transientCoreCacheFactoryExp;
  public XPathExpression tracerConfigExp;

  public XPathExpression coreLoadThreadsExp;
  public XPathExpression persistentExp;
  public XPathExpression sharedLibExp;
  public XPathExpression zkHostExp;
  public XPathExpression coresExp;

  public XPathExpression xpathOrExp;
  public XPathExpression schemaNameExp;
  public XPathExpression schemaVersionExp;
  public XPathExpression schemaSimExp;
  public XPathExpression defaultSearchFieldExp;
  public XPathExpression solrQueryParserDefaultOpExp;
  public XPathExpression schemaUniqueKeyExp;
  public XPathExpression fieldTypeXPathExpressionsExp;
  public XPathExpression copyFieldsExp;

  public XPathExpression luceneMatchVersionExp;
  public XPathExpression indexDefaultsExp;
  public XPathExpression mainIndexExp;
  public XPathExpression nrtModeExp;
  public XPathExpression unlockOnStartupExp;

  public XPathExpression metricsReporterExp;

  public XPathExpression analyzerQueryExp;
  public XPathExpression analyzerMultiTermExp;

  public XPathExpression analyzerIndexExp;
  public XPathExpression similarityExp;
  public XPathExpression charFilterExp;
  public XPathExpression tokenizerExp;
  public XPathExpression filterExp;

  public XPathExpression filterCacheExp;
  public XPathExpression documentCacheExp;
  public XPathExpression queryResultCacheExp;
  public XPathExpression fieldValueCacheExp;


  public XPathExpression useFilterForSortedQueryExp;
  public XPathExpression queryResultWindowSizeeExp;
  public XPathExpression enableLazyFieldLoadingExp;
  public XPathExpression queryResultMaxDocsCachedExp;
  public XPathExpression useRangeVersionsExp;
  public XPathExpression maxBooleanClausesExp;
  public XPathExpression useColdSearcherExp;

  public XPathExpression multipartUploadLimitInKBExp;
  public XPathExpression formdataUploadLimitInKBExp;
  public XPathExpression enableRemoteStreamingExp;
  public XPathExpression enableStreamBodyExp;

  public XPathExpression handleSelectExp;
  public XPathExpression addHttpRequestToContextExp;

  public XPathExpression maxWarmingSearchersExp;
  public XPathExpression never304Exp;
  public XPathExpression slowQueryThresholdMillisExp;

  public XPathExpression  updateHandlerClassExp;
  public XPathExpression autoCommitMaxDocsExp;
  public XPathExpression  autoCommitMaxSizeExp;
  public XPathExpression autoCommitMaxTimeExp;
  public XPathExpression  indexWriterCloseWaitsForMergesExp;
  public XPathExpression autoCommitOpenSearcherExp;
  public XPathExpression autoSoftCommitMaxTimeExp;
  public XPathExpression autoSoftCommitMaxDocsExp;
  public XPathExpression  commitWithinSoftCommitExp;

  public XPathExpression useCompoundFileExp;
  public XPathExpression maxBufferedDocsExp;
  public XPathExpression ramPerThreadHardLimitMBExp;
  public XPathExpression writeLockTimeoutExp;

  public XPathExpression maxMergeDocsExp;
  public XPathExpression infoStreamExp;
  public XPathExpression mergeFactorExp;

  public XPathExpression indexConfigExp;
  public XPathExpression mergeSchedulerExp;
  public XPathExpression mergePolicyExp;
  public XPathExpression ramBufferSizeMBExp;
  public XPathExpression checkIntegrityAtMergeExp;

  public XPathExpression versionBucketLockTimeoutMsExp;

  Configuration conf;

  com.fasterxml.aalto.sax.SAXParserFactoryImpl parser = new com.fasterxml.aalto.sax.SAXParserFactoryImpl();


  {

    parser.setValidating(false);
    parser.setXIncludeAware(false);
    conf = Configuration.newConfiguration();
    //conf.setSourceParserClass("com.fasterxml.aalto.sax.SAXParserFactoryImpl");
    //    conf.setNamePool(this.conf.getNamePool());
    //    conf.setDocumentNumberAllocator(this.conf.getDocumentNumberAllocator());
    // conf.setXIncludeAware(true);
    conf.setExpandAttributeDefaults(false);
    conf.setValidation(false);

    xpathFactory = new XPathFactoryImpl(conf);;

    refreshConf();
  }

  public void refreshConf() {
    try {

      XPath xpath = xpathFactory.newXPath();

      luceneMatchVersionExp = xpath.compile(luceneMatchVersionPath);

      indexDefaultsExp = xpath.compile(indexDefaultsPath);

      mainIndexExp = xpath.compile(mainIndexPath);

      nrtModeExp = xpath.compile(nrtModePath);

      unlockOnStartupExp = xpath.compile(unlockOnStartupPath);

      shardHandlerFactoryExp = xpath.compile(shardHandlerFactoryPath);

      counterExp = xpath.compile(counterExpPath);

      meterExp = xpath.compile(meterPath);

      timerExp = xpath.compile(timerPath);

      histoExp = xpath.compile(histoPath);

      historyExp = xpath.compile(historyPath);

      transientCoreCacheFactoryExp = xpath.compile(transientCoreCacheFactoryPath);

      tracerConfigExp = xpath.compile(tracerConfigPath);

      coreLoadThreadsExp = xpath.compile(coreLoadThreadsPath);

      persistentExp = xpath.compile(persistentPath);

      sharedLibExp = xpath.compile(sharedLibPath);

      zkHostExp = xpath.compile(zkHostPath);

      coresExp = xpath.compile(coresPath);

      String expression =
          IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.FIELD) + IndexSchema.XPATH_OR + IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.DYNAMIC_FIELD) + IndexSchema.XPATH_OR
              + IndexSchema.stepsToPath(IndexSchema.SCHEMA, IndexSchema.FIELDS, IndexSchema.FIELD) + IndexSchema.XPATH_OR + IndexSchema
              .stepsToPath(IndexSchema.SCHEMA, IndexSchema.FIELDS, IndexSchema.DYNAMIC_FIELD);
      xpathOrExp = xpath.compile(expression);

      schemaNameExp = xpath.compile(schemaNamePath);

      schemaVersionExp = xpath.compile(schemaVersionPath);

      schemaSimExp = xpath.compile(schemaSimPath);

      defaultSearchFieldExp = xpath.compile(defaultSearchFieldPath);

      solrQueryParserDefaultOpExp = xpath.compile(solrQueryParserDefaultOpPath);

      schemaUniqueKeyExp = xpath.compile(schemaUniqueKeyPath);

      fieldTypeXPathExpressionsExp = xpath.compile(fieldTypeXPathExpressions);

      copyFieldsExp = xpath.compile(copyFieldPath);

      metricsReporterExp = xpath.compile(metricsReporterPath);

      try {
        analyzerQueryExp = xpath.compile("./analyzer[@type='query']");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        analyzerMultiTermExp = xpath.compile("./analyzer[@type='multiterm']");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }

      try {
        analyzerIndexExp = xpath.compile("./analyzer[not(@type)] | ./analyzer[@type='index']");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        similarityExp = xpath.compile("./similarity");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        charFilterExp = xpath.compile("./charFilter");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        tokenizerExp = xpath.compile("./tokenizer");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        filterExp = xpath.compile("./filter");
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        filterCacheExp = xpath.compile(filterCachePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        documentCacheExp = xpath.compile(documentCachePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        queryResultCacheExp = xpath.compile(queryResultCachePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        fieldValueCacheExp = xpath.compile(fieldValueCachePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }

      try {
        useFilterForSortedQueryExp = xpath.compile(useFilterForSortedQueryPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        queryResultWindowSizeeExp = xpath.compile(queryResultWindowSizePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        enableLazyFieldLoadingExp = xpath.compile(enableLazyFieldLoadingPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        queryResultMaxDocsCachedExp = xpath.compile(queryResultMaxDocsCachedPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        useRangeVersionsExp = xpath.compile(useRangeVersionsPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        maxBooleanClausesExp = xpath.compile(maxBooleanClausesPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        useColdSearcherExp = xpath.compile(useColdSearcherPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }

      try {
        multipartUploadLimitInKBExp = xpath.compile(multipartUploadLimitInKBPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        formdataUploadLimitInKBExp = xpath.compile(formdataUploadLimitInKBPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        enableRemoteStreamingExp = xpath.compile(enableRemoteStreamingPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        enableStreamBodyExp = xpath.compile(enableStreamBodyPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        handleSelectExp = xpath.compile(handleSelectPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        addHttpRequestToContextExp = xpath.compile(addHttpRequestToContextPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        maxWarmingSearchersExp = xpath.compile(maxWarmingSearchersPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        never304Exp = xpath.compile(never304Path);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        slowQueryThresholdMillisExp = xpath.compile(slowQueryThresholdMillisPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }


      try {
        updateHandlerClassExp = xpath.compile(updateHandlerClassPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        autoCommitMaxDocsExp = xpath.compile(autoCommitMaxDocsPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        autoCommitMaxTimeExp = xpath.compile(autoCommitMaxTimePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        autoCommitMaxSizeExp = xpath.compile(autoCommitMaxSizePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        indexWriterCloseWaitsForMergesExp = xpath.compile(indexWriterCloseWaitsForMergesPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        autoCommitOpenSearcherExp = xpath.compile(autoCommitOpenSearcherPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        autoSoftCommitMaxTimeExp = xpath.compile(autoSoftCommitMaxTimePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        autoSoftCommitMaxDocsExp = xpath.compile(autoSoftCommitMaxDocsPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        commitWithinSoftCommitExp = xpath.compile(commitWithinSoftCommitPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }


      try {
        useCompoundFileExp = xpath.compile(useCompoundFilePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        maxBufferedDocsExp = xpath.compile(maxBufferedDocsPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        ramPerThreadHardLimitMBExp = xpath.compile(ramPerThreadHardLimitMBPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        writeLockTimeoutExp = xpath.compile(writeLockTimeoutPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }

      try {
        maxMergeDocsExp = xpath.compile(maxMergeDocPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        infoStreamExp = xpath.compile(infoStreamPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        mergeFactorExp = xpath.compile(mergeFactorPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        indexConfigExp = xpath.compile(indexConfigPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        mergeSchedulerExp = xpath.compile(mergeSchedulerPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        mergePolicyExp = xpath.compile(mergePolicyPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        ramBufferSizeMBExp = xpath.compile(ramBufferSizeMBPath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
      try {
        checkIntegrityAtMergeExp = xpath.compile(checkIntegrityAtMergePath);
      } catch (XPathExpressionException e) {
        log.error("", e);
      }
    } catch (Exception e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override public void close() throws IOException {
    if (conf != null) {
      try {
        conf.close();
      } catch (Exception e) {
        log.info("Exception closing Configuration " + e.getClass().getName() + " " + e.getMessage());
      }
    }
  }
}
