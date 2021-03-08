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
package org.apache.solr.rest.schema;

import net.sf.saxon.event.PipelineConfiguration;
import net.sf.saxon.lib.ParseOptions;
import net.sf.saxon.lib.Validation;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.sapling.SaplingElement;
import net.sf.saxon.sapling.Saplings;
import net.sf.saxon.trans.XPathException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.xerces.jaxp.DocumentBuilderFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting a JSON definition of a FieldType into the
 * XML format expected by the FieldTypePluginLoader.
 */
public class FieldTypeXmlAdapter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final static ThreadLocal<DocumentBuilder> THREAD_LOCAL_DB= new ThreadLocal<>() {
    protected DocumentBuilder initialValue() {
      DocumentBuilder db;
      try {
        db = dbf.newDocumentBuilder();
      } catch (ParserConfigurationException e) {
        log.error("Error in parser configuration", e);
        throw new RuntimeException(e);
      }
      return  db;
    }
  };


  public static final javax.xml.parsers.DocumentBuilderFactory dbf;

  static {
    dbf = new DocumentBuilderFactoryImpl();
    try {
      dbf.setXIncludeAware(true);
      dbf.setNamespaceAware(true);
      dbf.setValidating(false);
    //  trySetDOMFeature(dbf, XMLConstants.FEATURE_SECURE_PROCESSING, true);
    } catch(UnsupportedOperationException e) {
      log.warn("XML parser doesn't support XInclude option", e);
    }
  }

  public static DocumentBuilder getDocumentBuilder() {
    DocumentBuilder db = THREAD_LOCAL_DB.get();
    return db;
  }


  public static void trySetDOMFeature(DocumentBuilderFactory factory, String feature, boolean enabled) {
    try {
      factory.setFeature(feature, enabled);
    } catch (Exception ex) {
      ParWork.propagateInterrupt(ex);
      // ignore
    }
  }

  public static NodeInfo toNode(SolrResourceLoader loader, Map<String,?> json) {

    SaplingElement fieldType = Saplings.elem(IndexSchema.FIELD_TYPE);
    fieldType = appendAttrs(fieldType, json);

    // transform the analyzer definitions into XML elements
    SaplingElement analyzer = transformAnalyzer(fieldType, json, "analyzer", null);
    if (analyzer != null) fieldType = fieldType.withChild(analyzer);

    analyzer = transformAnalyzer(fieldType, json, "indexAnalyzer", "index");
    if (analyzer != null) fieldType = fieldType.withChild(analyzer);

    analyzer = transformAnalyzer(fieldType, json, "queryAnalyzer", "query");
    if (analyzer != null) fieldType = fieldType.withChild(analyzer);

    analyzer = transformAnalyzer(fieldType, json, "multiTermAnalyzer", "multiterm");
    if (analyzer != null) fieldType = fieldType.withChild(analyzer);

    SaplingElement similarity = transformSimilarity(fieldType, json, "similarity");
    if (similarity != null) fieldType = fieldType.withChild(similarity);


    try {

      PipelineConfiguration plc = loader.getConf().makePipelineConfiguration();

      ParseOptions po = plc.getParseOptions();
      po.setEntityResolver(loader.getSysIdResolver());
      po.setXMLReader(loader.getXmlReader());
      // Set via conf already
      // po.setXIncludeAware(true);
      //  po.setExpandAttributeDefaults(true);
      //  po.setCheckEntityReferences(false);
      //po.setDTDValidationMode(Validation.STRIP);

      po.setPleaseCloseAfterUse(true);

      return fieldType.toNodeInfo(plc.getConfiguration());
    } catch (XPathException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  @SuppressWarnings("unchecked")
  protected static SaplingElement transformSimilarity(SaplingElement doc, Map<String,?> json, String jsonFieldName) {
    Object jsonField = json.get(jsonFieldName);
    if (jsonField == null)
      return null; // it's ok for this field to not exist in the JSON map

    if (!(jsonField instanceof Map))
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid fieldType definition! Expected JSON object for "+
          jsonFieldName+" not a "+jsonField.getClass().getName());

    SaplingElement similarity = Saplings.elem("similarity");
    Map<String,?> config = (Map<String,?>)jsonField;
    similarity = similarity.withAttr(SimilarityFactory.CLASS_NAME, (String)config.remove(SimilarityFactory.CLASS_NAME));
    for (Map.Entry<String,?> entry : config.entrySet()) {
      Object val = entry.getValue();
      if (val != null) {
        SaplingElement child = Saplings.elem(classToXmlTag(val.getClass()));
        child = child.withAttr(CommonParams.NAME, entry.getKey());
        child = child.withText(entry.getValue().toString());
        similarity = similarity.withChild(child);
      }
    }
    return similarity;
  }

  /** Convert types produced by noggit's ObjectBuilder (Boolean, Double, Long, String) to plugin param XML tags. */
  protected static String classToXmlTag(Class<?> clazz) {
    switch (clazz.getSimpleName()) {
      case "Boolean": return "bool";
      case "Double":  return "double";
      case "Long":    return "long";
      case "String":  return "str";
    }
    throw new SolrException(ErrorCode.BAD_REQUEST, "Unsupported object type '" + clazz.getSimpleName() + "'");
  }
  
  @SuppressWarnings("unchecked")
  protected static SaplingElement transformAnalyzer(SaplingElement doc, Map<String,?> json, String jsonFieldName, String analyzerType) {
    Object jsonField = json.get(jsonFieldName);
    if (jsonField == null)
      return null; // it's ok for this field to not exist in the JSON map
    
    if (!(jsonField instanceof Map))
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid fieldType definition! Expected JSON object for "+
         jsonFieldName+" not a "+jsonField.getClass().getName());
    
    return createAnalyzerElement(doc, analyzerType, (Map<String,?>)jsonField);    
  }
  
  @SuppressWarnings("unchecked")
  protected static SaplingElement createAnalyzerElement(SaplingElement doc, String type, Map<String,?> analyzer) {
    SaplingElement analyzerElem = appendAttrs(Saplings.elem("analyzer"), analyzer);
    if (type != null)
      analyzerElem = analyzerElem.withAttr("type", type);

    List<Map<String,?>> charFilters = (List<Map<String,?>>)analyzer.get("charFilters");
    Map<String,?> tokenizer = (Map<String,?>)analyzer.get("tokenizer");
    List<Map<String,?>> filters = (List<Map<String,?>>)analyzer.get("filters");

    if (analyzer.get("class") == null) {
      if (charFilters != null)
        analyzerElem = appendFilterElements(doc, analyzerElem, "charFilter", charFilters);

      if (tokenizer == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Analyzer must define a tokenizer!");

      if (tokenizer.get("class") == null && tokenizer.get("name") == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Every tokenizer must define a class or name property!");

      analyzerElem = analyzerElem.withChild(appendAttrs(Saplings.elem(("tokenizer")), tokenizer));

      if (filters != null)
        analyzerElem = appendFilterElements(doc, analyzerElem, "filter", filters);

    } else { // When analyzer class is specified: char filters, tokenizers, and filters are disallowed
      if (charFilters != null)
        throw new SolrException
            (ErrorCode.BAD_REQUEST, "An analyzer with a class property may not define any char filters!");

      if (tokenizer != null)
        throw new SolrException
            (ErrorCode.BAD_REQUEST, "An analyzer with a class property may not define a tokenizer!");

      if (filters != null)
        throw new SolrException
            (ErrorCode.BAD_REQUEST, "An analyzer with a class property may not define any filters!");
    }
    
    return analyzerElem;
  }
  
  protected static SaplingElement appendFilterElements(SaplingElement doc, SaplingElement analyzer, String filterName, List<Map<String,?>> filters) {
    for (Map<String,?> next : filters) {
      String filterClass = (String)next.get("class");
      String filterSPIName = (String)next.get("name");
      if (filterClass == null && filterSPIName == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, 
            "Every "+filterName+" must define a class or name property!");
      analyzer = analyzer.withChild(appendAttrs(Saplings.elem(filterName), next));
    }
    return analyzer;
  }
  
  protected static SaplingElement appendAttrs(SaplingElement elm, Map<String,?> json) {
    for (Map.Entry<String,?> entry : json.entrySet()) {
      Object val = entry.getValue();
      if (val != null && !(val instanceof Map))
        elm = elm.withAttr(entry.getKey(), val.toString());
    }
    return elm;
  }
}
