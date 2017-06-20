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

import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SimilarityFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Utility class for converting a JSON definition of a FieldType into the
 * XML format expected by the FieldTypePluginLoader.
 */
public class FieldTypeXmlAdapter {
  
  public static Node toNode(Map<String,?> json) {
    DocumentBuilder docBuilder;
    try {
      docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    
    Document doc = docBuilder.newDocument();    
    Element fieldType = doc.createElement(IndexSchema.FIELD_TYPE);
    appendAttrs(fieldType, json);
    
    // transform the analyzer definitions into XML elements
    Element analyzer = transformAnalyzer(doc, json, "analyzer", null);
    if (analyzer != null)
      fieldType.appendChild(analyzer);

    analyzer = transformAnalyzer(doc, json, "indexAnalyzer", "index");
    if (analyzer != null)
      fieldType.appendChild(analyzer);

    analyzer = transformAnalyzer(doc, json, "queryAnalyzer", "query");
    if (analyzer != null)
      fieldType.appendChild(analyzer);

    analyzer = transformAnalyzer(doc, json, "multiTermAnalyzer", "multiterm");
    if (analyzer != null)
      fieldType.appendChild(analyzer);

    Element similarity = transformSimilarity(doc, json, "similarity");
    if (similarity != null)
      fieldType.appendChild(similarity);
        
    return fieldType;
  }

  @SuppressWarnings("unchecked")
  protected static Element transformSimilarity(Document doc, Map<String,?> json, String jsonFieldName) {
    Object jsonField = json.get(jsonFieldName);
    if (jsonField == null)
      return null; // it's ok for this field to not exist in the JSON map

    if (!(jsonField instanceof Map))
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid fieldType definition! Expected JSON object for "+
          jsonFieldName+" not a "+jsonField.getClass().getName());

    Element similarity = doc.createElement("similarity");
    Map<String,?> config = (Map<String,?>)jsonField;
    similarity.setAttribute(SimilarityFactory.CLASS_NAME, (String)config.remove(SimilarityFactory.CLASS_NAME));
    for (Map.Entry<String,?> entry : config.entrySet()) {
      Object val = entry.getValue();
      if (val != null) {
        Element child = doc.createElement(classToXmlTag(val.getClass()));
        child.setAttribute(CommonParams.NAME, entry.getKey());
        child.setTextContent(entry.getValue().toString());
        similarity.appendChild(child);
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
  protected static Element transformAnalyzer(Document doc, Map<String,?> json, String jsonFieldName, String analyzerType) {
    Object jsonField = json.get(jsonFieldName);
    if (jsonField == null)
      return null; // it's ok for this field to not exist in the JSON map
    
    if (!(jsonField instanceof Map))
      throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid fieldType definition! Expected JSON object for "+
         jsonFieldName+" not a "+jsonField.getClass().getName());
    
    return createAnalyzerElement(doc, analyzerType, (Map<String,?>)jsonField);    
  }
  
  @SuppressWarnings("unchecked")
  protected static Element createAnalyzerElement(Document doc, String type, Map<String,?> analyzer) {
    Element analyzerElem = appendAttrs(doc.createElement("analyzer"), analyzer);
    if (type != null)
      analyzerElem.setAttribute("type", type);

    List<Map<String,?>> charFilters = (List<Map<String,?>>)analyzer.get("charFilters");
    Map<String,?> tokenizer = (Map<String,?>)analyzer.get("tokenizer");
    List<Map<String,?>> filters = (List<Map<String,?>>)analyzer.get("filters");

    if (analyzer.get("class") == null) {
      if (charFilters != null)
        appendFilterElements(doc, analyzerElem, "charFilter", charFilters);

      if (tokenizer == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Analyzer must define a tokenizer!");

      if (tokenizer.get("class") == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, "Every tokenizer must define a class property!");

      analyzerElem.appendChild(appendAttrs(doc.createElement("tokenizer"), tokenizer));

      if (filters != null)
        appendFilterElements(doc, analyzerElem, "filter", filters);

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
  
  protected static void appendFilterElements(Document doc, Element analyzer, String filterName, List<Map<String,?>> filters) {
    for (Map<String,?> next : filters) {
      String filterClass = (String)next.get("class");
      if (filterClass == null)
        throw new SolrException(ErrorCode.BAD_REQUEST, 
            "Every "+filterName+" must define a class property!");      
      analyzer.appendChild(appendAttrs(doc.createElement(filterName), next));
    }    
  }
  
  protected static Element appendAttrs(Element elm, Map<String,?> json) {
    for (Map.Entry<String,?> entry : json.entrySet()) {
      Object val = entry.getValue();
      if (val != null && !(val instanceof Map))
        elm.setAttribute(entry.getKey(), val.toString());
    }
    return elm;
  }
}
