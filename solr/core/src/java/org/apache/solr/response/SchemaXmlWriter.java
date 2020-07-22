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
package org.apache.solr.response;
import java.io.IOException;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.XML;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.search.ReturnFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * @lucene.internal
 */
public class SchemaXmlWriter extends TextResponseWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final char[] XML_DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".toCharArray();
  private static final char[] MANAGED_SCHEMA_DO_NOT_EDIT_WARNING 
      = "<!-- Solr managed schema - automatically generated - DO NOT EDIT -->".toCharArray();
  
  private boolean emitManagedSchemaDoNotEditWarning = false;
  public void setEmitManagedSchemaDoNotEditWarning(boolean emitManagedSchemaDoNotEditWarning) { 
    this.emitManagedSchemaDoNotEditWarning = emitManagedSchemaDoNotEditWarning; 
  }

  public static void writeResponse(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    SchemaXmlWriter schemaXmlWriter = null;
    try {
      schemaXmlWriter = new SchemaXmlWriter(writer, req, rsp);
      schemaXmlWriter.writeResponse();
    } finally {
      if (null != schemaXmlWriter) {
        schemaXmlWriter.close();
      }
    }
  }

  public SchemaXmlWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(writer, req, rsp);
  }

  @SuppressWarnings({"unchecked"})
  public void writeResponse() throws IOException {
    
    writer.write(XML_DECLARATION);
    if (emitManagedSchemaDoNotEditWarning) {
      if (doIndent) {
        writer.write('\n');
      }
      writer.write(MANAGED_SCHEMA_DO_NOT_EDIT_WARNING);
    }

    Map<String,Object> schemaProperties
        = (Map<String , Object>)rsp.getValues().get(IndexSchema.SCHEMA);

    openStartTag(IndexSchema.SCHEMA);
    writeAttr(IndexSchema.NAME, schemaProperties.get(IndexSchema.NAME).toString());
    writeAttr(IndexSchema.VERSION, schemaProperties.get(IndexSchema.VERSION).toString());
    closeStartTag(false);
    incLevel();

    for (Map.Entry<String, Object> entry : schemaProperties.entrySet()) {
      String schemaPropName = entry.getKey();
      Object val = entry.getValue();
      if (schemaPropName.equals(IndexSchema.NAME) || schemaPropName.equals(IndexSchema.VERSION)) {
        continue;
      }
      if (schemaPropName.equals(IndexSchema.UNIQUE_KEY)) {
        openStartTag(IndexSchema.UNIQUE_KEY);
        closeStartTag(false);
        writer.write(val.toString());
        endTag(IndexSchema.UNIQUE_KEY, false);
      } else if (schemaPropName.equals(IndexSchema.SIMILARITY)) {
        writeSimilarity((SimpleOrderedMap<Object>) val);
      } else if (schemaPropName.equals(IndexSchema.FIELD_TYPES)) {
        writeFieldTypes((List<SimpleOrderedMap<Object>>) val);
      } else if (schemaPropName.equals(IndexSchema.FIELDS)) {
        List<SimpleOrderedMap<Object>> fieldPropertiesList
            = (List<SimpleOrderedMap<Object>>) val;
        for (SimpleOrderedMap<Object> fieldProperties : fieldPropertiesList) {
          openStartTag(IndexSchema.FIELD);
          for (int fieldPropNum = 0 ; fieldPropNum < fieldProperties.size() ; ++fieldPropNum) {
            writeAttr(fieldProperties.getName(fieldPropNum), fieldProperties.getVal(fieldPropNum).toString());
          }
          closeStartTag(true);
        }
      } else if (schemaPropName.equals(IndexSchema.DYNAMIC_FIELDS)) {
        List<SimpleOrderedMap<Object>> dynamicFieldPropertiesList
            = (List<SimpleOrderedMap<Object>>) val;
        for (SimpleOrderedMap<Object> dynamicFieldProperties : dynamicFieldPropertiesList) {
          openStartTag(IndexSchema.DYNAMIC_FIELD);
          for (int dynamicFieldPropNum = 0 ; dynamicFieldPropNum < dynamicFieldProperties.size() ; ++dynamicFieldPropNum) {
            writeAttr(dynamicFieldProperties.getName(dynamicFieldPropNum), 
                      dynamicFieldProperties.getVal(dynamicFieldPropNum).toString());
          }
          closeStartTag(true);
        }
      } else if (schemaPropName.equals(IndexSchema.COPY_FIELDS)) {
        List<SimpleOrderedMap<Object>> copyFieldPropertiesList
            = (List<SimpleOrderedMap<Object>>) val;
        for (SimpleOrderedMap<Object> copyFieldProperties : copyFieldPropertiesList) {
          openStartTag(IndexSchema.COPY_FIELD);
          for (int copyFieldPropNum = 0 ; copyFieldPropNum < copyFieldProperties.size() ; ++ copyFieldPropNum) {
            writeAttr(copyFieldProperties.getName(copyFieldPropNum), 
                      copyFieldProperties.getVal(copyFieldPropNum).toString());
          }
          closeStartTag(true);
        }
      } else {
        log.warn("Unknown schema component '{}'", schemaPropName);
      }
    }
    decLevel();
    endTag(IndexSchema.SCHEMA);
  }

  @SuppressWarnings({"unchecked"})
  private void writeFieldTypes(List<SimpleOrderedMap<Object>> fieldTypePropertiesList) throws IOException {
    for (SimpleOrderedMap<Object> fieldTypeProperties : fieldTypePropertiesList) {
      SimpleOrderedMap<Object> analyzerProperties = null;
      SimpleOrderedMap<Object> indexAnalyzerProperties = null;
      SimpleOrderedMap<Object> queryAnalyzerProperties = null;
      SimpleOrderedMap<Object> multiTermAnalyzerProperties = null;
      SimpleOrderedMap<Object> perFieldSimilarityProperties = null;
      openStartTag(IndexSchema.FIELD_TYPE);
      for (int fieldTypePropNum = 0 ; fieldTypePropNum < fieldTypeProperties.size() ; ++fieldTypePropNum) {
        String fieldTypePropName = fieldTypeProperties.getName(fieldTypePropNum);
        if (fieldTypePropName.equals(FieldType.ANALYZER)) {
          analyzerProperties = (SimpleOrderedMap<Object>)fieldTypeProperties.getVal(fieldTypePropNum);
        } else if (fieldTypePropName.equals(FieldType.INDEX_ANALYZER)) {
          indexAnalyzerProperties =  (SimpleOrderedMap<Object>)fieldTypeProperties.getVal(fieldTypePropNum); 
        } else if (fieldTypePropName.equals(FieldType.QUERY_ANALYZER)) {
          queryAnalyzerProperties = (SimpleOrderedMap<Object>)fieldTypeProperties.getVal(fieldTypePropNum);
        } else if (fieldTypePropName.equals(FieldType.MULTI_TERM_ANALYZER)) {
          multiTermAnalyzerProperties = (SimpleOrderedMap<Object>)fieldTypeProperties.getVal(fieldTypePropNum);
        } else if (fieldTypePropName.equals(FieldType.SIMILARITY)) {
          perFieldSimilarityProperties = (SimpleOrderedMap<Object>)fieldTypeProperties.getVal(fieldTypePropNum);
        } else {
          writeAttr(fieldTypePropName, fieldTypeProperties.getVal(fieldTypePropNum).toString());
        }
      }
      boolean isEmptyTag = null == analyzerProperties           && null == indexAnalyzerProperties
                        && null == queryAnalyzerProperties      && null == multiTermAnalyzerProperties
                        && null == perFieldSimilarityProperties;
      if (isEmptyTag) {
        closeStartTag(true);
      } else {
        closeStartTag(false);
        incLevel();
        if (null != analyzerProperties)           writeAnalyzer(analyzerProperties, null);
        if (null != indexAnalyzerProperties)      writeAnalyzer(indexAnalyzerProperties, FieldType.INDEX);
        if (null != queryAnalyzerProperties)      writeAnalyzer(queryAnalyzerProperties, FieldType.QUERY);
        if (null != multiTermAnalyzerProperties)  writeAnalyzer(multiTermAnalyzerProperties, FieldType.MULTI_TERM);
        if (null != perFieldSimilarityProperties) writeSimilarity(perFieldSimilarityProperties);
        decLevel();
        endTag(IndexSchema.FIELD_TYPE);
      }
    }
  }

  private void writeSimilarity(SimpleOrderedMap<Object> similarityProperties) throws IOException {
    openStartTag(IndexSchema.SIMILARITY);
    writeAttr(SimilarityFactory.CLASS_NAME, similarityProperties.get(SimilarityFactory.CLASS_NAME).toString());
    if (similarityProperties.size() > 1) {
      closeStartTag(false);
      incLevel();
      writeNamedList(null, similarityProperties);
      decLevel();
      endTag(IndexSchema.SIMILARITY);
    } else {
      closeStartTag(true);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void writeAnalyzer(SimpleOrderedMap<Object> analyzerProperties, String analyzerType) throws IOException {
    openStartTag(FieldType.ANALYZER);
    if (null != analyzerType) {
      writeAttr(FieldType.TYPE, analyzerType);
    }
    List<SimpleOrderedMap<Object>> charFilterPropertiesList = null;
    SimpleOrderedMap<Object> tokenizerProperties = null;
    List<SimpleOrderedMap<Object>> filterPropertiesList = null;
    for (int i = 0 ; i < analyzerProperties.size() ; ++i) {
      String name = analyzerProperties.getName(i);
      if (name.equals(FieldType.CHAR_FILTERS)) {
        charFilterPropertiesList = (List<SimpleOrderedMap<Object>>)analyzerProperties.getVal(i);
      } else if (name.equals(FieldType.TOKENIZER)) {
        tokenizerProperties = (SimpleOrderedMap<Object>)analyzerProperties.getVal(i);
      } else if (name.equals(FieldType.FILTERS)) {
        filterPropertiesList = (List<SimpleOrderedMap<Object>>)analyzerProperties.getVal(i);
      } else if (name.equals(FieldType.CLASS_NAME)) {
        if ( ! "solr.TokenizerChain".equals(analyzerProperties.getVal(i))) {
          writeAttr(name, analyzerProperties.getVal(i).toString());
        }
      } else if (name.equals(IndexSchema.LUCENE_MATCH_VERSION_PARAM)) {
        writeAttr(name, analyzerProperties.getVal(i).toString());
      }
    }
    boolean isEmptyTag
        = null == charFilterPropertiesList && null == tokenizerProperties && null == filterPropertiesList;
    if (isEmptyTag) {
      closeStartTag(true);
    } else {
      closeStartTag(false);
      incLevel();
      if (null != charFilterPropertiesList) {
        for (SimpleOrderedMap<Object> charFilterProperties : charFilterPropertiesList) {
          openStartTag(FieldType.CHAR_FILTER);
          for (int i = 0 ; i < charFilterProperties.size() ; ++i) {
            writeAttr(charFilterProperties.getName(i), charFilterProperties.getVal(i).toString());
          }
          closeStartTag(true);
        }
      }
      if (null != tokenizerProperties) {
        openStartTag(FieldType.TOKENIZER);
        for (int i = 0 ; i < tokenizerProperties.size() ; ++i) {
          writeAttr(tokenizerProperties.getName(i), tokenizerProperties.getVal(i).toString());
        }
        closeStartTag(true);
      }
      if (null != filterPropertiesList) {
        for (SimpleOrderedMap<Object> filterProperties : filterPropertiesList) {
          openStartTag(FieldType.FILTER);
          for (int i = 0 ; i < filterProperties.size() ; ++i) {
            writeAttr(filterProperties.getName(i), filterProperties.getVal(i).toString());
          }
          closeStartTag(true);
        }
      }
      decLevel();
      endTag(FieldType.ANALYZER);
    }
  }

  void openStartTag(String tag) throws IOException {
    if (doIndent) indent();
    writer.write('<');
    writer.write(tag);
  }
  
  void closeStartTag(boolean isEmptyTag) throws IOException {
    if (isEmptyTag) writer.write('/');
    writer.write('>');
  }

  void endTag(String tag) throws IOException {
    endTag(tag, true);
  }
  
  void endTag(String tag, boolean indentThisTag) throws IOException {
    if (doIndent && indentThisTag) indent();

    writer.write('<');
    writer.write('/');
    writer.write(tag);
    writer.write('>');
  }

  /** Writes the XML attribute name/val. A null val means that the attribute is missing. */
  private void writeAttr(String name, String val) throws IOException {
    writeAttr(name, val, true);
  }

  public void writeAttr(String name, String val, boolean escape) throws IOException{
    if (val != null) {
      writer.write(' ');
      writer.write(name);
      writer.write("=\"");
      if (escape){
        XML.escapeAttributeValue(val, writer);
      } else {
        writer.write(val);
      }
      writer.write('"');
    }
  }

  @Override
  public void writeNamedList(String name, @SuppressWarnings({"rawtypes"})NamedList val) throws IOException {
    // name is ignored - this method is only used for SimilarityFactory
    int sz = val.size();
    for (int i=0; i<sz; i++) {
      String valName = val.getName(i);
      if ( ! valName.equals(SimilarityFactory.CLASS_NAME)) {
        writeVal(valName, val.getVal(i));
      }
    }
  }

  void startTag(String tag, String name, boolean closeTag) throws IOException {
    if (doIndent) indent();

    writer.write('<');
    writer.write(tag);
    if (name!=null) {
      writeAttr(NAME, name);
      if (closeTag) {
        writer.write("/>");
      } else {
        writer.write(">");
      }
    } else {
      if (closeTag) {
        writer.write("/>");
      } else {
        writer.write('>');
      }
    }
  }


  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void writeMap(String name, @SuppressWarnings({"rawtypes"})Map map, boolean excludeOuter, boolean isFirstVal) throws IOException {
    int sz = map.size();

    if (!excludeOuter) {
      startTag("lst", name, sz<=0);
      incLevel();
    }

    for (Map.Entry entry : (Set<Map.Entry>)map.entrySet()) {
      Object k = entry.getKey();
      Object v = entry.getValue();
      // if (sz<indentThreshold) indent();
      writeVal( null == k ? null : k.toString(), v);
    }

    if (!excludeOuter) {
      decLevel();
      if (sz > 0) {
        if (doIndent) indent();
        writer.write("</lst>");
      }
    }
  }

  @Override
  public void writeArray(String name, Object[] val) throws IOException {
    writeArray(name, Arrays.asList(val).iterator());
  }

  @Override
  public void writeArray(String name, @SuppressWarnings({"rawtypes"})Iterator iter) throws IOException {
    if( iter.hasNext() ) {
      startTag("arr", name, false );
      incLevel();
      while( iter.hasNext() ) {
        writeVal(null, iter.next());
      }
      decLevel();
      if (doIndent) indent();
      writer.write("</arr>");
    }
    else {
      startTag("arr", name, true );
    }
  }

  //
  // Primitive types
  //

  @Override
  public void writeNull(String name) throws IOException {
    writePrim("null",name,"",false);
  }

  @Override
  public void writeStr(String name, String val, boolean escape) throws IOException {
    writePrim("str",name,val,escape);
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    writePrim("int",name,val,false);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    writePrim("long",name,val,false);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    writePrim("bool",name,val,false);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    writePrim("float",name,val,false);
  }

  @Override
  public void writeFloat(String name, float val) throws IOException {
    writeFloat(name,Float.toString(val));
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    writePrim("double",name,val,false);
  }

  @Override
  public void writeDouble(String name, double val) throws IOException {
    writeDouble(name,Double.toString(val));
  }


  @Override
  public void writeDate(String name, String val) throws IOException {
    writePrim("date",name,val,false);
  }


  //
  // OPT - specific writeInt, writeFloat, methods might be faster since
  // there would be less write calls (write("<int name=\"" + name + ... + </int>)
  //
  private void writePrim(String tag, String name, String val, boolean escape) throws IOException {
    int contentLen = val==null ? 0 : val.length();

    startTag(tag, name, contentLen==0);
    if (contentLen==0) return;

    if (escape) {
      XML.escapeCharData(val,writer);
    } else {
      writer.write(val,0,contentLen);
    }

    writer.write('<');
    writer.write('/');
    writer.write(tag);
    writer.write('>');
  }

  @Override
  public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore) throws IOException {
    // no-op
  }
  
  @Override
  public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException {
    // no-op
  }

  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields returnFields, int idx) throws IOException {
    // no-op
  }

  @Override
  public void writeEndDocumentList() throws IOException {
    // no-op
  }
}
