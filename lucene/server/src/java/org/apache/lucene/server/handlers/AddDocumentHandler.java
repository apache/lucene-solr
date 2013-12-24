package org.apache.lucene.server.handlers;

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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.server.Constants;
import org.apache.lucene.server.FieldDef;
import org.apache.lucene.server.FinishRequest;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState.DocumentAndFacets;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.params.*;
import org.apache.lucene.util.BytesRef;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

public class AddDocumentHandler extends Handler {

  public final static StructType DOCUMENT_TYPE = new StructType(
                                       new Param("fields", "Fields to index into one document",
                                           new StructType(
                                                          new Param("anyFieldName", "A name/value pair for this document.  Multiple name/values can be specified, but each field name must already have been registered via @registerFields.  The type of the value must match how the field was registered.", new AnyType()))));

  final StructType TYPE = new StructType(
                                     new Param("indexName", "Index name", new StringType()));

  public AddDocumentHandler(GlobalState state) {
    super(state);
    TYPE.params.putAll(DOCUMENT_TYPE.params);
  }

  @Override
  public StructType getType() {
    return TYPE;
  }

  @Override
  public String getTopDoc() {
    return "Adds one document to the index.  Returns the index generation (indexGen) that contains this added document.";
  }

  private static class MyField extends Field {
    public MyField(String name, FieldType ft, Object value) {
      super(name, ft);
      fieldsData = value;
    }
  }

  // NOTE: only used by BinaryDocumentPlugin
  @SuppressWarnings({"unchecked"})
  public static void parseOneValue(FieldDef fd, DocumentAndFacets doc, Object o, float boost) {

    assert fd != null;
    
    if (fd.fieldType.stored() || fd.fieldType.indexed() || fd.fieldType.docValueType() != null) {
      if (fd.valueType.equals("text") || fd.valueType.equals("atom")) {
        if (!(o instanceof String)) {
          fail(fd.name, "expected String value but got " + (o == null ? "null" : o.getClass().toString()));
        }
      } else if (fd.valueType.equals("boolean")) {
        if (!(o instanceof Boolean)) {
          fail(fd.name, "expected Boolean value but got " + o.getClass());
        }
      } else {
        // nocommit tighten this?  ie should be Float/Double
        // if valueType is float/double:
        if (!(o instanceof Number)) {
          fail(fd.name, "expected Number value but got " + o);
        }
      }
    }

    if (fd.faceted.equals("flat")) {
      if (o instanceof JSONArray) {
        fail(fd.name, "value should be String when facet=flat; got array");
      }
      if (doc.facets == null) {
        doc.facets = new ArrayList<CategoryPath>();
      }
      doc.facets.add(new CategoryPath(fd.name, o.toString()));
    } else if (fd.faceted.equals("hierarchy")) {
      if (doc.facets == null) {
        doc.facets = new ArrayList<CategoryPath>();
      }
      if (o instanceof JSONArray) {
        JSONArray arr = (JSONArray) o;
        String[] values = new String[1+arr.size()];
        values[0] = fd.name;
        for(int idx=0;idx<values.length-1;idx++) {
          values[idx+1] = arr.get(idx).toString();
        }
        doc.facets.add(new CategoryPath(values));
      } else if (o instanceof List) { 
        List<String> values = (List<String>) o;
        CategoryPath cp = null;
        try {
          cp = new CategoryPath(values.toArray(new String[values.size()]));
        } catch (IllegalArgumentException iae) {
          fail(fd.name, "unable to create facet path: " + iae.getMessage());
        }
        doc.facets.add(cp);
      } else {
        doc.facets.add(new CategoryPath(fd.name, o.toString()));
      }
    }

    if (fd.highlighted) {
      if (!(o instanceof String)) {
        fail(fd.name, "can only highlight text or atom fields");
      }
      if (!fd.singleValued && (((String) o).indexOf(Constants.INFORMATION_SEP) != -1)) {
        // TODO: we could remove this restriction if it
        // ever matters ... we can highlight multi-valued
        // fields at search time without stealing a
        // character:
        fail(fd.name, "multiValued and hihglighted fields cannot contain INFORMATION_SEPARATOR (U+001F) character: this character is used internally when highlighting multi-valued fields");
      }
    }

    if (o instanceof String &&
        (fd.fieldType.docValueType() == DocValuesType.BINARY ||
         fd.fieldType.docValueType() == DocValuesType.SORTED)) {
      BytesRef br = new BytesRef((String) o);
      if (fd.fieldType.docValueType() == DocValuesType.BINARY) {
        doc.doc.add(new BinaryDocValuesField(fd.name, br));
      } else {
        doc.doc.add(new SortedDocValuesField(fd.name, br));
      }
    }

    if (fd.fieldType.docValueType() == DocValuesType.NUMERIC) {
      if (fd.valueType.equals("float")) {
        //o = new Long(Float.floatToRawIntBits(((Number) o).floatValue()));
        doc.doc.add(new NumericDocValuesField(fd.name, Float.floatToRawIntBits(((Number) o).floatValue())));
      } else if (fd.valueType.equals("double")) {
        //o = new Long(Double.doubleToRawLongBits(((Number) o).floatValue()));
        doc.doc.add(new NumericDocValuesField(fd.name, Double.doubleToRawLongBits(((Number) o).doubleValue())));
      } else if (fd.valueType.equals("int")) {
        //o = new Long(((Integer) o).intValue());
        doc.doc.add(new NumericDocValuesField(fd.name, ((Number) o).intValue()));
      } else if (fd.valueType.equals("long")) {
        doc.doc.add(new NumericDocValuesField(fd.name, ((Number) o).longValue()));
      } else if (fd.valueType.equals("boolean")) {
        if (o == Boolean.TRUE) {
          //o = Long.valueOf(1);
          doc.doc.add(new NumericDocValuesField(fd.name, 1L));
        } else {
          //o = Long.valueOf(0);
          doc.doc.add(new NumericDocValuesField(fd.name, 0L));
        }
      } else if (fd.valueType.equals("boolean")) {
        fail(fd.name, "invalid type " + fd.valueType + " for numeric doc values");
      }
    }

    // nocommit if field is ONLY for faceting don't add to
    // doc ...
    if (fd.fieldType.stored() || fd.fieldType.indexed()) {
      // nocommit this is a mess:
      if (o == Boolean.TRUE) {
        o = "true";
      } else if (o == Boolean.FALSE) {
        o = "false";
      }
      Field f = new MyField(fd.name, fd.fieldTypeNoDV, o);
      f.setBoost(boost);
      doc.doc.add(f);
    } else {
      // nocommit assert / real check here!?
    }
  }

  public interface PostHandle {
    public void invoke(IndexState state, Request r, DocumentAndFacets doc) throws IOException;
    public boolean invoke(IndexState state, String fieldName, JsonParser p, DocumentAndFacets doc) throws IOException;
  }

  final List<PostHandle> postHandlers = new CopyOnWriteArrayList<PostHandle>();

  public void addPostHandle(PostHandle handler) {
    postHandlers.add(handler);
  }

  public static Object fixType(FieldDef fd, String value) {
    Object o;
    if (fd.valueType.equals("int")) {
      o = Integer.valueOf(Integer.parseInt(value));
    } else if (fd.valueType.equals("long")) {
      o = Long.valueOf(Long.parseLong(value));
    } else if (fd.valueType.equals("float")) {
      o = Float.valueOf(Float.parseFloat(value));
    } else if (fd.valueType.equals("double")) {
      o = Double.valueOf(Double.parseDouble(value));
    } else {
      o = value;
    }
    return o;
  }

  static void fail(String fieldName, String message) {
    throw new IllegalArgumentException("field=" + fieldName + ": " + message);
  }

  void parseFields(IndexState state, DocumentAndFacets doc, JsonParser p) throws IOException {
    JsonToken token = p.nextToken();
    if (token != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("fields should be an object");
    }
    while (true) {
      token = p.nextToken();
      if (token == JsonToken.END_OBJECT) {
        break;
      }
      assert token == JsonToken.FIELD_NAME;
      parseOneField(p, state, doc, p.getText());
    }
  }

  /** Parses using Jackson's streaming parser API. */
  DocumentAndFacets parseDocument(IndexState state, JsonParser p) throws IOException {
    //System.out.println("parseDocument: " + r);
    JsonToken token = p.nextToken();
    if (token == JsonToken.END_ARRAY) {
      // nocommit hackish.. caller should tell us this means "end"?
      return null;
    } else if (token != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("expected JSON Object");
    }

    final DocumentAndFacets doc = new DocumentAndFacets();
    while (true) {
      token = p.nextToken();
      if (token == JsonToken.END_OBJECT) {
        break;
      }
      assert token == JsonToken.FIELD_NAME: token;

      String fieldName = p.getText();
      if (fieldName.equals("fields")) {
        parseFields(state, doc, p);
      } else {
        boolean handled = false;
        for(PostHandle postHandle : postHandlers) {
          if (postHandle.invoke(state, fieldName, p, doc)) {
            handled = true;
            break;
          }
        }

        if (!handled) {
          throw new IllegalArgumentException("unrecognized field " + p.getText());
        }
      }
      // nocommit need test that same field name can't
      // appear more than once?
    }

    return doc;
  }

  private static void parseOneField(JsonParser p, IndexState state, DocumentAndFacets doc, String name) throws IOException {

    FieldDef fd = state.getField(name);

    if (!fd.singleValued) {
      JsonToken token = p.nextToken();
      if (token != JsonToken.START_ARRAY) {
        fail(name, "field is multiValued; expected array but got " + token);
      }
      while (true) {
        if (!parseOneValue(fd, p, doc)) {
          break;
        }
      }
    } else {
      parseOneValue(fd, p, doc);
    }
  }

  private static boolean parseOneValue(FieldDef fd, JsonParser p, DocumentAndFacets doc) throws IOException {

    Object o = null;

    JsonToken token = p.nextToken();
    if (token == JsonToken.END_ARRAY) {
      return false;
    }

    float boost = 1.0f;
    if (fd.fieldType.indexed() && token == JsonToken.START_OBJECT) {
      // Parse a {boost: X, value: Y}
      while(true) {
        token = p.nextToken();
        if (token == JsonToken.END_OBJECT) {
          break;
        }
        assert token == JsonToken.FIELD_NAME;
        String fName = p.getText();
        if (fName.equals("boost")) {
          token = p.nextToken();
          if (token == JsonToken.VALUE_NUMBER_INT | token == JsonToken.VALUE_NUMBER_FLOAT) {
            boost = p.getFloatValue();
          } else {
            fail(fd.name, "boost in inner object field value must have float value");
          }
        } else if (fName.equals("value")) {
          token = p.nextToken();
          if (token == JsonToken.VALUE_STRING) {
            o = p.getText();
          } else if (token == JsonToken.VALUE_NUMBER_INT) {
            o = Long.valueOf(p.getLongValue());
          } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
            o = Double.valueOf(p.getDoubleValue());
          } else {
            fail(fd.name, "value in inner object field value should be string or int or float; got " + o);
          }
        } else {
          fail(fd.name, "unrecognized field \"" + fName + "\" in inner object field value; must be boost or value");
        }
      }
      if (o == null) {
        fail(fd.name, "value in inner object field value must have value member");
      }
    } else {
      // Parse a single value:
      if (token == JsonToken.VALUE_STRING) {
        o = p.getText();
      } else if (token == JsonToken.VALUE_NUMBER_INT) {
        o = Long.valueOf(p.getLongValue());
      } else if (token == JsonToken.VALUE_NUMBER_FLOAT) {
        o = Double.valueOf(p.getDoubleValue());
      } else if (token == JsonToken.VALUE_NULL) {
        fail(fd.name, "null field value not supported; just omit this field from the document instead");
      } else if (token == JsonToken.VALUE_TRUE) {
        o = Boolean.TRUE;
      } else if (token == JsonToken.VALUE_FALSE) {
        o = Boolean.FALSE;
      }
    }

    if (fd.faceted.equals("hierarchy") && o == null) {
      if (token == JsonToken.START_ARRAY) {
        List<String> values = new ArrayList<String>();
        values.add(fd.name);
        while (true) {
          token = p.nextToken();
          if (token == JsonToken.END_ARRAY) {
            break;
          } else if (token != JsonToken.VALUE_STRING) {
            if (token == JsonToken.START_ARRAY) {
              fail(fd.name, "expected string but saw array");
            } else {
              fail(fd.name, "expected string but saw " + token);
            }
          }
          values.add(p.getText());
        }
        o = values;
      }          
    }

    parseOneValue(fd, doc, o, boost);
    return true;
  }

  @Override
  public FinishRequest handle(final IndexState state, final Request r, Map<String,List<String>> params) throws Exception {

    if (!state.started()) {
      r.fail("indexName",  "call startIndex first");
    }

    // NOTE: somewhat wasteful since we re-serialize to
    // string only to re-parse the JSON, but this allows
    // single-source (bulk) for parsing, and apps that care
    // about performance will use bulk APIs:

    JSONObject raw = r.getRawParams();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"");
    sb.append(state.name);
    sb.append("\", \"documents\": [");
    sb.append(raw.toString());
    sb.append("]}");
    raw.clear();

    final String bulkRequestString = sb.toString();

    return new FinishRequest() {
      @Override
      public String finish() throws Exception {
        String result = globalState.getHandler("bulkAddDocument").handleStreamed(new StringReader(bulkRequestString), null);
        if (result.indexOf("errors") != -1) {
          JSONObject o = (JSONObject) JSONValue.parseStrict(result);
          if (o.containsKey("errors")) {
            JSONObject err = (JSONObject) ((JSONArray) o.get("errors")).get(0);
            throw new IllegalArgumentException((String) err.get("exception"));
          }
        }
        return result;
      }
    };
  }
}
