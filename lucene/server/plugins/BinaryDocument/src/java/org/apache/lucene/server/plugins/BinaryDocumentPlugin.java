package org.apache.lucene.server.plugins;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.server.FieldDef;
import org.apache.lucene.server.GlobalState;
import org.apache.lucene.server.IndexState.DocumentAndFacets;
import org.apache.lucene.server.IndexState;
import org.apache.lucene.server.handlers.AddDocumentHandler;
import org.apache.lucene.server.handlers.BulkUpdateDocumentHandler;
import org.apache.lucene.server.handlers.CrackDocumentHandler;
import org.apache.lucene.server.params.IntType;
import org.apache.lucene.server.params.Param;
import org.apache.lucene.server.params.Request;
import org.apache.lucene.server.params.StringType;
import org.apache.lucene.server.params.StructType;
import org.apache.lucene.server.plugins.Plugin;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.sax.BodyContentHandler;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

// TODO
//   - allow pre-registering the mappings in settings/fields

public class BinaryDocumentPlugin extends Plugin {

  @Override
  public String getTopDoc() {
    return "Parses non-plain-text document formats, such as Adobe PDF, Microsoft Office, HTML, into separate fields with plain-text values, using Apache Tika.  This plugin modifies the indexing methods to accept an optional per-document binary value, and adds a new @crackDocument method.";
  }

  @Override
  public String getName() {
    return "BinaryDocument";
  }

  public static final Parser tikaAutoDetectParser = new AutoDetectParser();

  private static class AddDocumentPostHandler implements AddDocumentHandler.PostHandle {

    @Override
    public void invoke(IndexState state, Request r, DocumentAndFacets doc) throws IOException {
      if (r.hasParam("binary")) {
        // Parse binary document using Tika:
        Request r2 = r.getStruct("binary");
        byte[] decodedBytes = Base64.decodeBase64(r2.getString("content"));
        ParseContext parseContext = new ParseContext();
        parseContext.set(Parser.class, tikaAutoDetectParser);
        Metadata md = new Metadata();
        if (r2.hasParam("fileName")) {
          md.add(Metadata.RESOURCE_NAME_KEY, r2.getString("fileName"));
        }
        if (r2.hasParam("contentType")) {
          md.add(Metadata.CONTENT_TYPE, r2.getString("contentType"));
        }
        if (r2.hasParam("password")) {
          final String password = r2.getString("password");
          parseContext.set(PasswordProvider.class, new PasswordProvider() {
            @Override
            public String getPassword(Metadata metadata) {
              return password;
            }
          });
        }

        // nocommit this discards non-body text!
        ContentHandler handler = new BodyContentHandler(r2.getInt("maxLength"));
        try {
          tikaAutoDetectParser.parse(TikaInputStream.get(decodedBytes),
                                     handler, md, parseContext);
        } catch (SAXException se) {
          throw new IllegalArgumentException("exception while parsing document", se);
        } catch (TikaException te) {
          throw new IllegalArgumentException("exception while parsing document", te);
        }

        Request mappings = r2.getStruct("mappings");
        Iterator<Map.Entry<String,Object>> it = mappings.getParams();
        while (it.hasNext()) {
          Map.Entry<String,Object> ent = it.next();
          if (!(ent.getValue() instanceof String)) {
            r2.fail(ent.getKey(), "value must be a string; got " + ent.getValue().getClass());
          }
          String fieldName = (String) ent.getValue();
          String value;
          if (ent.getKey().equals("body")) {
            value = handler.toString();
          } else {
            // nocommit what about multi-valued?
            value = md.get(ent.getKey());
          }
          if (value != null) {
            FieldDef fd = state.getField(fieldName);
            // TODO: how to pass boost?
            AddDocumentHandler.parseOneValue(fd, doc, AddDocumentHandler.fixType(fd, value), 1.0f);
          }
        }
        r2.clearParams();
      }
    }

    static void parseBinary(IndexState state, DocumentAndFacets doc, JsonParser p) throws IOException {
      JsonToken token = p.nextToken();
      if (token != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("binary should be an object");
      }
      Metadata md = new Metadata();
      Map<String,String> mappings = new HashMap<String,String>();
      ParseContext parseContext = new ParseContext();
      byte[] decodedBytes = null;
      int maxLength = -1;
      while (true) {
        token = p.nextToken();
        if (token == JsonToken.END_OBJECT) {
          break;
        }
        assert token == JsonToken.FIELD_NAME: token;
        String fieldName = p.getText();
        if (fieldName.equals("content")) {
          token = p.nextToken();
          if (token != JsonToken.VALUE_STRING) {
            throw new IllegalArgumentException("binary > contents should be a Base64 encoded string");
          }
          decodedBytes = Base64.decodeBase64(p.getText());
        } else if (fieldName.equals("fileName")) {
          token = p.nextToken();
          if (token != JsonToken.VALUE_STRING) {
            throw new IllegalArgumentException("binary > fileName should be a string");
          }
          md.add(Metadata.RESOURCE_NAME_KEY, p.getText());
        } else if (fieldName.equals("contentType")) {
          token = p.nextToken();
          if (token != JsonToken.VALUE_STRING) {
            throw new IllegalArgumentException("binary > contentType should be a string");
          }
          md.add(Metadata.CONTENT_TYPE, p.getText());
        } else if (fieldName.equals("password")) {
          token = p.nextToken();
          if (token != JsonToken.VALUE_STRING) {
            throw new IllegalArgumentException("binary > password should be a string");
          }
          final String password = p.getText();
          parseContext.set(PasswordProvider.class, new PasswordProvider() {
            @Override
            public String getPassword(Metadata metadata) {
              return password;
            }
          });
        } else if (fieldName.equals("maxLength")) {
          if (token != JsonToken.VALUE_NUMBER_INT) {
            throw new IllegalArgumentException("binary > maxLength should be an int");
          }
          maxLength = p.getIntValue();
        } else if (fieldName.equals("mappings")) {
          token = p.nextToken();
          if (token != JsonToken.START_OBJECT) {
            throw new IllegalArgumentException("binary > mappings should be an object");
          }
          while (true) {
            token = p.nextToken();
            if (token == JsonToken.END_OBJECT) {
              break;
            }
            assert token == JsonToken.FIELD_NAME;
            fieldName = p.getText();
            token = p.nextToken();
            if (token != JsonToken.VALUE_STRING) {
              throw new IllegalArgumentException("binary > mappings > " + fieldName + " should be a string");
            }
            mappings.put(fieldName, p.getText());
          }
        } else {
          throw new IllegalArgumentException("binary: unrecognized argument " + fieldName);
        }
      }

      if (decodedBytes == null) {
        throw new IllegalArgumentException("binary > content is missing");
      }

      ContentHandler handler = new BodyContentHandler(maxLength);
      try {
        tikaAutoDetectParser.parse(TikaInputStream.get(decodedBytes),
                                   handler, md, parseContext);
      } catch (SAXException se) {
        throw new IllegalArgumentException("exception while parsing document", se);
      } catch (TikaException te) {
        throw new IllegalArgumentException("exception while parsing document", te);
      }

      // Used only for errors:
      Request r = new Request(null, "binary", null, null);

      for(Map.Entry<String,String> ent : mappings.entrySet()) {
        String fieldName = ent.getValue();
        String value;
        if (ent.getKey().equals("body")) {
          value = handler.toString();
        } else {
          // nocommit what about multi-valued?
          value = md.get(ent.getKey());
        }
        if (value != null) {
          FieldDef fd = state.getField(fieldName);
          AddDocumentHandler.parseOneValue(fd, doc, AddDocumentHandler.fixType(fd, value), 1.0f);
        }
      }
    }

    @Override
    public boolean invoke(IndexState state, String fieldName, JsonParser p, DocumentAndFacets doc) throws IOException {
      if (fieldName.equals("binary")) {
        parseBinary(state, doc, p);
        return true;
      } else {
        return false;
      }
    }
  }

  public BinaryDocumentPlugin(GlobalState state) {

    state.addHandler("crackDocument", new CrackDocumentHandler(state));

    // Register our pre-processor in addDocument:
    AddDocumentHandler addDocHandler = (AddDocumentHandler) state.getHandler("addDocument");
    addDocHandler.addPostHandle(new AddDocumentPostHandler());
    
    Param binParam = new Param("binary", "Binary document to extract text and metadata fields from.  To see all metadata available for a given document without indexing it, use @crackDocument.",
                         new StructType(
                             new Param("content", "Base64 encoded binary content", new StringType()),
                             new Param("contentType", "Mime type, if known (for example, application/pdf); otherwise the mime type will be autodetected", new StringType()),
                             new Param("fileName", "File name, if known; otherwise the mime type will be autodetected", new StringType()),
                             new Param("password", "Password, if necessary", new StringType()),
                             new Param("maxLength", "Maximum length in bytes of extracted text, or -1 to disable the limit", new IntType(), 1024*1024),
                             new Param("mappings", "How to map the document body and metadata to registered fields; this is an object whose keys are 'body' or Dublin Core metadata keys, and whose values are the registered field names to map to.", new StructType())));

    // nocommit messy: this is a static ... if we are
    // installed more than once then we double-add!
    AddDocumentHandler.DOCUMENT_TYPE.addParam(binParam);
    state.getHandler("addDocument").getType().addParam(binParam);
    state.getHandler("updateDocument").getType().addParam(binParam);

    // nocommit messy: this is a static ... if we are
    // installed more than once then we double-add!
    BulkUpdateDocumentHandler.UPDATE_DOCUMENT_TYPE.addParam(binParam);
  }
}
