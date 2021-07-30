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
package org.apache.solr.handler.loader;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.JsonRecordReader;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.RecordingJSONParser;
import org.noggit.JSONParser;
import org.noggit.JSONParser.ParseException;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;
import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CommonParams.VERSION_FIELD;
import static org.apache.solr.common.params.ShardParams._ROUTE_;


/**
 * @since solr 4.0
 */
public class JsonLoader extends ContentStreamLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();
  public static final String CHILD_DOC_KEY = "_childDocuments_";

  @Override
  public String getDefaultWT() {
    return JSON;
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp,
                   ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    new SingleThreadedJsonLoader(req, rsp, processor).load(req, rsp, stream, processor);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SolrInputDocument buildDoc(Map<String, Object> m) {
    SolrInputDocument result = new SolrInputDocument();
    for (Map.Entry<String, Object> e : m.entrySet()) {
      if (mapEntryIsChildDoc(e.getValue())) { // parse child documents
        if (e.getValue() instanceof List) {
          List value = (List) e.getValue();
          for (Object o : value) {
            if (o instanceof Map) {
              // retain the value as a list, even if the list contains a single value.
              if(!result.containsKey(e.getKey())) {
                result.setField(e.getKey(), new ArrayList<>(1));
              }
              result.addField(e.getKey(), buildDoc((Map) o));
            }
          }
        } else if (e.getValue() instanceof Map) {
          result.addField(e.getKey(), buildDoc((Map) e.getValue()));
        }
      } else {
        result.setField(e.getKey(), e.getValue());
      }
    }
    return result;
  }

  private static boolean mapEntryIsChildDoc(Object val) {
    if(val instanceof List) {
      @SuppressWarnings({"rawtypes"})
      List listVal = (List) val;
      if (listVal.size() == 0) return false;
      return  listVal.get(0) instanceof Map;
    }
    return val instanceof Map;
  }

  static class SingleThreadedJsonLoader extends ContentStreamLoader {

    protected final UpdateRequestProcessor processor;
    protected final SolrQueryRequest req;
    protected SolrQueryResponse rsp;
    protected JSONParser parser;
    protected final int commitWithin;
    protected final boolean overwrite;

    SingleThreadedJsonLoader(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor processor) {
      this.processor = processor;
      this.req = req;
      this.rsp = rsp;

      commitWithin = req.getParams().getInt(UpdateParams.COMMIT_WITHIN, -1);
      overwrite = req.getParams().getBool(UpdateParams.OVERWRITE, true);
    }

    @Override
    public void load(SolrQueryRequest req,
                     SolrQueryResponse rsp,
                     ContentStream stream,
                     UpdateRequestProcessor processor) throws Exception {

      Reader reader = null;
      try {
        reader = stream.getReader();
        if (log.isTraceEnabled()) {
          String body = IOUtils.toString(reader);
          log.trace("body: {}", body);
          reader = new StringReader(body);
        }

        this.processUpdate(reader);
      } catch (ParseException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot parse provided JSON: " + e.getMessage());
      }
      finally {
        IOUtils.closeQuietly(reader);
      }
    }

    @SuppressWarnings("fallthrough")
    void processUpdate(Reader reader) throws IOException {
      String path = (String) req.getContext().get(PATH);
      if (UpdateRequestHandler.DOC_PATH.equals(path) || "false".equals(req.getParams().get("json.command"))) {
        String split = req.getParams().get("split");
        String[] f = req.getParams().getParams("f");
        handleSplitMode(split, f, reader);
        return;
      }
      parser = new JSONParser(reader);
      int ev = parser.nextEvent();
      while (ev != JSONParser.EOF) {

        switch (ev) {
          case JSONParser.ARRAY_START:
            handleAdds();
            break;

          case JSONParser.STRING:
            if (parser.wasKey()) {
              String v = parser.getString();
              if (v.equals(UpdateRequestHandler.ADD)) {
                int ev2 = parser.nextEvent();
                if (ev2 == JSONParser.OBJECT_START) {
                  processor.processAdd(parseAdd());
                } else if (ev2 == JSONParser.ARRAY_START) {
                  handleAdds();
                } else {
                  assertEvent(ev2, JSONParser.OBJECT_START);
                }
              } else if (v.equals(UpdateRequestHandler.COMMIT)) {
                CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);
                cmd.waitSearcher = true;
                parseCommitOptions(cmd);
                processor.processCommit(cmd);
              } else if (v.equals(UpdateRequestHandler.OPTIMIZE)) {
                CommitUpdateCommand cmd = new CommitUpdateCommand(req, true);
                cmd.waitSearcher = true;
                parseCommitOptions(cmd);
                processor.processCommit(cmd);
              } else if (v.equals(UpdateRequestHandler.DELETE)) {
                handleDeleteCommand();
              } else if (v.equals(UpdateRequestHandler.ROLLBACK)) {
                processor.processRollback(parseRollback());
              } else {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown command '" + v + "' at [" + parser.getPosition() + "]");
              }
              break;
            }
            // fall through

          case JSONParser.LONG:
          case JSONParser.NUMBER:
          case JSONParser.BIGNUMBER:
          case JSONParser.BOOLEAN:
          case JSONParser.NULL:
            if (log.isInfoEnabled()) {
              log.info("Can't have a value here. Unexpected {} at [{}]"
                  , JSONParser.getEventString(ev), parser.getPosition());
            }

          case JSONParser.OBJECT_START:
          case JSONParser.OBJECT_END:
          case JSONParser.ARRAY_END:
            break;

          default:
            log.info("Noggit UNKNOWN_EVENT_ID: {}", ev);
            break;
        }
        // read the next event
        ev = parser.nextEvent();
      }
    }

    private void handleSplitMode(String split, String[] fields, final Reader reader) throws IOException {
      if (split == null) split = "/";
      if (fields == null || fields.length == 0) fields = new String[]{"$FQN:/**"};
      final boolean echo = "true".equals(req.getParams().get("echo"));
      final String srcField = req.getParams().get("srcField");
      final boolean mapUniqueKeyOnly = req.getParams().getBool("mapUniqueKeyOnly", false);
      if (srcField != null) {
        if (!"/".equals(split))
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Raw data can be stored only if split=/");
        parser = new RecordingJSONParser(reader);
      } else {
        parser = new JSONParser(reader);

      }

      JsonRecordReader jsonRecordReader = JsonRecordReader.getInst(split, Arrays.asList(fields));
      jsonRecordReader.streamRecords(parser, new JsonRecordReader.Handler() {
        ArrayList<Map<String, Object>> docs = null;

        @Override
        public void handle(Map<String, Object> record, String path) {
          Map<String, Object> copy = getDocMap(record, parser, srcField, mapUniqueKeyOnly);

          if (echo) {
            if (docs == null) {
              docs = new ArrayList<>();
              rsp.add("docs", docs);
            }
            changeChildDoc(copy);
            docs.add(copy);
          } else {
            AddUpdateCommand cmd = new AddUpdateCommand(req);
            cmd.commitWithin = commitWithin;
            cmd.overwrite = overwrite;
            cmd.solrDoc = buildDoc(copy);
            try {
              processor.processAdd(cmd);
            } catch (IOException e) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error inserting document: ", e);
            }
          }
        }
      });
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Map<String, Object> getDocMap(Map<String, Object> record, JSONParser parser, String srcField, boolean mapUniqueKeyOnly) {
      Map result = record;
      if (srcField != null && parser instanceof RecordingJSONParser) {
        //if srcFIeld specified extract it out first
        result = new LinkedHashMap(record);
        RecordingJSONParser rjp = (RecordingJSONParser) parser;
        result.put(srcField, rjp.getBuf());
        rjp.resetBuf();
      }
      if (mapUniqueKeyOnly) {
        SchemaField sf = req.getSchema().getUniqueKeyField();
        if (sf == null)
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No uniqueKey specified in schema");
        String df = req.getParams().get(CommonParams.DF);
        if (df == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No 'df' specified in request");
        Map copy = new LinkedHashMap();
        String uniqueField = (String) record.get(sf.getName());
        if (uniqueField == null) uniqueField = UUID.randomUUID().toString().toLowerCase(Locale.ROOT);
        copy.put(sf.getName(), uniqueField);
        if (srcField != null && result.containsKey(srcField)) {
          copy.put(srcField, result.remove(srcField));
        }
        copy.put(df, result.values());
        result = copy;
      }


      return result;
    }


    //
    // "delete":"id"
    // "delete":["id1","id2"]
    // "delete":{"id":"foo"}
    // "delete":{"query":"myquery"}
    //
    void handleDeleteCommand() throws IOException {
      int ev = parser.nextEvent();
      switch (ev) {
        case JSONParser.ARRAY_START:
          handleDeleteArray(ev);
          break;
        case JSONParser.OBJECT_START:
          handleDeleteMap(ev);
          break;
        default:
          handleSingleDelete(ev);
      }
    }

    // returns the string value for a primitive value, or null for the null value
    String getString(int ev) throws IOException {
      switch (ev) {
        case JSONParser.STRING:
          return parser.getString();
        case JSONParser.BIGNUMBER:
        case JSONParser.NUMBER:
        case JSONParser.LONG:
          return parser.getNumberChars().toString();
        case JSONParser.BOOLEAN:
          return Boolean.toString(parser.getBoolean());
        case JSONParser.NULL:
          return null;
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Expected primitive JSON value but got: " + JSONParser.getEventString(ev)
                  + " at [" + parser.getPosition() + "]");
      }
    }


    void handleSingleDelete(int ev) throws IOException {
      if (ev == JSONParser.OBJECT_START) {
        handleDeleteMap(ev);
      } else {
        DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
        cmd.commitWithin = commitWithin;
        String id = getString(ev);
        cmd.setId(id);
        processor.processDelete(cmd);
      }
    }

    void handleDeleteArray(int ev) throws IOException {
      assert ev == JSONParser.ARRAY_START;
      for (; ; ) {
        ev = parser.nextEvent();
        if (ev == JSONParser.ARRAY_END) return;
        handleSingleDelete(ev);
      }
    }

    void handleDeleteMap(int ev) throws IOException {
      assert ev == JSONParser.OBJECT_START;

      DeleteUpdateCommand cmd = new DeleteUpdateCommand(req);
      cmd.commitWithin = commitWithin;

      while (true) {
        ev = parser.nextEvent();
        if (ev == JSONParser.STRING) {
          String key = parser.getString();
          if (parser.wasKey()) {
            if (ID.equals(key)) {
              cmd.setId(getString(parser.nextEvent()));
            } else if ("query".equals(key)) {
              cmd.setQuery(parser.getString());
            } else if ("commitWithin".equals(key)) {
              cmd.commitWithin = (int) parser.getLong();
            } else if (VERSION_FIELD.equals(key)) {
              cmd.setVersion(parser.getLong());
            } else if (_ROUTE_.equals(key)) {
              cmd.setRoute(parser.getString());
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown key '" + key + "' at [" + parser.getPosition() + "]");
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "invalid string: " + key
                    + " at [" + parser.getPosition() + "]");
          }
        } else if (ev == JSONParser.OBJECT_END) {
          if (cmd.getId() == null && cmd.getQuery() == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing id or query for delete at [" + parser.getPosition() + "]");
          }

          processor.processDelete(cmd);
          return;
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Got: " + JSONParser.getEventString(ev)
                  + " at [" + parser.getPosition() + "]");
        }
      }
    }


    RollbackUpdateCommand parseRollback() throws IOException {
      assertNextEvent(JSONParser.OBJECT_START);
      assertNextEvent(JSONParser.OBJECT_END);
      return new RollbackUpdateCommand(req);
    }

    void parseCommitOptions(CommitUpdateCommand cmd) throws IOException {
      assertNextEvent(JSONParser.OBJECT_START);
      @SuppressWarnings({"unchecked"})
      final Map<String, Object> map = (Map) ObjectBuilder.getVal(parser);

      // SolrParams currently expects string values...
      SolrParams p = new SolrParams() {
        @Override
        public String get(String param) {
          Object o = map.get(param);
          return o == null ? null : o.toString();
        }

        @Override
        public String[] getParams(String param) {
          return new String[]{get(param)};
        }

        @Override
        public Iterator<String> getParameterNamesIterator() {
          return map.keySet().iterator();
        }
      };

      RequestHandlerUtils.validateCommitParams(p);
      p = SolrParams.wrapDefaults(p, req.getParams());   // default to the normal request params for commit options
      RequestHandlerUtils.updateCommit(cmd, p);
    }

    AddUpdateCommand parseAdd() throws IOException {
      AddUpdateCommand cmd = new AddUpdateCommand(req);
      cmd.commitWithin = commitWithin;
      cmd.overwrite = overwrite;

      while (true) {
        int ev = parser.nextEvent();
        if (ev == JSONParser.STRING) {
          if (parser.wasKey()) {
            String key = parser.getString();
            if ("doc".equals(key)) {
              if (cmd.solrDoc != null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Multiple documents in same"
                    + " add command at [" + parser.getPosition() + "]");
              }
              ev = assertNextEvent(JSONParser.OBJECT_START);
              cmd.solrDoc = parseDoc(ev);
            } else if (UpdateRequestHandler.OVERWRITE.equals(key)) {
              cmd.overwrite = parser.getBoolean(); // reads next boolean
            } else if (UpdateRequestHandler.COMMIT_WITHIN.equals(key)) {
              cmd.commitWithin = (int) parser.getLong();
            } else if ("boost".equals(key)) {
              String boost = parser.getNumberChars().toString();
              String message = "Ignoring document boost: " + boost + " as index-time boosts are not supported anymore";
              if (WARNED_ABOUT_INDEX_TIME_BOOSTS.compareAndSet(false, true)) {
                log.warn(message);
              } else {
                log.debug(message);
              }
            } else {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown key '" + key + "' at [" + parser.getPosition() + "]");
            }
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Should be a key "
                    + " at [" + parser.getPosition() + "]");
          }
        } else if (ev == JSONParser.OBJECT_END) {
          if (cmd.solrDoc == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing solr document at [" + parser.getPosition() + "]");
          }
          return cmd;
        } else {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Got: " + JSONParser.getEventString(ev)
                  + " at [" + parser.getPosition() + "]");
        }
      }
    }


    void handleAdds() throws IOException {
      while (true) {
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.commitWithin = commitWithin;
        cmd.overwrite = overwrite;

        int ev = parser.nextEvent();
        if (ev == JSONParser.ARRAY_END) break;

        assertEvent(ev, JSONParser.OBJECT_START);
        cmd.solrDoc = parseDoc(ev);
        processor.processAdd(cmd);
      }
    }


    int assertNextEvent(int expected) throws IOException {
      int got = parser.nextEvent();
      assertEvent(got, expected);
      return got;
    }

    void assertEvent(int ev, int expected) {
      if (ev != expected) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Expected: " + JSONParser.getEventString(expected)
                + " but got " + JSONParser.getEventString(ev)
                + " at [" + parser.getPosition() + "]");
      }
    }


    private SolrInputDocument parseDoc(int ev) throws IOException {
      assert ev == JSONParser.OBJECT_START;

      SolrInputDocument sdoc = new SolrInputDocument();
      for (; ; ) {
        ev = parser.nextEvent();
        if (ev == JSONParser.OBJECT_END) {
          return sdoc;
        }
        String fieldName = parser.getString();

        if (fieldName.equals(JsonLoader.CHILD_DOC_KEY)) { // somewhat legacy
          ev = parser.nextEvent();
          assertEvent(ev, JSONParser.ARRAY_START);
          while ((ev = parser.nextEvent()) != JSONParser.ARRAY_END) {
            assertEvent(ev, JSONParser.OBJECT_START);

            sdoc.addChildDocument(parseDoc(ev));
          }
        } else {
          ev = parser.nextEvent();
          Object val = parseFieldValue(ev, fieldName);
          // SolrInputDocument.addField will do the right thing
          // if the doc already has another value for this field
          // (ie: repeating fieldname keys)
          sdoc.addField(fieldName, val);
        }

      }
    }

    private Object parseFieldValue(int ev, String fieldName) throws IOException {
      switch (ev) {
        case JSONParser.STRING:
          return parser.getString();
        case JSONParser.LONG:
          return parser.getLong();
        case JSONParser.NUMBER:
          return parser.getDouble();
        case JSONParser.BIGNUMBER:
          return parser.getNumberChars().toString();
        case JSONParser.BOOLEAN:
          return parser.getBoolean();
        case JSONParser.NULL:
          parser.getNull();
          return null;
        case JSONParser.ARRAY_START:
          return parseArrayFieldValue(ev, fieldName);
        case JSONParser.OBJECT_START:
          return parseObjectFieldValue(ev, fieldName);
        default:
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error parsing JSON field value. "
              + "Unexpected " + JSONParser.getEventString(ev) + " at [" + parser.getPosition() + "], field=" + fieldName);
      }
    }

    @SuppressWarnings({"unchecked"})
    private List<Object> parseArrayFieldValue(int ev, String fieldName) throws IOException {
      assert ev == JSONParser.ARRAY_START;

      @SuppressWarnings({"rawtypes"})
      ArrayList lst = new ArrayList(2);
      for (; ; ) {
        ev = parser.nextEvent();
        if (ev == JSONParser.ARRAY_END) {
          return lst;
        }
        lst.add(parseFieldValue(ev, fieldName));
      }
    }

    /**
     * Parses this object as either a map for atomic update, or a child document.
     */
    private Object parseObjectFieldValue(int ev, String fieldName) throws IOException {
      assert ev == JSONParser.OBJECT_START;

      SolrInputDocument extendedSolrDocument = parseDoc(ev);
      // is this a partial update or a child doc?
      if (isChildDoc(extendedSolrDocument)) {
        return extendedSolrDocument;
      } else {
        //return extendedSolrDocument.toMap(new HashMap<>(extendedSolrDocument.size()));  not quite right
        Map<String, Object> map = new HashMap<>(extendedSolrDocument.size());
        for (SolrInputField inputField : extendedSolrDocument) {
          map.put(inputField.getName(), inputField.getValue());
        }
        return map;
      }
    }

    private boolean isChildDoc(SolrInputDocument extendedFieldValue) {
      return extendedFieldValue.containsKey(req.getSchema().getUniqueKeyField().getName());
    }

  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object changeChildDoc(Object o) {
    if (o instanceof List) {
      return ((List) o)
          .stream()
          .map(JsonLoader::changeChildDoc)
          .collect(toList());
    }
    Map m = (Map) o;
    if (m.containsKey(null)) m.put(CHILD_DOC_KEY, changeChildDoc(m.remove(null)));
    return m;
  }

}
