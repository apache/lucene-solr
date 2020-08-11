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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import org.apache.solr.core.SolrCore;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.common.util.XMLErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.io.IOUtils;

import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p> An implementation of {@link EntityProcessor} which uses a streaming xpath parser to extract values out of XML documents.
 * It is typically used in conjunction with {@link URLDataSource} or {@link FileDataSource}. </p> <p> Refer to <a
 * href="http://wiki.apache.org/solr/DataImportHandler">http://wiki.apache.org/solr/DataImportHandler</a> for more
 * details. </p>
 * <p>
 * <b>This API is experimental and may change in the future.</b>
 *
 *
 * @see XPathRecordReader
 * @since solr 1.3
 */
public class XPathEntityProcessor extends EntityProcessorBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final XMLErrorLogger xmllog = new XMLErrorLogger(log);

  private static final Map<String, Object> END_MARKER = new HashMap<>();
  
  protected List<String> placeHolderVariables;

  protected List<String> commonFields;

  private String pk;

  private XPathRecordReader xpathReader;

  protected DataSource<Reader> dataSource;

  protected javax.xml.transform.Transformer xslTransformer;

  protected boolean useSolrAddXml = false;

  protected boolean streamRows = false;

  // Amount of time to block reading/writing to queue when streaming
  protected int blockingQueueTimeOut = 10;
  
  // Units for pumpTimeOut
  protected TimeUnit blockingQueueTimeOutUnits = TimeUnit.SECONDS;
  
  // Number of rows to queue for asynchronous processing
  protected int blockingQueueSize = 1000;

  protected Thread publisherThread;

  protected boolean reinitXPathReader = true;
  
  @Override
  @SuppressWarnings("unchecked")
  public void init(Context context) {
    super.init(context);
    if (reinitXPathReader)
      initXpathReader(context.getVariableResolver());
    pk = context.getEntityAttribute("pk");
    dataSource = context.getDataSource();
    rowIterator = null;

  }

  private void initXpathReader(VariableResolver resolver) {
    reinitXPathReader = false;
    useSolrAddXml = Boolean.parseBoolean(context
            .getEntityAttribute(USE_SOLR_ADD_SCHEMA));
    streamRows = Boolean.parseBoolean(context
            .getEntityAttribute(STREAM));
    if (context.getResolvedEntityAttribute("batchSize") != null) {
      blockingQueueSize = Integer.parseInt(context.getEntityAttribute("batchSize"));
    }
    if (context.getResolvedEntityAttribute("readTimeOut") != null) {
      blockingQueueTimeOut = Integer.parseInt(context.getEntityAttribute("readTimeOut"));
    }
    String xslt = context.getEntityAttribute(XSL);
    if (xslt != null) {
      xslt = context.replaceTokens(xslt);
      try {
        // create an instance of TransformerFactory
        TransformerFactory transFact = TransformerFactory.newInstance();
        final SolrCore core = context.getSolrCore();
        final StreamSource xsltSource;
        if (core != null) {
          final ResourceLoader loader = core.getResourceLoader();
          transFact.setURIResolver(new SystemIdResolver(loader).asURIResolver());
          xsltSource = new StreamSource(loader.openResource(xslt),
            SystemIdResolver.createSystemIdFromResourceName(xslt));
        } else {
          // fallback for tests
          xsltSource = new StreamSource(xslt);
        }
        transFact.setErrorListener(xmllog);
        try {
          xslTransformer = transFact.newTransformer(xsltSource);
        } finally {
          // some XML parsers are broken and don't close the byte stream (but they should according to spec)
          IOUtils.closeQuietly(xsltSource.getInputStream());
        }
        if (log.isInfoEnabled()) {
          log.info("Using xslTransformer: {}", xslTransformer.getClass().getName());
        }
      } catch (Exception e) {
        throw new DataImportHandlerException(SEVERE,
                "Error initializing XSL ", e);
      }
    }

    if (useSolrAddXml) {
      // Support solr add documents
      xpathReader = new XPathRecordReader("/add/doc");
      xpathReader.addField("name", "/add/doc/field/@name", true);
      xpathReader.addField("value", "/add/doc/field", true);
    } else {
      String forEachXpath = context.getResolvedEntityAttribute(FOR_EACH);
      if (forEachXpath == null)
        throw new DataImportHandlerException(SEVERE,
                "Entity : " + context.getEntityAttribute("name")
                        + " must have a 'forEach' attribute");
      if (forEachXpath.equals(context.getEntityAttribute(FOR_EACH))) reinitXPathReader = true;

      try {
        xpathReader = new XPathRecordReader(forEachXpath);
        for (Map<String, String> field : context.getAllEntityFields()) {
          if (field.get(XPATH) == null)
            continue;
          int flags = 0;
          if ("true".equals(field.get("flatten"))) {
            flags = XPathRecordReader.FLATTEN;
          }
          String xpath = field.get(XPATH);
          xpath = context.replaceTokens(xpath);
          //!xpath.equals(field.get(XPATH) means the field xpath has a template
          //in that case ensure that the XPathRecordReader is reinitialized
          //for each xml
          if (!xpath.equals(field.get(XPATH)) && !context.isRootEntity()) reinitXPathReader = true;
          xpathReader.addField(field.get(DataImporter.COLUMN),
                  xpath,
                  Boolean.parseBoolean(field.get(DataImporter.MULTI_VALUED)),
                  flags);
        }
      } catch (RuntimeException e) {
        throw new DataImportHandlerException(SEVERE,
                "Exception while reading xpaths for fields", e);
      }
    }
    String url = context.getEntityAttribute(URL);
    List<String> l = url == null ? Collections.emptyList() : resolver.getVariables(url);
    for (String s : l) {
      if (s.startsWith(entityName + ".")) {
        if (placeHolderVariables == null)
          placeHolderVariables = new ArrayList<>();
        placeHolderVariables.add(s.substring(entityName.length() + 1));
      }
    }
    for (Map<String, String> fld : context.getAllEntityFields()) {
      if (fld.get(COMMON_FIELD) != null && "true".equals(fld.get(COMMON_FIELD))) {
        if (commonFields == null)
          commonFields = new ArrayList<>();
        commonFields.add(fld.get(DataImporter.COLUMN));
      }
    }

  }

  @Override
  public Map<String, Object> nextRow() {
    Map<String, Object> result;

    if (!context.isRootEntity())
      return fetchNextRow();

    while (true) {
      result = fetchNextRow();

      if (result == null)
        return null;

      if (pk == null || result.get(pk) != null)
        return result;
    }
  }

  @Override
  public void postTransform(Map<String, Object> r) {
    readUsefulVars(r);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> fetchNextRow() {
    Map<String, Object> r = null;
    while (true) {
      if (rowIterator == null)
        initQuery(context.replaceTokens(context.getEntityAttribute(URL)));
      r = getNext();
      if (r == null) {
        Object hasMore = context.getSessionAttribute(HAS_MORE, Context.SCOPE_ENTITY);
        try {
          if ("true".equals(hasMore) || Boolean.TRUE.equals(hasMore)) {
            String url = (String) context.getSessionAttribute(NEXT_URL, Context.SCOPE_ENTITY);
            if (url == null)
              url = context.getEntityAttribute(URL);
            addNamespace();
            initQuery(context.replaceTokens(url));
            r = getNext();
            if (r == null)
              return null;
          } else {
            return null;
          }
        } finally {
          context.setSessionAttribute(HAS_MORE,null,Context.SCOPE_ENTITY);
          context.setSessionAttribute(NEXT_URL,null,Context.SCOPE_ENTITY);
        }
      }
      addCommonFields(r);
      return r;
    }
  }

  private void addNamespace() {
    Map<String, Object> namespace = new HashMap<>();
    Set<String> allNames = new HashSet<>();
    if (commonFields != null) allNames.addAll(commonFields);
    if (placeHolderVariables != null) allNames.addAll(placeHolderVariables);
    if(allNames.isEmpty()) return;

    for (String name : allNames) {
      Object val = context.getSessionAttribute(name, Context.SCOPE_ENTITY);
      if (val != null) namespace.put(name, val);
    }
    context.getVariableResolver().addNamespace(entityName, namespace);
  }

  private void addCommonFields(Map<String, Object> r) {
    if(commonFields != null){
      for (String commonField : commonFields) {
        if(r.get(commonField) == null) {
          Object val = context.getSessionAttribute(commonField, Context.SCOPE_ENTITY);
          if(val != null) r.put(commonField, val);
        }

      }
    }

  }

  @SuppressWarnings({"unchecked"})
  private void initQuery(String s) {
    Reader data = null;
    try {
      final List<Map<String, Object>> rows = new ArrayList<>();
      try {
        data = dataSource.getData(s);
      } catch (Exception e) {
        if (ABORT.equals(onError)) {
          wrapAndThrow(SEVERE, e);
        } else if (SKIP.equals(onError)) {
          if (log.isDebugEnabled()) {
            log.debug("Skipping url : {}", s, e);
          }
          wrapAndThrow(DataImportHandlerException.SKIP, e);
        } else {
          log.warn("Failed for url : {}", s, e);
          rowIterator = Collections.EMPTY_LIST.iterator();
          return;
        }
      }
      if (xslTransformer != null) {
        try {
          SimpleCharArrayReader caw = new SimpleCharArrayReader();
          xslTransformer.transform(new StreamSource(data),
                  new StreamResult(caw));
          data = caw.getReader();
        } catch (TransformerException e) {
          if (ABORT.equals(onError)) {
            wrapAndThrow(SEVERE, e, "Exception in applying XSL Transformation");
          } else if (SKIP.equals(onError)) {
            wrapAndThrow(DataImportHandlerException.SKIP, e);
          } else {
            log.warn("Failed for url : {}", s, e);
            rowIterator = Collections.EMPTY_LIST.iterator();
            return;
          }
        }
      }
      if (streamRows) {
        rowIterator = getRowIterator(data, s);
      } else {
        try {
          xpathReader.streamRecords(data, (record, xpath) -> rows.add(readRow(record, xpath)));
        } catch (Exception e) {
          String msg = "Parsing failed for xml, url:" + s + " rows processed:" + rows.size();
          if (rows.size() > 0) msg += " last row: " + rows.get(rows.size() - 1);
          if (ABORT.equals(onError)) {
            wrapAndThrow(SEVERE, e, msg);
          } else if (SKIP.equals(onError)) {
            log.warn(msg, e);
            Map<String, Object> map = new HashMap<>();
            map.put(DocBuilder.SKIP_DOC, Boolean.TRUE);
            rows.add(map);
          } else if (CONTINUE.equals(onError)) {
            log.warn(msg, e);
          }
        }
        rowIterator = rows.iterator();
      }
    } finally {
      if (!streamRows) {
        closeIt(data);
      }

    }
  }

  private void closeIt(Reader data) {
    try {
      data.close();
    } catch (Exception e) { /* Ignore */
    }
  }

  @SuppressWarnings({"unchecked"})
  protected Map<String, Object> readRow(Map<String, Object> record, String xpath) {
    if (useSolrAddXml) {
      List<String> names = (List<String>) record.get("name");
      List<String> values = (List<String>) record.get("value");
      Map<String, Object> row = new HashMap<>();
      for (int i = 0; i < names.size() && i < values.size(); i++) {
        if (row.containsKey(names.get(i))) {
          Object existing = row.get(names.get(i));
          if (existing instanceof List) {
            @SuppressWarnings({"rawtypes"})
            List list = (List) existing;
            list.add(values.get(i));
          } else {
            @SuppressWarnings({"rawtypes"})
            List list = new ArrayList();
            list.add(existing);
            list.add(values.get(i));
            row.put(names.get(i), list);
          }
        } else {
          row.put(names.get(i), values.get(i));
        }
      }
      return row;
    } else {
      record.put(XPATH_FIELD_NAME, xpath);
      return record;
    }
  }


  private static class SimpleCharArrayReader extends CharArrayWriter {
    public Reader getReader() {
      return new CharArrayReader(super.buf, 0, super.count);
    }

  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> readUsefulVars(Map<String, Object> r) {
    Object val = r.get(HAS_MORE);
    if (val != null)
      context.setSessionAttribute(HAS_MORE, val,Context.SCOPE_ENTITY);
    val = r.get(NEXT_URL);
    if (val != null)
      context.setSessionAttribute(NEXT_URL, val,Context.SCOPE_ENTITY);
    if (placeHolderVariables != null) {
      for (String s : placeHolderVariables) {
        val = r.get(s);
        context.setSessionAttribute(s, val,Context.SCOPE_ENTITY);
      }
    }
    if (commonFields != null) {
      for (String s : commonFields) {
        Object commonVal = r.get(s);
        if (commonVal != null) {
          context.setSessionAttribute(s, commonVal,Context.SCOPE_ENTITY);
        }
      }
    }
    return r;

  }

  private Iterator<Map<String, Object>> getRowIterator(final Reader data, final String s) {
    //nothing atomic about it. I just needed a StongReference
    final AtomicReference<Exception> exp = new AtomicReference<>();
    final BlockingQueue<Map<String, Object>> blockingQueue = new ArrayBlockingQueue<>(blockingQueueSize);
    final AtomicBoolean isEnd = new AtomicBoolean(false);
    final AtomicBoolean throwExp = new AtomicBoolean(true);
    publisherThread = new Thread() {
      @Override
      public void run() {
        try {
          xpathReader.streamRecords(data, (record, xpath) -> {
            if (isEnd.get()) {
              throwExp.set(false);
              //To end the streaming . otherwise the parsing will go on forever
              //though consumer has gone away
              throw new RuntimeException("BREAK");
            }
            Map<String, Object> row;
            try {
              row = readRow(record, xpath);
            } catch (Exception e) {
              isEnd.set(true);
              return;
            }
            offer(row);
          });
        } catch (Exception e) {
          if(throwExp.get()) exp.set(e);
        } finally {
          closeIt(data);
          if (!isEnd.get()) {
            offer(END_MARKER);
          }
        }
      }
      
      private void offer(Map<String, Object> row) {
        try {
          while (!blockingQueue.offer(row, blockingQueueTimeOut, blockingQueueTimeOutUnits)) {
            if (isEnd.get()) return;
            log.debug("Timeout elapsed writing records.  Perhaps buffer size should be increased.");
          }
        } catch (InterruptedException e) {
          return;
        } finally {
          synchronized (this) {
            notifyAll();
          }
        }
      }
    };
    
    publisherThread.start();

    return new Iterator<Map<String, Object>>() {
      private Map<String, Object> lastRow;
      int count = 0;

      @Override
      public boolean hasNext() {
        return !isEnd.get();
      }

      @Override
      public Map<String, Object> next() {
        Map<String, Object> row;
        
        do {
          try {
            row = blockingQueue.poll(blockingQueueTimeOut, blockingQueueTimeOutUnits);
            if (row == null) {
              log.debug("Timeout elapsed reading records.");
            }
          } catch (InterruptedException e) {
            log.debug("Caught InterruptedException while waiting for row.  Aborting.");
            isEnd.set(true);
            return null;
          }
        } while (row == null);
        
        if (row == END_MARKER) {
          isEnd.set(true);
          if (exp.get() != null) {
            String msg = "Parsing failed for xml, url:" + s + " rows processed in this xml:" + count;
            if (lastRow != null) msg += " last row in this xml:" + lastRow;
            if (ABORT.equals(onError)) {
              wrapAndThrow(SEVERE, exp.get(), msg);
            } else if (SKIP.equals(onError)) {
              wrapAndThrow(DataImportHandlerException.SKIP, exp.get());
            } else {
              log.warn(msg, exp.get());
            }
          }
          return null;
        } 
        count++;
        return lastRow = row;
      }

      @Override
      public void remove() {
        /*no op*/
      }
    };

  }


  public static final String URL = "url";

  public static final String HAS_MORE = "$hasMore";

  public static final String NEXT_URL = "$nextUrl";

  public static final String XPATH_FIELD_NAME = "$forEach";

  public static final String FOR_EACH = "forEach";

  public static final String XPATH = "xpath";

  public static final String COMMON_FIELD = "commonField";

  public static final String USE_SOLR_ADD_SCHEMA = "useSolrAddSchema";

  public static final String XSL = "xsl";

  public static final String STREAM = "stream";

}
