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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This can be useful for users who have a DB field containing xml and wish to use a nested {@link XPathEntityProcessor}
 * <p>
 * The datasouce may be configured as follows
 * <p>
 * &lt;datasource name="f1" type="FieldReaderDataSource" /&gt;
 * <p>
 * The entity which uses this datasource must keep the url value as the variable name url="field-name"
 * <p>
 * The fieldname must be resolvable from {@link VariableResolver}
 * <p>
 * This may be used with any {@link EntityProcessor} which uses a {@link DataSource}&lt;{@link Reader}&gt; eg: {@link XPathEntityProcessor}
 * <p>
 * Supports String, BLOB, CLOB data types and there is an extra field (in the entity) 'encoding' for BLOB types
 *
 * @since 1.4
 */
public class FieldReaderDataSource extends DataSource<Reader> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected VariableResolver vr;
  protected String dataField;
  private String encoding;
  private EntityProcessorWrapper entityProcessor;

  @Override
  public void init(Context context, Properties initProps) {
    dataField = context.getEntityAttribute("dataField");
    encoding = context.getEntityAttribute("encoding");
    entityProcessor = (EntityProcessorWrapper) context.getEntityProcessor();
    /*no op*/
  }

  @Override
  public Reader getData(String query) {
    Object o = entityProcessor.getVariableResolver().resolve(dataField);
    if (o == null) {
       throw new DataImportHandlerException (SEVERE, "No field available for name : " +dataField);
    }
    if (o instanceof String) {
      return new StringReader((String) o);
    } else if (o instanceof Clob) {
      Clob clob = (Clob) o;
      try {
        //Most of the JDBC drivers have getCharacterStream defined as public
        // so let us just check it
        return readCharStream(clob);
      } catch (Exception e) {
        log.info("Unable to get data from CLOB");
        return null;

      }

    } else if (o instanceof Blob) {
      Blob blob = (Blob) o;
      try {
        return getReader(blob);
      } catch (Exception e) {
        log.info("Unable to get data from BLOB");
        return null;

      }
    } else {
      return new StringReader(o.toString());
    }

  }

  static Reader readCharStream(Clob clob) {
    try {
      return clob.getCharacterStream();
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e,"Unable to get reader from clob");
      return null;//unreachable
    }
  }

  private Reader getReader(Blob blob)
          throws SQLException, UnsupportedEncodingException {
    if (encoding == null) {
      return (new InputStreamReader(blob.getBinaryStream(), StandardCharsets.UTF_8));
    } else {
      return (new InputStreamReader(blob.getBinaryStream(), encoding));
    }
  }

  @Override
  public void close() {

  }
}
