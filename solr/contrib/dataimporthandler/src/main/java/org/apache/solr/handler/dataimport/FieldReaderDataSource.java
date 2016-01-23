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
package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Blob;
import java.sql.Clob;
import java.util.Properties;

/**
 * This can be useful for users who have a DB field containing xml and wish to use a nested XPathEntityProcessor
 * <p/>
 * The datasouce may be configured as follows
 * <p/>
 * <datasource name="f1" type="FieldReaderDataSource" />
 * <p/>
 * The enity which uses this datasource must keep the url value as the variable name url="field-name"
 * <p/>
 * The fieldname must be resolvable from VariableResolver
 * <p/>
 * This may be used with any EntityProcessor which uses a DataSource<Reader> eg:XPathEntityProcessor
 * <p/>
 * Supports String, BLOB, CLOB data types and there is an extra field (in the entity) 'encoding' for BLOB types
 *
 * @version $Id$
 * @since 1.4
 */
public class FieldReaderDataSource extends DataSource<Reader> {
  private static final Logger LOG = LoggerFactory.getLogger(FieldReaderDataSource.class);
  protected VariableResolver vr;
  protected String dataField;
  private String encoding;
  private EntityProcessorWrapper entityProcessor;

  public void init(Context context, Properties initProps) {
    dataField = context.getEntityAttribute("dataField");
    encoding = context.getEntityAttribute("encoding");
    entityProcessor = (EntityProcessorWrapper) context.getEntityProcessor();
    /*no op*/
  }

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
        LOG.info("Unable to get data from CLOB");
        return null;

      }

    } else if (o instanceof Blob) {
      Blob blob = (Blob) o;
      try {
        //Most of the JDBC drivers have getBinaryStream defined as public
        // so let us just check it
        Method m = blob.getClass().getDeclaredMethod("getBinaryStream");
        if (Modifier.isPublic(m.getModifiers())) {
          return getReader(m, blob);
        } else {
          // force invoke
          m.setAccessible(true);
          return getReader(m, blob);
        }
      } catch (Exception e) {
        LOG.info("Unable to get data from BLOB");
        return null;

      }
    } else {
      return new StringReader(o.toString());
    }

  }

  static Reader readCharStream(Clob clob) {
    try {
      Method m = clob.getClass().getDeclaredMethod("getCharacterStream");
      if (Modifier.isPublic(m.getModifiers())) {
        return (Reader) m.invoke(clob);
      } else {
        // force invoke
        m.setAccessible(true);
        return (Reader) m.invoke(clob);
      }
    } catch (Exception e) {
      wrapAndThrow(SEVERE, e,"Unable to get reader from clob");
      return null;//unreachable
    }
  }

  private Reader getReader(Method m, Blob blob)
          throws IllegalAccessException, InvocationTargetException, UnsupportedEncodingException {
    InputStream is = (InputStream) m.invoke(blob);
    if (encoding == null) {
      return (new InputStreamReader(is));
    } else {
      return (new InputStreamReader(is, encoding));
    }
  }

  public void close() {

  }
}
