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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This can be useful for users who have a DB field containing BLOBs which may be Rich documents
 * <p>
 * The datasource may be configured as follows
 * <p>
 * &lt;dataSource name="f1" type="FieldStreamDataSource" /&gt;
 * <p>
 * The entity which uses this datasource must keep and attribute dataField
 * <p>
 * The fieldname must be resolvable from {@link VariableResolver}
 * <p>
 * This may be used with any {@link EntityProcessor} which uses a {@link DataSource}&lt;{@link InputStream}&gt; eg: TikaEntityProcessor
 *
 * @since 3.1
 */
public class FieldStreamDataSource extends DataSource<InputStream> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected VariableResolver vr;
  protected String dataField;
  private EntityProcessorWrapper wrapper;

  @Override
  public void init(Context context, Properties initProps) {
    dataField = context.getEntityAttribute("dataField");
    wrapper = (EntityProcessorWrapper) context.getEntityProcessor();
    /*no op*/
  }

  @Override
  public InputStream getData(String query) {
    Object o = wrapper.getVariableResolver().resolve(dataField);
    if (o == null) {
      throw new DataImportHandlerException(SEVERE, "No field available for name : " + dataField);
    } else if (o instanceof Blob) {
      Blob blob = (Blob) o;
      try {
        return blob.getBinaryStream();
      } catch (SQLException sqle) {
        log.info("Unable to get data from BLOB");
        return null;
      }
    } else if (o instanceof byte[]) {
      byte[] bytes = (byte[]) o;
      return new ByteArrayInputStream(bytes);
    } else {
      throw new RuntimeException("unsupported type : " + o.getClass());
    } 

  }

  @Override
  public void close() {
  }
}
