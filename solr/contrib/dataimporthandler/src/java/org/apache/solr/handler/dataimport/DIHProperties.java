package org.apache.solr.handler.dataimport;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in combstract clapliance with
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

import java.util.Date;
import java.util.Map;

/**
 * Implementations write out properties about the last data import
 * for use by the next import.  ex: to persist the last import timestamp
 * so that future delta imports can know what needs to be updated.
 * 
 * @lucene.experimental
 */
public abstract class DIHProperties {
  
  public abstract void init(DataImporter dataImporter, Map<String, String> initParams);
  
  public abstract boolean isWritable();
  
  public abstract void persist(Map<String, Object> props);
  
  public abstract Map<String, Object> readIndexerProperties();
  
  public abstract String convertDateToString(Date d);
  
  public Date getCurrentTimestamp() {
    return new Date();
  }
  
}
