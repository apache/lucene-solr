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

/**
 * JDBC Driver Package
 *
 * Sample usage
 * <pre>
 * Connection con = null;
 * Statement stmt = null;
 * ResultSet rs = null;
 *
 * try {
 *  con = DriverManager.getConnection("jdbc:solr://zkHost:port?collection=collection&amp;aggregationMode=map_reduce");
 *  stmt = con.createStatement();
 *  rs = stmt.executeQuery("select a, sum(b) from tablex group by a");
 *  while(rs.next()) {
 *    String a = rs.getString("a");
 *    double sumB = rs.getString("sum(b)");
 *  }
 * } finally {
 *  rs.close();
 *  stmt.close();
 *  con.close();
 * }
 * </pre>
 *
 * Connection properties can also be passed in using a Properties object.
 *
 * The <b>collection</b> parameter is mandatory and should point to a SolrCloud collection that is configured with the /sql
 * request handler.
 *
 * The aggregationMode parameter is optional. It can be used to switch between Map/Reduce (map_reduce) or the JSON Facet API (facet) for
 * group by aggregations. The default is "facet".
 **/

package org.apache.solr.client.solrj.io.sql;
