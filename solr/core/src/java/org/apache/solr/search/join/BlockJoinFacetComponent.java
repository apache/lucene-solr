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
package org.apache.solr.search.join;

/** this is just a stub refers to {@link BlockJoinDocSetFacetComponent} to avoid
 * changes in configs
 * 
 * @deprecated This functionality is considered deprecated and will be removed at 9.0
 * Users are encouraged to use <code>"uniqueBlock(\_root_)"</code> aggregation 
 * under <code>"terms"</code> facet and <code>"domain": { "blockChildren":...}</code>  */
@Deprecated
public class BlockJoinFacetComponent extends BlockJoinDocSetFacetComponent {

}
