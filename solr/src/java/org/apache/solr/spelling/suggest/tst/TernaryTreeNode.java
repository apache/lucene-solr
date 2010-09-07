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

package org.apache.solr.spelling.suggest.tst;

/**
 * The class creates a TST node.
 * @variable splitchar the character stored by a node.
 * @variable loKid a reference object to the node containing character smaller than
 * this node's character.
 * @variable eqKid a reference object to the node containg character next to this
 * node's character as occuring in the inserted token.
 * @variable hiKid a reference object to the node containing character higher than
 * this node's character.
 * @variable token used by leaf nodes to store the complete tokens to be added to 
 * suggest list while auto-completing the prefix.
 */

public class TernaryTreeNode {
	char splitchar;
	TernaryTreeNode loKid, eqKid, hiKid;
	String token;
	Object val;
}
