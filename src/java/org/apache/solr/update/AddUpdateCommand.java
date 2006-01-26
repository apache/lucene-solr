/**
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.update;

import org.apache.lucene.document.Document;
/**
 * @author yonik
 * @version $Id$
 */
public class AddUpdateCommand extends UpdateCommand {
   public String id;
   public Document doc;
   public boolean allowDups;
   public boolean overwritePending;
   public boolean overwriteCommitted;

   public AddUpdateCommand() {
     super("add");
   }

   public String toString() {
     StringBuilder sb = new StringBuilder(commandName);
     sb.append(':');
     if (id!=null) sb.append("id=").append(id);
     sb.append(",allowDups=").append(allowDups);
     sb.append(",overwritePending=").append(overwritePending);
     sb.append(",overwriteCommitted=").append(overwriteCommitted);
     return sb.toString();
   }
 }
