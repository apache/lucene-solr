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

package org.apache.solr.search;


/**
 * A struct who's only purpose is to hold both a DocList and a DocSet so that both
 * may be returned from a single method.
 * <p>
 * The DocList and DocSet returned should <b>not</b> be modified as they may
 * have been retrieved or inserted into a cache and should be considered shared.
 * <p>
 * Oh, if only java had "out" parameters or multiple return args...
 * <p>
 *
 * @author yonik
 * @version $Id: DocListAndSet.java,v 1.3 2005/04/08 05:38:05 yonik Exp $
 * @since solr 0.9
 */
public final class DocListAndSet {
  public DocList docList;
  public DocSet docSet;
}
