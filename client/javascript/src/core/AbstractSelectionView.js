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

/**
 * Displays the current selection. Use the displaySelection method.
 * 
 * @param properties A map of fields to set. Refer to the list of non-private fields.  
 * @class AbstractSelectionView
 */
jQuery.solrjs.AbstractSelectionView = jQuery.solrjs.createClass (null, /** @lends jQuery.solrjs.AbstractSelectionView.prototype */ { 

  /** 
   * A unique identifier of this widget.
   *
   * @field 
   * @public
   */
  id : "", 
  
  /** 
   * A css classname representing the "target div" inside the html page.
   * All ui changes will be performed inside this empty div.
   * 
   * @field 
   * @private
   */
  target : "",

  displaySelection : function(selectedItems) { 
    throw "Abstract method displaySelection";    	
	}
	
});
