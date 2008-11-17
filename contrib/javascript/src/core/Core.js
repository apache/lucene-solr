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
 * @namespace A unique namespace inside jQuery.
 */
jQuery.solrjs = function() {};


/** 
 * A static "constructor" method that creates widget classes.
 *
 * <p> It uses manual jquery inheritance inspired by 
 * http://groups.google.com/group/jquery-dev/msg/12d01b62c2f30671' </p>
 * 
 * @param baseClass The name of the parent class. Set null to create a top level class. 
 * @param subClass The fields and methods of the new class.
 * @returns A constructor method that represents the new class. 
 * 
 * @example
 *   jQuery.solrjs.MyClass = jQuery.solrjs.createClass ("MyBaseClass", {
 *      property1: "value",
 *      function1: function() { alert("Hello World") }
 *   });
 *
 * 
 */
jQuery.solrjs.createClass = function(baseClassName, subClass) {
		
	// create new class, adding the constructor 
  var newClass = jQuery.extend(true, subClass , {	

		constructor : function(options) {
			// if a baseclass is specified, inherit methods and props, and store it in _super_			
			if (baseClassName != null) { 
	    	jQuery.extend(true, this, new jQuery.solrjs[baseClassName](options) );
			}
			// add new methods and props for this class
		  jQuery.extend(true, this , subClass);
      // add constructor arguments	    
			jQuery.extend(true, this, options); 
  	}
	});
	
	// make new class accessible
	return newClass.constructor;

};
