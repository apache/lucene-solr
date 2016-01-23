<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Solr-Powered File Search

This README guides you through creating a Solr-powered search engine for your own set of files including Word documents,
PDFs, HTML, and many other supported types. 

For further explanations, see the frequently asked questions at the end of the guide.

##GETTING STARTED

* To start Solr, enter the following command (make sure you’ve cd’ed into the directory in which Solr was installed): 

	bin/solr start 

* If you’ve started correctly, you should see the following output:
	
		Waiting to see Solr listening on port 8983 [/]  
		Started Solr server on port 8983 (pid=<your pid>). Happy searching!
<hr>

##CREATING THE CORE/COLLECTION

* Before you can index your documents, you’ll need to create a core/collection. Do this by entering:

		bin/solr create -c files -d example/files/conf

* Now you’ve created a core called “files” using a configuration tuned for indexing and querying rich text files.

* You should see the following response:

		Creating new core 'files' using command:
		http://localhost:8983/solr/admin/cores?action=CREATE&name=files&instanceDir=files

		{
			"responseHeader":{
				"status":0,
				"QTime":239},
			"core":"files"}

<hr>
##INDEXING DOCUMENTS

* Return to your command shell. To post all of your documents to the documents core, enter the following: 

		bin/post -c files ~/Documents

* Depending on how many documents you have, this could take a while. Sit back and watch the magic happen. When all of your documents have been indexed you’ll see something like:

		<some number> files indexed.
		COMMITting Solr index changes to http://localhost:8983/solr/files/update...
		Time spent: <some amount of time>
		
* To see a list of accepted file types, do:
  	  	bin/post -h
	

<hr>
##BROWSING DOCUMENTS

* Your document information can be viewed in multiple formats: XML, JSON, CSV, as well as a nice HTML interface. 

* To view your document information in the HTML interface view, adjust the URL in your address bar to [http://localhost:8983/solr/files/browse](http://localhost:8983/solr/files/browse)

* To view your document information in XML or other formats, add &wt (for writer type) to the end of that URL. i.e. To view your results in xml format direct your browser to:
	[http://localhost:8983/solr/files/browse?&wt=xml](http://localhost:8983/solr/files/browse?&wt=xml)

<hr>
##ADMIN UI

* Another way to verify that your core has been created is to view it in the Admin User Interface.

	- The Admin_UI serves as a visual tool for indexing and querying your index in Solr.

* To access the Admin UI, go to your browser and visit :
	[http://localhost:8983/solr/](http://localhost:8983/solr/)

	- <i>The Admin UI is only accessible when Solr is running</i>

* On the left-hand side of the home page, click on “Core Selector”. The core you created, called “files” should be listed there; click on it. If it’s not listed, your core was not created and you’ll need to re-enter the create command.
* Alternatively, you could just go to the core page directly by visiting : [http://localhost:8983/solr/#/files](http://localhost:8983/solr/#/files)

* Now you’ve opened the core page. On this page there are a multitude of different tools you can use to analyze and search your core. You will make use of these features after indexing your documents.
* Take note of the "Num Docs" field in your core Statistics. If after indexing your documents, it shows Num Docs to be 0, that means there was a problem indexing.

<hr>
##QUERYING INDEX

* In the Admin UI, enter a term in the query box to see which documents contain the word. 

* You can filter the results by switching between the different content type tabs. To view an international version of this interface, hover over the globe icon in the top right hand section of the page.

* Notice the tag cloud on the right side, which facets by top phrases extracted during indexing.
  Click on the phrases to see which documents contain them.

* Another way to query the index is by manipulating the URL in your address bar once in the browse view.

* i.e. : [http://localhost:8983/solr/files/browse?q=Lucene](http://localhost:8983/solr/files/browse?q=Lucene)
<hr>
##FAQs

* Why use -d when creating a core?
	* -d specifies a specific configuration to use.  This example as a configuration tuned for indexing and query rich
	  text files.
	
* How do I delete a core?
	* To delete a core (i.e. files), you can enter the following in your command shell:
		bin/solr delete -c files

	* You should see the following output:
	
		Deleting core 'files' using command:
			http://localhost:8983/solr/admin/cores?action=UNLOAD&core=files&deleteIndex=true&deleteDataDir=true&deleteInstanceDir=true
    
			{"responseHeader":{
					"status":0,
					"QTime":19}}

	* This calls the Solr core admin handler, "UNLOAD", and the parameters "deleteDataDir" and "deleteInstanceDir" to ensure that all data associated with core is also removed

* How can I change the /browse UI?

	The primary templates are under example/files/conf/velocity.  **In order to edit those files in place (without having to
	re-create or patch a core/collection with an updated configuration)**, Solr can be started with a special system property
	set to the _absolute_ path to the conf/velocity directory, like this: 
	
		bin/solr start -Dvelocity.template.base.dir=</full/path/to>/example/files/conf/velocity/
	
        If you want to adjust the browse templates for an existing collection, edit the core’s configuration
        under server/solr/files/conf/velocity.


=======

* Provenance of free images used in this example:
  - Globe icon: visualpharm.com
  - Flag icons: freeflagicons.com