Getting Started
---------------
To start using Solr UIMA Metadata Extraction Library you should go through the following configuration steps:

1. copy generated solr-uima jar and its libs (under contrib/uima/lib) inside a Solr libraries directory.
   or set <lib/> tags in solrconfig.xml appropriately to point those jar files.

   <lib dir="../../contrib/uima/lib" />
   <lib dir="../../dist/" regex="apache-solr-uima-\d.*\.jar" />

2. modify your schema.xml adding the fields you want to be hold metadata specifying proper values for type, indexed, stored and multiValued options:

   for example you could specify the following

  <field name="language" type="string" indexed="true" stored="true" required="false"/>
  <field name="concept" type="string" indexed="true" stored="true" multiValued="true" required="false"/>
  <field name="sentence" type="text" indexed="true" stored="true" multiValued="true" required="false" />

3. modify your solrconfig.xml adding the following snippet:

  <updateRequestProcessorChain name="uima">
    <processor class="org.apache.solr.uima.processor.UIMAUpdateRequestProcessorFactory">
      <lst name="uimaConfig">
        <lst name="runtimeParameters">
          <str name="keyword_apikey">VALID_ALCHEMYAPI_KEY</str>
          <str name="concept_apikey">VALID_ALCHEMYAPI_KEY</str>
          <str name="lang_apikey">VALID_ALCHEMYAPI_KEY</str>
          <str name="cat_apikey">VALID_ALCHEMYAPI_KEY</str>
          <str name="entities_apikey">VALID_ALCHEMYAPI_KEY</str>
          <str name="oc_licenseID">VALID_OPENCALAIS_KEY</str>
        </lst>
        <str name="analysisEngine">/org/apache/uima/desc/OverridingParamsExtServicesAE.xml</str>
        <!-- Set to true if you want to continue indexing even if text processing fails.
             Default is false. That is, Solr throws RuntimeException and
             never indexed documents entirely in your session. -->
        <bool name="ignoreErrors">true</bool>
        <!-- This is optional. It is used for logging when text processing fails.
             If logField is not specified, uniqueKey will be used as logField.
        <str name="logField">id</str>
        -->
        <lst name="analyzeFields">
          <bool name="merge">false</bool>
          <arr name="fields">
            <str>text</str>
          </arr>
        </lst>
        <lst name="fieldMappings">
          <lst name="type">
            <str name="name">org.apache.uima.alchemy.ts.concept.ConceptFS</str>
            <lst name="mapping">
              <str name="feature">text</str>
              <str name="field">concept</str>
            </lst>
          </lst>
          <lst name="type">
            <str name="name">org.apache.uima.alchemy.ts.language.LanguageFS</str>
            <lst name="mapping">
              <str name="feature">language</str>
              <str name="field">language</str>
            </lst>
          </lst>
          <lst name="type">
            <str name="name">org.apache.uima.SentenceAnnotation</str>
            <lst name="mapping">
              <str name="feature">coveredText</str>
              <str name="field">sentence</str>
            </lst>
          </lst>
        </lst>
      </lst>
    </processor>
    <processor class="solr.LogUpdateProcessorFactory" />
    <processor class="solr.RunUpdateProcessorFactory" />
  </updateRequestProcessorChain>

   where VALID_ALCHEMYAPI_KEY is your AlchemyAPI Access Key. You need to register AlchemyAPI Access
   key to exploit the AlchemyAPI services: http://www.alchemyapi.com/api/register.html

   where VALID_OPENCALAIS_KEY is your Calais Service Key. You need to register Calais Service
   key to exploit the Calais services: http://www.opencalais.com/apikey
  
   the analysisEngine must contain an AE descriptor inside the specified path in the classpath

   the analyzeFields must contain the input fields that need to be analyzed by UIMA,
   if merge=true then their content will be merged and analyzed only once

   field mapping describes which features of which types should go in a field

4. in your solrconfig.xml replace the existing default (<requestHandler name="/update"...)  or create a new UpdateRequestHandler with the following:
  <requestHandler name="/update" class="solr.XmlUpdateRequestHandler">
    <lst name="defaults">
      <str name="update.processor">uima</str>
    </lst>
  </requestHandler>

Once you're done with the configuration you can index documents which will be automatically enriched with the specified fields
