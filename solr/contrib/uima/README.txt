Getting Started
---------------
To start using Solr UIMA Metadata Extraction Library you should go through the following configuration steps:

1. copy generated solr-uima jar and its libs (under contrib/uima/lib) inside a Solr libraries directory.

2. modify your schema.xml adding the fields you want to be hold metadata specifying proper values for type, indexed, stored and multiValued options:

3. for example you could specify the following
  <field name="language" type="string" indexed="true" stored="true" required="false"/>
  <field name="concept" type="string" indexed="true" stored="true" multiValued="true" required="false"/>
  <field name="sentence" type="text" indexed="true" stored="true" multiValued="true" required="false" />

4. modify your solrconfig.xml adding the following snippet:
  <uimaConfig>
    <runtimeParameters>
      <keyword_apikey>VALID_ALCHEMYAPI_KEY</keyword_apikey>
      <concept_apikey>VALID_ALCHEMYAPI_KEY</concept_apikey>
      <lang_apikey>VALID_ALCHEMYAPI_KEY</lang_apikey>
      <cat_apikey>VALID_ALCHEMYAPI_KEY</cat_apikey>
      <entities_apikey>VALID_ALCHEMYAPI_KEY</entities_apikey>
      <oc_licenseID>VALID_OPENCALAIS_KEY</oc_licenseID>
    </runtimeParameters>
    <analysisEngine>/org/apache/uima/desc/OverridingParamsExtServicesAE.xml</analysisEngine>
    <analyzeFields merge="false">text</analyzeFields>
    <fieldMapping>
      <type name="org.apache.uima.alchemy.ts.concept.ConceptFS">
       <map feature="text" field="concept"/>
      </type>
      <type name="org.apache.uima.alchemy.ts.language.LanguageFS">
       <map feature="language" field="language"/>
      </type>
      <type name="org.apache.uima.SentenceAnnotation">
        <map feature="coveredText" field="sentence"/>
      </type>
    </fieldMapping>
  </uimaConfig>
  
5. the analysisEngine tag must contain an AE descriptor inside the specified path in the classpath

6. the analyzeFields tag must contain the input fields that need to be analyzed by UIMA,
   if merge=true then their content will be merged and analyzed only once

7. field mapping describes which features of which types should go in a field

8. define in your solrconfig.xml an UpdateRequestProcessorChain as following:
  <updateRequestProcessorChain name="uima">
    <processor class="org.apache.solr.uima.processor.UIMAProcessorFactory"/>
    <processor class="solr.LogUpdateProcessorFactory" />
    <processor class="solr.RunUpdateProcessorFactory" />
  </updateRequestProcessorChain>

9. in your solrconfig.xml replace the existing default (<requestHandler name="/update"...)  or create a new UpdateRequestHandler with the following:
  <requestHandler name="/update" class="solr.XmlUpdateRequestHandler">
    <lst name="defaults">
      <str name="update.processor">uima</str>
    </lst>
  </requestHandler>

Once you're done with the configuration you can index documents which will be automatically enriched with the specified fields
