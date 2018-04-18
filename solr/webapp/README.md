# Solr Admin conversion
  - This is a conversion from "AngularJS" to "Angular"

## Old Version
  - The unconverted portions of the AnguarJS UI are located in `solr/webapp/angularJS`
  - These are for reference and convenience during development of the new version.

## Jetty config
  - The web.xml file is at solr/webapp/web/WEB-INF for packaging in the war

## Versions used
  - node: v8.11.0
  - npm: v5.6.0
  - ng: v.1.7.4

## Building Angular UI
  - `cd solr/webapp/angular`
  - installs dependencies in "node_modules": `npm install`
  - builds Solr UI: `ng build --prod --build-optimizer`

## References
  - https://angular.io/guide/deployment
  - https://stackoverflow.com/questions/45915379/how-to-setup-angular-4-inside-a-maven-based-java-war-project
