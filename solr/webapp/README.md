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
  - installs the ng-select dependency: `npm install --save @ng-select/ng-select`
  - builds Solr UI: `ng build --prod --build-optimizer`

## debugging / development
  - `ng serve` requires angular app to be served from a separate host than solr itself, presenting CORS issues.
  - Instead, this development cycle may be more productive:
    - setup...
      - build solr server: `ant server`
      - run solr server: `bin/solr -c`
      - browse to solr ui under development: `http://localhost:8983/solr`
      - switch to angular directory: `cd webapp/angular`
    - develop....
      - make changes in a good typescript ide (I settled on atom w/typescript plugin).
      - run: `ng build`
      - clean out old deployment: `rm ../../server/solr-webapp/webapp/admin-ui/*`
      - copy in new deployment: `cp ./dist/* ../../server/solr-webapp/webapp/admin-ui/`
      - refresh browser

## References
  - https://angular.io/guide/deployment
  - https://stackoverflow.com/questions/45915379/how-to-setup-angular-4-inside-a-maven-based-java-war-project
