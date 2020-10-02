We have a movie data set in JSON, Solr XML, and CSV formats.
All 3 formats contain the same data.  You can use any one format to index documents to Solr.

The data is fetched from Freebase and the data license is present in the films-LICENSE.txt file.

This data consists of the following fields:
 * "id" - unique identifier for the movie
 * "name" - Name of the movie
 * "directed_by" - The person(s) who directed the making of the film
 * "initial_release_date" - The earliest official initial film screening date in any country
 * "genre" - The genre(s) that the movie belongs to

 Steps:
   * Start Solr:
     ```
     bin/solr start
     ```

   * Create a "films" core:
   
     ```
     bin/solr create -c films
     ```

   * Set the schema on a couple of fields that Solr would otherwise guess differently (than we'd like) about:
   
      ```
      curl http://localhost:8983/solr/films/schema -X POST -H 'Content-type:application/json' --data-binary '{
        "add-field" : {
          "name":"name",
          "type":"text_general",
          "multiValued":false,
          "stored":true
        },
        "add-field" : {
          "name":"initial_release_date",
          "type":"pdate",
          "stored":true
        }
      }'
      ```

   * Now let's index the data, using one of these three commands:

     - JSON: `bin/post -c films example/films/films.json`
     - XML: `bin/post -c films example/films/films.xml`
     - CSV: 
     ```
         bin/post \
                  -c films \
                  example/films/films.csv \
                  -params "f.genre.split=true&f.directed_by.split=true&f.genre.separator=|&f.directed_by.separator=|"
     ```
   * Let's get searching!
     - Search for 'Batman':
     
       http://localhost:8983/solr/films/query?q=name:batman

       * If you get an error about the name field not existing, you haven't yet indexed the data
       * If you don't get an error, but zero results, chances are that the _name_ field schema type override wasn't set
         before indexing the data the first time (it ended up as a "string" type, requiring exact matching by case even).
         It's easiest to simply reset the environment and try again, ensuring that each step successfully executes.

     - Show me all 'Super hero' movies:
     
       http://localhost:8983/solr/films/query?q=*:*&fq=genre:%22Superhero%20movie%22

     - Let's see the distribution of genres across all the movies. See the facet section of the response for the counts:
     
       http://localhost:8983/solr/films/query?q=*:*&facet=true&facet.field=genre

Exploring the data further - 

  * Increase the MAX_ITERATIONS value, put in your freebase API_KEY and run the film_data_generator.py script using Python 3.
    Now re-index Solr with the new data.

FAQ:
  Why override the schema of the _name_ and _initial_release_date_ fields?

     Without overriding those field types, the _name_ field would have been guessed as a multi-valued string field type
     and _initial_release_date_ would have been guessed as a multi-valued pdate type.  It makes more sense with this
     particular data set domain to have the movie name be a single valued general full-text searchable field,
     and for the release date also to be single valued.

  How do I clear and reset my environment?

      See the script below.

  Is there an easy to copy/paste script to do all of the above?

```
    Here ya go << END_OF_SCRIPT

bin/solr stop
rm server/logs/*.log
rm -Rf server/solr/films/
bin/solr start
bin/solr create -c films
curl http://localhost:8983/solr/films/schema -X POST -H 'Content-type:application/json' --data-binary '{
    "add-field" : {
        "name":"name",
        "type":"text_general",
        "multiValued":false,
        "stored":true
    },
    "add-field" : {
        "name":"initial_release_date",
        "type":"pdate",
        "stored":true
    }
}'
bin/post -c films example/films/films.json

# END_OF_SCRIPT
```
