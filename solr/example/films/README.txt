We have a movie data set in JSON, Solr XML, and CSV formats.
All 3 formats contain the same data.  You can use any one format to index documents to Solr.

The data is fetched from Freebase and the data license is present in the films-LICENSE.txt file.

This data consists of the following fields -
 * "id" - unique identifier for the movie
 * "name" - Name of the movie
 * "directed_by" - The person(s) who directed the making of the film
 * "initial_release_date" - The earliest official initial film screening date in any country
 * "genre" - The genre(s) that the movie belongs to

 Steps:
   * Start Solr:
       bin/solr start

   * Create a "films" core
       bin/solr create_core -n films -c data_driven_schema_configs

   * Update the schema (by default it will guess the field types based on the date as it is indexed):
curl http://localhost:8983/solr/films/schema/fields -X POST -H 'Content-type:application/json' --data-binary '
[
    {
        "name":"genre",
        "type":"string",
        "stored":true,
        "multiValued":true
    },
    {
        "name":"directed_by",
        "type":"string",
        "stored":true,
        "multiValued":true
    },
    {
        "name":"name",
        "type":"text_general",
        "stored":true
    },
    {
        "name":"initial_release_date",
        "type":"tdate",
        "stored":true
    }
]'

   * Now let's index the data, using one of these three commands:

     - JSON: bin/post films example/films/films.json
     - XML: bin/post films example/films/films.xml
     - CSV: bin/post films example/films/films.csv params=f.genre.split=true&f.directed_by.split=true&f.genre.separator=|&f.directed_by.separator=|

   * Let's get searching.
     - Search for 'Batman':
       http://localhost:8983/solr/films/query?q=name:batman

     - Show me all 'Super hero' movies:
       http://localhost:8983/solr/films/query?q=*:*&fq=genre:%22Superhero%20movie%22

     - Let's see the distribution of genres across all the movies. See the facet section for the counts:
       http://localhost:8983/solr/films/query?q=*:*&facet=true&facet.field=genre

Exploring the data further - 

  * Increase the MAX_ITERATIONS value, put in your freebase API_KEY and run the exampledocs_generator.py script using Python 3.
    Now re-index Solr with the new data.