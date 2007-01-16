# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'marc'
require 'solr'

solr_url = ENV["SOLR_URL"] || "http://localhost:8983/solr"
marc_filename = ARGV[0]
debug = ARGV[1] == "-debug"

$KCODE = 'UTF8'

mapping = {
  # :solr_field_name => String
  # :solr_field_name => Array of Strings
  # :solr_field_name => Proc  [Proc operates on record]
  #    String = 3 digit control field number or 3 digit data field number + subfield letter
  
  :id => '001',
  :subject_genre_facet => ['600v', '610v', '611v', '651v', '650v', '655a'], 
  :subject_era_facet => ['650y', '651y'],
  :subject_topic_facet => ['650a', '650x'],
  :title_text => '245a',
  :author_text => '100a',
}

connection = Solr::Connection.new(solr_url)

reader = MARC::Reader.new(marc_filename)
count = 0

def extract_record_data(record, fields)
  extracted_data = []

  fields.each do |field|
    tag = field[0,3]
    
    extracted_fields = record.find_all {|f| f.tag === tag}

    extracted_fields.each do |field_instance|    
      if tag < '010' # control field
        extracted_data << field_instance.value rescue nil
      else # data field
        subfield = field[3].chr
        extracted_data << field_instance[subfield] rescue nil
      end
    end
  end
  
  extracted_data.compact.uniq
end

for record in reader
  doc = {}
  mapping.each do |key,value|
    data = nil
    case value
      when Proc
        data = value.call(record)
        
      when String, Array
        data = extract_record_data(record, value)
        data = nil if data.empty?
    end
    
    doc[key] = data if data
  end
  
  puts doc.inspect,"------" if debug

  connection.send(Solr::Request::AddDocument.new(doc)) unless debug
  
  count += 1
  
  puts count if count % 100 == 0
end

connection.send(Solr::Request::Commit.new) unless debug
