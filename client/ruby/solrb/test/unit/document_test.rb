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

require 'test/unit'
require 'solr/document'
require 'solr/field'

class DocumentTest < Test::Unit::TestCase

  def test_xml
    doc = Solr::Document.new
    doc << Solr::Field.new(:creator => 'Erik Hatcher')
    assert_kind_of REXML::Element, doc.to_xml
    assert "<doc><field name='creator'>Erik Hatcher</field></doc>", 
      doc.to_xml.to_s
  end

  def test_repeatable
    doc = Solr::Document.new
    doc << Solr::Field.new(:creator => 'Erik Hatcher')
    doc << Solr::Field.new(:creator => 'Otis Gospodnetic')
    assert "<doc><field name='creator'>Erik Hatcher</field><field name='creator'>Otis Gospodnetic</field></doc>", doc.to_xml.to_s
  end
  
  def test_bad_doc
    doc = Solr::Document.new
    assert_raise(RuntimeError) do
      doc << "invalid"
    end
  end

  def test_hash_shorthand
    doc = Solr::Document.new :creator => 'Erik Hatcher', :title => 'Lucene in Action'
    assert_equal 'Erik Hatcher', doc[:creator]
    assert_equal 'Lucene in Action', doc[:title]
    assert_equal nil, doc[:foo]
    
    doc = Solr::Document.new
    doc << {:creator => 'Erik Hatcher', :title => 'Lucene in Action'}
    doc[:subject] = 'Search'
    assert_equal 'Erik Hatcher', doc[:creator]
    assert_equal 'Lucene in Action', doc[:title]
    assert_equal 'Search', doc[:subject]
  end

end
