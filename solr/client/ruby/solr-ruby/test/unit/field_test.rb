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
require 'solr'

class FieldTest < Test::Unit::TestCase
  
  def test_xml
    field = Solr::Field.new :creator => 'Erik Hatcher'
    assert_kind_of Solr::XML::Element, field.to_xml
    assert_match(/<field name=["']creator["']>Erik Hatcher<\/field>/, field.to_xml.to_s)
  end
  
  def test_escaped_xml
    field = Solr::Field.new :creator => 'Erik Hatcher & His Amazing Leaping Ability'
    assert_kind_of Solr::XML::Element, field.to_xml
    assert_match(/<field name=["']creator["']>Erik Hatcher &amp; His Amazing Leaping Ability<\/field>/, field.to_xml.to_s)
  end
  
  def test_xml_date
    field = Solr::Field.new :time => Time.now
    assert_kind_of Solr::XML::Element, field.to_xml
    assert_match(/<field name=["']time["']>[\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}Z<\/field>/, field.to_xml.to_s)
  end
  
  def test_i18n_xml
    field = Solr::Field.new :i18nstring => 'Äêâîôû Öëäïöü'
    assert_kind_of Solr::XML::Element, field.to_xml
    assert_match(/<field name=["']i18nstring["']>Äêâîôû Öëäïöü<\/field>/m, field.to_xml.to_s)
  end
  
  def test_boost_values
    field = Solr::Field.new(:blah => "squee", :boost => 3.0)
    assert_kind_of Solr::XML::Element, field.to_xml
    assert_match(/<field name=["']blah["'] boost=["']3.0["']>squee<\/field>/, field.to_xml.to_s)
  end
  
end
