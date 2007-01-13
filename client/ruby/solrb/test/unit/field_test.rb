require 'solr/field'

class FieldTest < Test::Unit::TestCase
  
  def test_xml
    field = Solr::Field.new :creator => 'Erik Hatcher'
    assert_kind_of REXML::Element, field.to_xml
    assert_equal "<field name='creator'>Erik Hatcher</field>", field.to_xml.to_s
  end

end
