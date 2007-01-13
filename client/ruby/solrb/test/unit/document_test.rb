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

  def test_hash_shorthand
    doc = Solr::Document.new :creator => 'Erik Hatcher', :title => 'Lucene in Action'
    assert_equal 'Erik Hatcher', doc[:creator]
    assert_equal 'Lucene in Action', doc[:title]
    assert_equal nil, doc[:foo]
  end

end
