require 'spec_helper'

describe MetadataHelper do

  let(:metadata_helper) { Class.new.include(MetadataHelper).new }


  describe '#generate_fields' do

    it 'generates additional fields used by other workers' do
      example = {
        'alveo:metadata' => {
          'dcterms:created' => '1994',
          'dcterms:isPartOf' => 'collection',
          'dcterms:identifier' => 'identifier',
        },
        'ausnc:document' => [
          {'dcterms:type' => 'Text'},
          {'dcterms:type' => 'Audio'}
        ]
      }
      allow(metadata_helper).to receive(:get_collection).and_return({owner: 'data_owner@intersect.org.au', id: 3})
      expected = {
        'date_group' => '1990 - 1999',
        'types' => ['Text', 'Audio'],
        'owner' => 'data_owner@intersect.org.au',
        'collection_id' => 3,
        'handle' => 'collection:identifier'
      } 
      actual = metadata_helper.generate_fields(example)
      expect(actual).to eq(expected)
    end

  end


  describe '#get_collection' do

    it 'retrieves the collection id and owner email if not cached' do
      pending('Implement me')
      fail
    end

    it 'returns the cached version if previous retrieved' do
      collection = {id: 3, owner: 'data_owner@intersect.org.au'}
      MetadataHelper.class_variable_set(:@@COLLECTIONS, {'collection' => collection})
      example = {
        'alveo:metadata' => {
          'dcterms:isPartOf' => 'collection',
        }
      }
      expected = collection
      actual = metadata_helper.get_collection(example)
      expect(actual).to eq(expected)
    end

  end


  describe '#get_handle' do

    it 'Generates a handle from the collection and identifier metadata' do
      example = {'alveo:metadata' => {'dcterms:isPartOf' => 'collection',
                'dcterms:identifier' => 'identifier'}}
      expected = 'collection:identifier'
      actual = metadata_helper.get_handle(example)
      expect(actual).to eq(expected)
    end

  end


  describe '#get_types' do

    it 'returns the documents types in the dcterms:type field' do
      example = {'ausnc:document' => [{'dcterms:type' => 'Text'}, {'dcterms:type' => 'Audio'}]}
      expected = ['Text', 'Audio']
      actual = metadata_helper.get_types(example)
      expect(actual).to eq(expected)
    end

    it 'returns the "unspecified" types if the dcterms:type field is not present' do
      example = {'ausnc:document' => [{}]}
      expected = ['unspecified']
      actual = metadata_helper.get_types(example)
      expect(actual).to eq(expected)
    end

  end


  describe '#get_date_group' do

    it 'returns a default range of 10' do
      example = {'alveo:metadata' => {'dcterms:created' => '1994'}}
      test_get_date_group(example, '1990 - 1999')
    end

    it 'returns an arbitrary range' do
      example = {'alveo:metadata' => {'dcterms:created' => '1994'}}
      test_get_date_group(example, '1988 - 1994', 7)
    end

    it 'returns "Unknown" for bad input' do
      example = {'alveo:metadata' => {'dcterms:created' => 'wutang clan'}}
      test_get_date_group(example, 'Unknown')
    end
    
    it 'returns "Unknown" for nil' do
      example = {'alveo:metadata' => {'dcterms:created' => nil}}
      test_get_date_group(example, 'Unknown')
    end

    def test_get_date_group(example, expected, resolution=10)
      actual = metadata_helper.get_date_group(example, resolution)
      expect(actual).to eq(expected)
    end

  end


  describe '#extract_year' do

    it 'parses "YYYY?"' do
      test_extract_year('1913?', 1913)
    end

    it 'parses "DD/MM/YY"' do
      test_extract_year('30/10/93', 1993)
    end

    it 'parses "YY/MM/DD"' do
      test_extract_year('96/05/17', 1996)
    end

    it 'parses "DD-DD/MM/YY"' do
      test_extract_year('7-11/11/94', 1994)
    end

    it 'parses "DD&DD/MM/YY"' do
      test_extract_year('17&19/8/93', 1993)
    end

    it 'parses "YYYY-MM-DD"' do
      test_extract_year('2012-03-07', 2012)
    end

    it 'parses "Month YYYY"' do
      test_extract_year('August 2000', 2000)
    end

    it 'parses "DD Month YYYY"' do
      test_extract_year('6 September 1986', 1986)
    end

    it 'parses "DD Season YYYY"' do
      test_extract_year('4 Spring 1986', 1986)
    end

    it 'throws an exception when parsing "Phase N season"' do
      example = 'Phase I fall'
      expect {
        metadata_helper.extract_year(example)
      }.to raise_error(ArgumentError)
    end

    def test_extract_year(example, expected)
      actual = metadata_helper.extract_year(example)
      expect(actual).to eq(expected)
    end

  end

end