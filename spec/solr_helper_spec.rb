require 'spec_helper'

describe SolrHelper do

  let(:solr_helper) {
    mock_class = Class.new
    mock_class.include(SolrHelper)
    mock_class.include(SpecHelper::ExposePrivate)
    mock_class.new
  }

  describe '#create_solr_document' do

    it 'creates a Solr document has from a JSON-LD metadata hash' do
      example = {
        'alveo:metadata' => { 'dc:isPartOf' => 'collection',
                              'olac:language' => 'eng',
                              'ausnc:mode' => 'written',
                              'ausnc:publication_status' => 'published',
                              'foobar:extra_field' => 'value'},
        'generated' => {'date_group' => '1800 - 1810',
                        'types' => ['Original'],
                        'handle' => 'collection:identifier',
                        'owner' => 'data_owner@alveo.edu.au'}
      }
      expected = {
        collection_name_facet: 'collection',
        date_group_facet: '1800 - 1810',
        DC_type_facet: ['Original'],
        OLAC_discourse_type_facet: 'unspecified',
        OLAC_language_facet: 'eng',
        AUSNC_mode_facet: 'written',
        AUSNC_speech_style_facet: 'unspecified',
        AUSNC_interactivity_facet: 'unspecified',
        AUSNC_communication_context_facet: 'unspecified',
        AUSNC_audience_facet: 'unspecified',
        AUSNC_written_mode_facet: 'unspecified',
        AUSNC_communication_setting_facet: 'unspecified',
        AUSNC_publication_status_facet: 'published',
        AUSNC_communication_medium_facet: 'unspecified',
        handle: 'collection:identifier',
        id: 'collection:identifier',
        full_text: 'unspecified',
        discover_access_group_ssim: "collection-discover",
        read_access_group_ssim: "collection-read",
        edit_access_group_ssim: "collection-edit",
        discover_access_person_ssim: 'data_owner@alveo.edu.au',
        read_access_person_ssim: 'data_owner@alveo.edu.au',
        edit_access_person_ssim: 'data_owner@alveo.edu.au',
        DC_created_sim: 'unspecified',
        DC_created_tesim: 'unspecified',
        DC_title_sim: 'unspecified',
        DC_title_tesim: 'unspecified',
        FOOBAR_extra_field_sim: 'value',
        FOOBAR_extra_field_tesim: 'value'
      }
      actual = solr_helper.create_solr_document(example)
      expect(actual).to eq(expected)
    end

  end

end
