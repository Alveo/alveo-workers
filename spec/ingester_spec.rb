require 'spec_helper'

describe Ingester do

  before(:all) do
    @ingester_options = {work_queue: 'upload', error_queue: 'error', client_class: 'BunnyMock'}
    @ingester = Ingester.new(@ingester_options)
    @ingester.connect
    @exchange = @ingester.instance_variable_get(:@exchange)
  end

  describe '#process_metadata_rdf' do

    it 'converts a Turle RDF file a JSON-LD string and publishes it' do
      example = './spec/files/turtle_example.rdf'
      json_ld = File.read('./spec/files/ttl_to_json-ld_expanded_example.json')
      expected = "{\"metadata\":#{json_ld}}"
      queue = BunnyMock::Queue.new(@ingester_options[:upload_queue])
      queue.bind(@exchange)
      @ingester.process_metadata_rdf(example)
      actual = queue.messages.first
      expect(actual).to eq(expected)
    end

  end

  describe '#process_job' do

    it 'processes metadata RDFs' do
      collection = 'collection'
      example = 'item-metadata.rdf'
      expect(@ingester).to receive(:process_metadata_rdf).with(example)
      expect(@ingester).to receive(:add_to_sesame).with(collection, example)
      @ingester.process_job(collection, [example])
    end

    it 'does not process annotations RDFs' do
      collection = 'collection'
      example = 'item-ann.rdf'
      expect(@ingester).to receive(:add_to_sesame).with(collection, example)
      @ingester.process_job(collection, [example])
    end

    it 'logs and error if there is an error when processing' do
      collection = 'collection'
      example = 'item-metadata.rdf'
      allow(@ingester).to receive(:process_metadata_rdf).and_raise(StandardError)
      error_logger = @ingester.instance_variable_get(:@error_logger)
      expect(error_logger).to receive(:error)
      @ingester.process_job(collection, [example])
    end

  end

end