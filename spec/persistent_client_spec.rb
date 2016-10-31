require 'spec_helper'

describe PersistentClient do

  before(:all) do
   @persistent_client = PersistentClient.new('test')
  end

  let(:path) {
    'http://localhost:8080'
  }

  describe '#build_request' do

    it 'builts a request object from arguements' do
      example_headers = {'Accept' => 'text/plain'}
      example_body = 'cat=meow'
      actual = @persistent_client.build_request(path, :post, example_headers, example_body)
      expect(actual).to be_a(Net::HTTP::Post)
      expect(actual.body).to eq(example_body)
      actual_headers = actual.instance_variable_get(:@header)
      expect(actual_headers['accept']).to include(example_headers['Accept'])
    end

  end

  describe '#perform_request' do

    it 'raises and Exceptions if the response is not successful' do
      pending("Don't understand this failure - need some ruby-foo")

      connection = @persistent_client.instance_variable_get(:@connection)
      response = Net::HTTPForbidden.new('', 403, 'Forbidden')
      allow(connection).to receive(:request).and_return(response)
      expect{
        @persistent_client.perform_request('')
      }.to raise_error(StandardError, 'Error performing request: Forbidden (403)')
    end

  end

end
