# frozen_string_literal: true

require 'spec_helper'

RSpec.describe Sidekiq::Metrics::Adapter::Bigquery do
  describe '.new(dataset, table, async:, sidekiq_worker_options:)' do
    it 'excludes own worker with configure' do
      described_class.new(:dummy_dataset, :dummy_table)
      expect(Sidekiq::Metrics.configuration.excludes).to be_include 'Sidekiq::Metrics::Adapter::Bigquery::Worker'
    end

    it 'set worker options of own worker' do
      described_class.new(:dummy_dataset, :dummy_table, sidekiq_worker_options: { retry: 100, queue: :bigquery })

      expect(Sidekiq::Metrics::Adapter::Bigquery::Worker.sidekiq_options_hash['retry']).to eq 100
      expect(Sidekiq::Metrics::Adapter::Bigquery::Worker.sidekiq_options_hash['queue']).to eq :bigquery
    end
  end

  describe '#write(worker_status)' do
    context 'when async = true' do
      it 'is expected to receive own worker.perform_async with worker_status' do
        adapter = described_class.new(:dummy_dataset, :dummy_table, async: true, sidekiq_worker_options: { retry: 100, queue: :bigquery })
        worker_status = {}
        expect(Sidekiq::Metrics::Adapter::Bigquery::Worker).to receive(:perform_async).with(worker_status)
        adapter.write(worker_status)
      end
    end

    context 'when async = false' do
      it 'is expected to receive own worker.perform_async with worker_status' do
        adapter = described_class.new(:dummy_dataset, :dummy_table, async: false, sidekiq_worker_options: { retry: 100, queue: :bigquery })
        worker_status = {}
        expect(Sidekiq::Metrics::Adapter::Bigquery::Worker).not_to receive(:perform_async).with(worker_status)
        expect_any_instance_of(Sidekiq::Metrics::Adapter::Bigquery::Worker).to receive(:perform).with(worker_status)
        adapter.write(worker_status)
      end
    end
  end

  describe '#table(suffix)' do
    it 'is expected to receive table' do
      dataset = double(:dataset)
      expect(dataset).to receive(:table).and_return(:dummy_table)
      adapter = described_class.new(dataset, :dummy_table, async: true, sidekiq_worker_options: { retry: 100, queue: :bigquery })
      adapter.table
    end
  end
end

RSpec.describe Sidekiq::Metrics::Adapter::Bigquery::Worker do
  describe '#perform(worker_status)' do
    it 'insert worker status to table of bigquery' do
      Sidekiq::Metrics.configure do |config|
        table = Google::Cloud::Bigquery::Table.new
        insert_errors = double(:insert_errors)
        allow(insert_errors).to receive(:insert_errors).and_return([])
        expect(table).to receive(:insert).and_return(Google::Cloud::Bigquery::InsertResponse.new(
          [],
          insert_errors
        ))

        adapter = double(:adapter)
        allow(adapter).to receive(:table).and_return(table)

        config.adapter = adapter
      end

      described_class.new.perform({
        'enqueued_at' => Time.now.to_i
      })
    end
  end
end
