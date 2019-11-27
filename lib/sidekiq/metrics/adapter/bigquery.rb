# frozen_string_literal: true

require 'sidekiq/worker'
require 'sidekiq/metrics/adapter/base'

module Sidekiq
  module Metrics
    module Adapter
      class Bigquery < Base
        class Worker
          class InsertError < StandardError; end

          include Sidekiq::Worker

          def perform(worker_status)
            result = @table.insert([worker_status])

            error = result.insert_error_for(worker_staus)
            raise InsertError, error.errors.to_json if error
          end
        end

        # @param [Google::Cloud::Bigquery] bigquery
        # @param [String] dataset
        # @param [String] table
        # @param [Hash] mapping
        # @param [boolean] async
        # @param [Hash] sidekiq_worker_options
        def initialize(bigquery,
                       dataset,
                       table = nil,
                       mapping: {
                         status: 'status',
                         queue: 'queue',
                         class: 'class',
                         jid: 'jid',
                         enqueued_at: 'enqueued_at',
                         started_at: 'started_at',
                         finished_at: 'finished_at'
                       },
                       async: true,
                       sidekiq_worker_options: {
                         queue: :default,
                         retry: 5
                       })
          @bigquery = bigquery
          # TODO: check dataset and table
          # TODO: table name with suffix
          @table = @bigquery.dataset(dataset).table(table)
          @mapping = mapping
          Worker.sidekiq_options(sidekiq_worker_options)
        end

        def write(worker_status)
          Worker.perform_async(worker_status)
        end

        private

        def mapped_row(worker_status)
          {}.tap do |row|
            @mapping.each do |from, to|
              row[to] = worker_status[form]
            end
          end
        end
      end
    end
  end
end
