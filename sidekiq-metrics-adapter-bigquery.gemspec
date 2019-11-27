# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'sidekiq-metrics-adapter-bigquery'
  spec.version       = '0.1.0'
  spec.authors       = %w[iyuuya]
  spec.email         = %w[i.yuuya@gmail.com]

  spec.summary       = %q{BigQuery adapter for sidekiq-metrics}
  spec.description   = %q{BigQuery adapter for sidekiq-metrics}
  spec.homepage      = 'https://github.com/iyuuya/sidekiq-metrics-adapter-bigquery'
  spec.license       = 'MIT'

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['changelog_uri'] = spec.homepage + '/releases'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path('..', __FILE__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']
  spec.required_ruby_version = '>= 2.5'

  spec.add_dependency 'sidekiq', '>= 5.0'
  spec.add_dependency 'sidekiq-metrics', '~> 0.1'
  spec.add_dependency 'google-cloud-bigquery', '~> 1.18'
  spec.add_development_dependency 'bundler', '~> 2.0'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '~> 3.0'
end
