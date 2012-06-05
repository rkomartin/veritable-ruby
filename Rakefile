#!/usr/bin/env rake
require "bundler/gem_tasks"
require 'rake/testtask'
require 'rdoc/task'

task :default => [:test]

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList['test/test*.rb']
  t.verbose = true
end

Rake::RDocTask.new do |rdoc|
  files = ['README.md', 'LICENSE', 'lib/**/*.rb', 'doc/**/*.rdoc']
  rdoc.rdoc_files.add(files)
  rdoc.main = 'README.md'
  rdoc.title = 'veritable-ruby Documentation'
  rdoc.rdoc_dir = 'doc'
  rdoc.options << '--line-numbers'
end
