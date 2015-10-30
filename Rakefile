require 'fileutils'

#RIAK_VERSION      = "2.0.2"
#RIAK_DOWNLOAD_URL = "http://s3.amazonaws.com/downloads.basho.com/riak/2.0/#{RIAK_VERSION}/osx/10.8/riak-#{RIAK_VERSION}-OSX-x86_64.tar.gz"
NUM_NODES = 4
NUM_NODES_STR = "4"
#RING_SIZE = 16
# BACKEND = 'leveldb' #options: bitcask, leveldb, memory.

task :default => :help

task :help do
  sh %{rake -T}
end

desc "counters # of errors lines in the dev cluster log"
task :errors do
  sh "cat _build/dev/dev?/dotted_db/log/error.log dev/dev?/log/crash.log| wc -l" rescue "print errors error"
  sh "cat _build/dev/dev1/dotted_db/log/error.log dev/dev1/log/crash.log| wc -l" rescue "print errors error"
  sh "cat _build/dev/dev2/dotted_db/log/error.log dev/dev2/log/crash.log| wc -l" rescue "print errors error"
  sh "cat _build/dev/dev3/dotted_db/log/error.log dev/dev3/log/crash.log| wc -l" rescue "print errors error"
  sh "cat _build/dev/dev4/dotted_db/log/error.log dev/dev4/log/crash.log| wc -l" rescue "print errors error"
end

desc "resets the logs"
task :clean_errors do
  (1..NUM_NODES).each do |n|
    sh %{rm -rf _build/dev/dotted_db/dev#{n}/log/*}
    sh "touch _build/dev/dev#{n}/dotted_db/log/error.log _build/dev/dev#{n}/dotted_db/log/crash.log" rescue "print clean error"
  end
end

desc "attach to a dottedDB console"
task :attach, :node do |t, args|
  args.with_defaults(:node => 1)
  sh %{_build/dev/dev#{args.node}/dotted_db/bin/dotted_db attach} rescue "attach error"
end


desc "make a binary release"
task :rel do
  sh "make rel" rescue "make error"
end

desc "install, start and join dotted_db nodes"
task :dev => [:build, :start, :join, :converge]

desc "compile the dotted_db source"
task :compile do
  sh "make compile-no-deps" rescue "make error"
end

desc "compile everything"
task :all do
  sh "make all" rescue "make error"
end

desc "make the dev dotted_db folders"
task :build => :clear do
  sh "make devrel" rescue "build dev error"
end

desc "start all dotted_db nodes"
task :start do
  # (1..NUM_NODES).each do |n|
  #   sh %{dev/dev#{n}/bin/dotted_db start}
  # end
  sh "for d in _build/dev/dev*; do $d/dotted_db/bin/dotted_db start; done" rescue "not running"
  puts "========================================"
  puts "Dotted Dev Cluster started"
  puts "========================================"
end

desc "join dotted_db nodes (only needed once)"
task :join do
  sleep(2)
  (2..NUM_NODES).each do |n|
      sh %{_build/dev/dev#{n}/dotted_db/bin/dotted_db-admin cluster join dotted_db1@127.0.0.1} rescue "already joined"
  end
  sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin cluster plan}
  sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin cluster commit}
end

desc "waits for cluster vnode converge to stabilize"
task :converge do
  puts "waiting for cluster vnode reshuffling to converge"
  $stdout.sync = true
  cmd = `_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status | grep "\ \-\-" | wc -l`
  # cmd = `dev/dev1/bin/dotted_db-admin member-status | grep "\ \-\-" | wc -l`
  counter = 1
  tries = 0
  continue = true
  while (cmd.strip != NUM_NODES_STR and continue)
    print "."
    sleep(1)
    cmd = `_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status | grep "\ \-\-" | wc -l`
    counter = counter + 1
    if counter > 5
      tries = tries + 1
      puts ""
      puts "Try # #{tries} of 20"
      sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status}
      counter = 1
    end
    if tries > 39
      continue = false
    end
  end
  sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status}
  if continue
    puts "READY SET GO!"
  else
    puts "Cluster is not converging :("
  end
end

desc "dotted_db-admin member-status"
task :member_status do
  sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status}
end

desc "stop all dotted_db nodes"
task :stop do
  # (1..NUM_NODES).each do |n|
  #   sh %{dev/dev#{n}/bin/dotted_db stop} rescue "not running"
  # end
  sh "for d in _build/dev/dev*; do $d/dotted_db/bin/dotted_db stop; done" rescue "not running"
  puts "========================================"
  puts "Dotted Dev Cluster stopped"
  puts "========================================"
end

desc "restart all dotted_db nodes"
task :restart => [:stop, :compile, :delete_storage, :clean_errors, :start, :errors, :attach]

desc "restart all dotted_db nodes"
task :restart_with_storage => [:stop, :compile, :start]

desc "clear data from all dotted_db nodes"
  task :clear => :stop do
    (1..NUM_NODES).each do |n|
      sh %{rm -rf _build/dev/dev#{n}}
  end
end

desc "ping all dotted_db nodes"
task :ping do
  (1..NUM_NODES).each do |n|
      sh %{_build/dev/dev#{n}/dotted_db/bin/dotted_db ping}
  end
end

desc "dotted_db-admin test"
task :test do
  (1..NUM_NODES).each do |n|
    sh %{_build/dev/dev#{n}/dotted_db/bin/dotted_db-admin test}
  end
end

desc "dotted_db-admin status"
task :status do
  sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin  status}
end

desc "dotted_db-admin ring-status"
task :ring_status do
  sh %{_build/dev/dev1/dotted_db/bin/dotted_db-admin  ring-status}
end

desc "plot local dev nodes stats"
task :local_plot do
  sh %{python benchmarks/plot.py}
end

desc "deletes the database storage to start from scratch"
task :delete_storage do
  (1..NUM_NODES).each do |n|
    sh %{rm -rf _build/dev/dev#{n}/dotted_db/data/vnode_state}
    sh %{rm -rf _build/dev/dev#{n}/dotted_db/data/objects}
    # sh %{rm -rf dev/dev#{n}/log}
  end
end

# task :copy_riak do
#   (1..NUM_NODES).each do |n|
#     system %{cp -nr riak-#{RIAK_VERSION}/ riak#{n}}
#    system %(sed -i '' 's/riak@127.0.0.1/riak#{n}@127.0.0.1/' riak#{n}/etc/riak.conf)
#    system %(sed -i '' 's/127.0.0.1:8098/127.0.0.1:1#{n}098/' riak#{n}/etc/riak.conf)
#    system %(sed -i '' 's/127.0.0.1:8087/127.0.0.1:1#{n}087/' riak#{n}/etc/riak.conf)
#    system %(echo 'riak_control = on' >> riak#{n}/etc/riak.conf)
#    system %(echo 'handoff.port = 1#{n}099' >> riak#{n}/etc/riak.conf)
#     system %(echo 'ring_size = #{RING_SIZE}' >> riak#{n}/etc/riak.conf)
#    system %(echo 'storage_backend = #{BACKEND}' >> riak#{n}/etc/riak.conf)
#   end
# end
