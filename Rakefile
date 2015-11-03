require 'fileutils'

#RIAK_VERSION      = "2.0.2"
#RIAK_DOWNLOAD_URL = "http://s3.amazonaws.com/downloads.basho.com/riak/2.0/#{RIAK_VERSION}/osx/10.8/riak-#{RIAK_VERSION}-OSX-x86_64.tar.gz"
#RING_SIZE = 16
# BACKEND = 'leveldb' #options: bitcask, leveldb, memory.
NUM_NODES = 4
NUM_NODES_STR = "4\n"

task :default => :help

task :help do
  sh %{rake -T}
end

desc "counters # of errors lines in the dev cluster log"
task :list_errors do
  print yellow2
  print "Total: " + `cat _build/dev/dev?/dotted_db/log/error.log _build/dev/dev?/dotted_db/log/crash.log | wc -l`
  (1..NUM_NODES).each do |n|
    print "Dev#{n}:  " + `cat _build/dev/dev#{n}/dotted_db/log/error.log _build/dev/dev#{n}/dotted_db/log/crash.log | wc -l`
  end
  print reset_color
end

desc "resets the logs"
task :clean_errors do
  (1..NUM_NODES).each do |n|
    `rm -rf _build/dev/dev#{n}/dotted_db/log/*`
    `touch _build/dev/dev#{n}/dotted_db/log/error.log _build/dev/dev#{n}/dotted_db/log/crash.log`
  end
  puts green " ========> Cleaned logs!             "
end

desc "attach to a dottedDB console"
task :attach, :node do |t, args|
  args.with_defaults(:node => 1)
  sh %{_build/dev/dev#{args.node}/dotted_db/bin/dotted_db attach}
end

desc "Make a release"
task :rel do
  puts green "           Making a Release!           "
  sh "./rebar3 release -d false --overlay_vars config/vars.config" rescue "release error"
end

desc "install, start and join dotted_db nodes"
task :dev => [:build_dev, :start, :join, :converge]

desc "compile the dotted_db source"
task :compile do
  puts green " ========> Compiling!               "
  print `./rebar3 compile`
end

desc "Make the dev dotted_db folders"
task :build_dev => :clean do
  (1..NUM_NODES).each do |n|
    print yellow %x<mkdir -p _build/dev/dev#{n}>
    print yellow %x<config/gen_dev dev#{n} config/vars/dev_vars.config.src config/vars/dev#{n}_vars.config>
    print yellow %x<./rebar3 release -o _build/dev/dev#{n} --overlay_vars config/vars/dev#{n}_vars.config>
  end
end

desc "start all dotted_db nodes"
task :start do
  print yellow `for d in _build/dev/dev*; do $d/dotted_db/bin/dotted_db start; done`
  # (1..NUM_NODES).each do |n|
  #   sh %{_build/dev/dev#{n}/dotted_db/bin/dotted_db start} rescue "no dev folders"
  # end
  puts green " ========> Dev Cluster Started!           "
end

desc "stop all dotted_db nodes"
task :stop do
  print yellow `for d in _build/dev/dev*; do $d/dotted_db/bin/dotted_db stop; done`
  # (1..NUM_NODES).each do |n|
  #   sh %{_build/dev/dev#{n}/dotted_db/bin/dotted_db stop} rescue "no dev folders"
  # end
  puts green " ========> Dev Cluster Stopped!           "
end

desc "join dotted_db nodes (only needed once)"
task :join do
  sleep(2)
  (2..NUM_NODES).each do |n|
      print yellow `_build/dev/dev#{n}/dotted_db/bin/dotted_db-admin cluster join dotted_db1@127.0.0.1`
  end
  puts bg_green "        Dev Cluster Joined!           "
  print yellow `_build/dev/dev1/dotted_db/bin/dotted_db-admin cluster plan`
  print yellow `_build/dev/dev1/dotted_db/bin/dotted_db-admin cluster commit`
  puts bg_green "        Dev Cluster Committed!           "
end

desc "waits for cluster vnode converge to stabilize"
task :converge do
  puts bg_yellow "   Waiting for cluster vnode reshuffling to converge   "
  $stdout.sync = true
  counter = 1
  tries = 0
  continue = true
  while (`_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status | grep "\ \-\-" | wc -l | xargs` != NUM_NODES_STR and continue)
    print "."
    sleep(0)
    counter = counter + 1
    if counter > 4
      tries = tries + 1
      puts ""
      puts "Try # #{tries} of 20"
      puts yellow `_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status`
      counter = 1
    end
    if tries > 30
      continue = false
    end
  end
  puts yellow `_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status`
  if continue
    puts bg_green "                                            "
    puts bg_green "               READY SET GO!                "
    puts bg_green "                                            "
  else
    puts bg_red "                                            "
    puts bg_red "         Cluster is not converging :(       "
    puts bg_red "                                            "
  end
end

desc "dotted_db-admin member-status"
task :member_status do
  puts yellow `_build/dev/dev1/dotted_db/bin/dotted_db-admin member-status`
end

desc "restart all dotted_db nodes"
task :restart => [:stop, :compile, :delete_storage, :clean_errors, :start, :list_errors, :attach]

desc "restart all dotted_db nodes"
task :restart_with_storage => [:stop, :compile, :start]

desc "clean data from all dotted_db nodes"
  task :clean => :stop do
    (1..NUM_NODES).each do |n|
      `rm -rf _build/dev/dev#{n}`
  end
  puts green " ========> Dev Cluster Cleaned!           "
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
    print yellow %x<rm -rf _build/dev/dev#{n}/dotted_db/data/vnode_state>
    print yellow %x<rm -rf _build/dev/dev#{n}/dotted_db/data/objects>
    # sh %{rm -rf dev/dev#{n}/log}
  end
  puts green " ========> Dev Storage Deleted!           "
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

def colorize(text, color_code)
  "\033[#{color_code}m#{text}\033[0m"
end

{
  :black    => 30,
  :red      => 31,
  :green    => 32,
  :yellow   => 33,
  :blue     => 34,
  :magenta  => 35,
  :cyan     => 36,
  :white    => 37
}.each do |key, color_code|
  define_method key do |text|
    colorize(text, color_code)
  end
end

def colorize_bg(text, color_code)
  if color_code == 41 ## red bg in white fg
    "\033[37;#{color_code}m\033[1m#{text}\033[0m"
  else
    "\033[30;#{color_code}m\033[1m#{text}\033[0m"
  end
end

{
  :bg_black    => 40,
  :bg_red      => 41,
  :bg_green    => 42,
  :bg_yellow   => 43,
  :bg_blue     => 44,
  :bg_magenta  => 45,
  :bg_cyan     => 46,
  :bg_white    => 47
}.each do |key, color_code|
  define_method key do |text|
    colorize_bg(text, color_code)
  end
end


def colorize2(color_code)
  "\033[#{color_code}m"
end

{
  :black2    => 30,
  :red2      => 31,
  :green2    => 32,
  :yellow2   => 33,
  :blue2     => 34,
  :magenta2  => 35,
  :cyan2     => 36,
  :white2    => 37
}.each do |key, color_code|
  define_method key do
    colorize2(color_code)
  end
end

def reset_color()
  "\033[0m"
end
