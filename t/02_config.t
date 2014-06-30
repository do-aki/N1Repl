use strict;
use warnings;
use Test::More;
use t::setUp;

use SwitchMaster;

my $c = new SwitchMaster::Config();
$c->load('t/02_config/conf.yaml');

is($c->get_data_file, 't/02_config/data.yaml', 'data_file');

my %conf = $c->get_connect_config();
is_deeply(\%conf, {
  host => '192.168.0.1',
  port => 3307,
  user => 'myuser',
  password => 'pass',
}, 'connect_config');

done_testing();

