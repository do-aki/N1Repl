use strict;
use warnings;
use Test::More;
use t::setUp;

use SwitchMaster::Config;

my $c = new SwitchMaster::Config();
$c->load('t/02_config/conf.yaml');

my %conf = $c->connect_config;
is_deeply(\%conf, {
  host => '192.168.0.1',
  port => 3307,
  user => 'myuser',
  password => 'pass',
}, 'connect_config');

my $master_conf = $c->master_config;
my $m1 = $master_conf->find('127.0.0.1', 3306);
is_deeply($m1, {
  MASTER_HOST     => '127.0.0.1',
  MASTER_LOG_FILE => 'mysql-bin.000003',
  MASTER_LOG_POS  => 203,
  MASTER_PORT     => 3306,
  MASTER_USER     => 'root',
  DISPLAY_NAME    => '127.0.0.1:3306',
}, 'master1');

my $m2 = $master_conf->find('127.0.0.1', 3307);
is_deeply($m2, {
  MASTER_HOST     => '127.0.0.1',
  MASTER_LOG_FILE => 'mysql-bin.000893',
  MASTER_LOG_POS  => 777,
  MASTER_PORT     => 3307,
  MASTER_USER     => 'root',
  DISPLAY_NAME    => '127.0.0.1:3307',
}, 'master2');


done_testing();

