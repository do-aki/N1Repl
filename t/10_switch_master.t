use strict;
use warnings;
use File::Temp qw/tempfile/;
use Test::More;
use DBI;
use Test::SharedFork;
use t::setUp;
use YAML::Tiny;

use SwitchMaster::Controller;

my $master1 = setup_mysqld();
my $master2 = setup_mysqld();

my $err_file = [tempfile()]->[1];
print "error-file is $err_file\n";
my $slave = setup_mysqld_slave($master1, 'log-error'=>$err_file);
#my $s = DBI->connect($slave->dsn);
#$s->do('STOP SLAVE');
#$s->disconnect();

my $mi1 = get_master_info($master1);
my $mi2 = get_master_info($master2);


my $data_file = [tempfile()]->[1];
YAML::Tiny::DumpFile($data_file, [$mi1, $mi2]);
my $master_config = new SwitchMaster::MasterConfig($data_file);

my $cmd_file = [tempfile()]->[1];
my $command_checker = new SwitchMaster::CommandChecker($cmd_file);

my $pid = fork();
if ($pid == 0) {
  Test::SharedFork->child;

  my $c = new SwitchMaster::Config();
  $c->master_config($master_config);
  $c->command_checker($command_checker);
  $c->{SWITCH_WAIT} = 1;

  my $sm = new SwitchMaster::Controller(driver=>'DBI', config=>$c);
  $sm->connect(MYSQL_HOST=>'127.0.0.1', MYSQL_PORT=>$slave->my_cnf->{port}, MYSQL_USER=>'root');
  $sm->run();
} elsif (0 < $pid) {
  Test::SharedFork->parent;

  my $dbm1 = DBI->connect($master1->dsn);
  my $dbm2 = DBI->connect($master2->dsn);
  $dbm1->do('CREATE TABLE master1 (id int)');
  $dbm2->do('CREATE TABLE master2 (id int)');
  for (my $i=0;$i<10;++$i) {
    $dbm1->do("INSERT INTO master1(id) VALUES($i)");
    $dbm2->do("INSERT INTO master2(id) VALUES($i)");
  }

  sleep(5);

  my $db_s = DBI->connect($slave->dsn);
  my $tbls = $db_s->selectcol_arrayref('SHOW TABLES');

  ok(grep {$_ eq 'master1'} @{$tbls} , 'TABLE master1 exists');
  ok(grep {$_ eq 'master2'} @{$tbls} , 'TABLE master2 exists');

  my $from_m1 = $db_s->selectall_arrayref('SELECT id FROM master1');
  is_deeply(map {$_[0]} $from_m1, [0..9] , 'master1 rows');

  my $from_m2 = $db_s->selectall_arrayref('SELECT id FROM master2');
  is_deeply(map {$_[0]} $from_m2, [0..9], 'master2 rows');


  kill 'HUP' => $pid;
  done_testing;
} else {
  die "failed fork: $!";
}


