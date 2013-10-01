use strict;
use warnings;
use File::Temp qw/tempfile/;
use Test::More;
use DBI;
use Test::SharedFork;
use t::setUp;
use YAML::Tiny;

use SwitchMaster;

my $master1 = setup_mysqld();
my $master2 = setup_mysqld();

my $err_file = [tempfile()]->[1];
print "error-file is $err_file\n";
my $slave = setup_mysqld_slave($master1, 'log-error'=>$err_file);

my $mi1 = get_master_info($master1);
my $mi2 = get_master_info($master2);


my $data_file = [tempfile()]->[1];
YAML::Tiny::DumpFile($data_file, [$mi1, $mi2]);

my $pid = fork();
if ($pid == 0) {
  Test::SharedFork->child;

  my $c = new SwitchMaster::Config(
    data_file => $data_file,
  );
  $c->load();
  $c->{SWITCH_WAIT} = 1;
  
  
  my $sm = new SwitchMaster(driver=>'DBI', config=>$c);
  $sm->connect(host=>'127.0.0.1', port=>$slave->my_cnf->{port}, user=>'root');

  $sm->run();
} elsif (0 < $pid) {
  Test::SharedFork->parent;

  my $dbm1 = DBI->connect($master1->dsn);
  my $dbm2 = DBI->connect($master2->dsn);
  my $dbs = DBI->connect($slave->dsn);
  $dbm1->do('CREATE TABLE master1 (id int)');
  $dbm2->do('CREATE TABLE master2 (id int)');

  sleep(5);

  my $tbls = $dbs->selectcol_arrayref('SHOW TABLES');

  ok(grep {$_ eq 'master1'} @{$tbls} , 'TABLE master1 exists');
  ok(grep {$_ eq 'master2'} @{$tbls} , 'TABLE master2 exists');

  kill 'HUP' => $pid;
  done_testing;
} else {
  die "failed fork: $!";
}





