package t::setUp;
use strict;
use warnings;
use local::lib;
use base qw/Exporter/;
use DBI;
use File::Temp qw/tempdir/;
use Test::mysqld;
use Net::EmptyPort qw/empty_port/;

our @EXPORT = qw/setup_mysqld setup_mysqld_slave get_master_info/;

sub setup_mysqld {
  my %opt = @_;
  my $tmpdir = tempdir(CLEANUP=>1);
  my $port = empty_port();
  my %conf = ((
      datadir => $tmpdir,
      port => $port,
      'bind-address' => '127.0.0.1',
      'log-bin' => 'mysql-bin',
      'server-id' => $port,
  ), %opt);

  Test::mysqld->new(
    base_dir => $tmpdir,
    my_cnf => \%conf
  ) or die $Test::mysqld::errstr;
}

sub setup_mysqld_slave {
  my ($master, %opt) = @_;
  my $port = empty_port();
  my $tmpdir = tempdir(CLEANUP=>1);
  my %conf = ((
      datadir => $tmpdir,
      port => $port,
      'bind-address' => '127.0.0.1',
      'server-id' => $port,
  ), %opt);

  my $slave = Test::mysqld->new(
    base_dir => $tmpdir,
    my_cnf => \%conf
  ) or die $Test::mysqld::errstr;

  my $mi = get_master_info($master);
  my $s = DBI->connect($slave->dsn);
  $s->do(<<"EOS");
    CHANGE MASTER TO MASTER_HOST='127.0.0.1', MASTER_PORT=$mi->{MASTER_PORT}, MASTER_USER='root',
    MASTER_LOG_FILE='$mi->{MASTER_LOG_FILE}', MASTER_LOG_POS=$mi->{MASTER_LOG_POS}
EOS
  $s->do('START SLAVE');

  $slave;
}

sub get_master_info {
  my $master = shift;

  my $m = DBI->connect($master->dsn());
  my ($file, $pos) = @{$m->selectrow_hashref('SHOW MASTER STATUS')}{'File', 'Position'};
  $m->disconnect();

  return {
    MASTER_HOST => '127.0.0.1',
    MASTER_USER => 'root',
    MASTER_PORT => $master->my_cnf->{port},
    MASTER_LOG_FILE => $file,
    MASTER_LOG_POS => $pos,
  };
}

1;

