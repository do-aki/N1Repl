package SwitchMaster::DBI;
use strict;
use warnings;
use utf8;
use Data::Dumper;
use DBI;

sub new {
  my ($cls, $host, $port, $user, $password) = @_;
  $port = $port || 3306;
  my $dbh = DBI->connect("DBI:mysql:mysql:$host;port=$port", $user, $password, {
    ShowErrorStatement => 1,
    mysql_enable_utf8 => 1,
    mysql_auto_reconnect => 1,
  }) or die 'cannot connect';
  return bless {dbh => $dbh} => $cls
}

sub show_slave_status {
  my ($self)  = @_;
  return $self->{dbh}->selectrow_hashref('SHOW SLAVE STATUS');
}

sub do {
  my ($self, $sql)  = @_;
  $self->{dbh}->do($sql);
}

sub master_pos_wait {
  my ($self, $file, $pos, $timeout) = @_;
  return $self->{dbh}->selectcol_arrayref(
    "SELECT MASTER_POS_WAIT('$file', $pos, $timeout)"
  )->[0];
}

1;

