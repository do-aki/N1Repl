package SwitchMaster::DBI;
use strict;
use warnings;
use utf8;
use IPC::Open2;
use FindBin;
use Data::Dumper;
use lib $FindBin::Bin . '/lib';
use YAML::Tiny;
use DBI;

sub new {
  my ($cls, $host, $user, $password) = @_;
  my $dbh = DBI->connect("DBI:mysql:mysql:$host", $user, $password) or die 'cannot connect';
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