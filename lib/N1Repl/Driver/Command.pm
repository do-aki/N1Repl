package N1Repl::Driver::Command;
use strict;
use warnings;
use utf8;
use IPC::Open2;
use Data::Dumper;

sub new {
  my ($cls, $host, $port, $user, $password) = @_;
  $port = $port || 3306;
  my $self = {mysql_cmd => "mysql --host=$host --port=$port --user=$user --password=$password "};
  return bless $self => $cls;
}


sub show_slave_status {
  my ($self)  = @_;
  my $status= $self->_mysql_cmd('SHOW SLAVE STATUS\G');
  my %ret;
  for my $line (split /\n/, $status) {
    if ($line =~ /\s*(\w+):\s*([^\n]*)$/msx) {
      $ret{$1} = $2;
    }
  }

  return \%ret if (scalar(%ret));
  return;
}

sub do {
  my ($self, $sql)  = @_;
  $self->_mysql_cmd($sql);
}

sub master_pos_wait {
  my ($self, $file, $pos, $timeout) = @_;
  my $ret = $self->_mysql_cmd("SELECT MASTER_POS_WAIT('$file', $pos, $timeout) \\G");
  $ret = [split /\n/, $ret]->[1];
  chomp($ret);
  return $ret;
}


sub _mysql_cmd {
  my ($self, $cmd) = @_;

  open2(\*READER, \*WRITER, $self->{mysql_cmd}) or die 'can not open mysql command';
  binmode READER, ":encoding(utf8)";
  binmode WRITER, ":encoding(utf8)";
  print WRITER "SET CHARSET utf8;\n";
  print WRITER $cmd, "\n";
  close(WRITER);

  my $ret = '';
  $ret .= $_  while(<READER>);
  close(READER);

  return $ret;
}

1;

