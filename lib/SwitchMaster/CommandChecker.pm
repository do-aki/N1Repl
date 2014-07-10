package SwitchMaster::CommandChecker;

use strict;
use warnings;
use Carp;
use Fcntl ':flock';
use SwitchMaster::Command;

sub new {
  my $class = shift;
  my $file = shift;

  bless {
    file    => $file,
  } => $class;
}

sub fetch {
  my $self = shift;

  unless (-f $self->{file}) {
    return SwitchMaster::Command::nop();
  }

  my $f;
  unless (open($f, '+<', $self->{file})) {
    return SwitchMaster::Command::nop();
  }

  unless (flock($f, LOCK_EX | LOCK_NB)) {
    close($f);
    return SwitchMaster::Command::nop();
  }

  my $line = <$f>;
  unless ($line) {
    return SwitchMaster::Command::nop();
  }
  chomp($line);
  my $cmd = SwitchMaster::Command::parse($line);

  truncate($f, 0);
  close($f);

  $cmd;
}

1;

