package N1Repl::CommandChecker;

use strict;
use warnings;
use Carp;
use Fcntl ':flock';
use N1Repl::Command;

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
    return N1Repl::Command::nop();
  }

  my $f;
  unless (open($f, '+<', $self->{file})) {
    return N1Repl::Command::nop();
  }

  unless (flock($f, LOCK_EX | LOCK_NB)) {
    close($f);
    return N1Repl::Command::nop();
  }

  my $line = <$f>;
  unless ($line) {
    return N1Repl::Command::nop();
  }
  chomp($line);
  my $cmd = N1Repl::Command::parse($line);

  truncate($f, 0);
  close($f);

  $cmd;
}

1;

