#
# START   start master roration
# STOP    stop  master rotation
# SWITCH  switch master 
#
#
package N1Repl::Command;


use strict;
use warnings;
use Carp;

sub new {
  my ($class, $cmd_name, %arg) = @_;

  $arg{_command} = $cmd_name;
  bless \%arg => $class;
}

sub called {
  my $self = shift;
  my $name = shift;

  return 0 unless ($name);


  if ($name eq $self->{_command}) {
    return 1;
  }

  return 0;
}

sub get_param {
  my $self = shift;
  my $name = shift;

  return $self->{$name};
}

sub parse {
  my $str = shift;

  chomp($str);
   my @list = split(/\t/, $str);
  my $cmd = shift(@list);
  my %params = map { split(/=/, $_, 2) } @list;

  return nop() unless ($cmd);

  if ($cmd eq 'START') {
    return new N1Repl::Command($cmd);
  } elsif ($cmd eq 'STOP') {
    return new N1Repl::Command($cmd);
  } elsif ($cmd eq 'SWITCH') {
    return new N1Repl::Command($cmd, %params);
  }

  return nop();
}

sub nop {
  return new N1Repl::Command('NOP')
}

1;

