package N1Repl::CommandPublisher;

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

sub make_command {
  my $self = shift;
  my $command = shift;

  if ($command eq 'START') {
    return \&cmd_start;
  } elsif ($command eq 'STOP') {
    return \&cmd_stop;
  } elsif ($command eq 'SWITCH') {
    return \&cmd_switch;
  }
}

sub cmd_start {
  "START";
}

sub cmd_stop {
  "STOP";
}

sub cmd_switch {
  my (%params) = @_;

  my @keys = qw/
    orig_master_host orig_master_port orig_master_log_file orig_master_log_pos
    new_master_host  new_master_port  new_master_log_file  new_master_log_pos/;

  for my $k (@keys) {
    Carp::croak("missing mandatory parameter $k") unless $params{$k};
  }

  join("\t", map {"$_=$params{$_}"} @keys);
}

sub publish {
  my ($self, $cmd_code, %params) = @_;
  publish_to_file($self->{file}, $cmd_code->(%params));
}

sub publish_to_file {
  my ($file, $cmd_data) = @_;

  if (-s $file) {
    return;
  }

  open(my $f, '+>>', $file) or return;

  unless (flock($f, LOCK_EX | LOCK_NB)) {
    close($f);
    return;
  }

  truncate($f, 0);
  print $f $cmd_data, "\n";
  close($f);

  return 1;
}

1;

