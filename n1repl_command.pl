#!/usr/bin/env perl

use strict;
use warnings;
use utf8;
use FindBin;
use lib "$FindBin::Bin/extlib/lib/perl5";
use lib "$FindBin::Bin/lib";
use Getopt::Long;
use Pod::Usage;
use Data::Dumper;
use SwitchMaster::Config;
use SwitchMaster::CommandPublisher;

++$|; # auto flush

my $daemonize = 0;
my $config_file;
my $command;
my %params;
sub command_options {
  my ($name, $value) = @_;
  $params{$name} = $value;
}
Getopt::Long::Configure('no_ignore_case', 'gnu_compat');
my $r = GetOptions(
  'config=s' => \$config_file,
  'h|help'   => \my $help,
  '<>'       => sub { $command = shift },

  'orig_master_host=s'     => \&command_options,
  'orig_master_port=i'     => \&command_options,
  'orig_master_log_file=s' => \&command_options,
  'orig_master_log_pos=i'  => \&command_options,
  'new_master_host=s'      => \&command_options,
  'new_master_port=i'      => \&command_options,
  'new_master_log_file=s'  => \&command_options,
  'new_master_log_pos=i'   => \&command_options,
);

if ($help || !$config_file || !$command) {
  pod2usage(-verbose=>2, -exitval=>0);
}


my $config = SwitchMaster::Config->new()->load($config_file);
my $publisher = $config->command_publisher;

my $cmd = $publisher->make_command(uc($command));
unless ($cmd) {
  print STDERR "unknown command $command\n";
  exit(1);
}

unless ($publisher->publish($cmd, %params)) {
  print "failed\n";
  exit(2);
}



__END__

=head1 NAME

    switch_master_command.pl

=head1 SYNOPSIS

    % switch_master_command.pl --config=sm.yaml start 

=head1 DESCRIPTION

    master n : slave 1 replication manager for mysql

=head1 OPTIONS

=over 4

=item -h --help

    Display help

=back

=head1 AUTHOR

    do_aki

=head1 LICENSE

    This program is free software; you can redistribute it and/or modify
    it under the same terms as Perl itself.

