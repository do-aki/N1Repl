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
use SwitchMaster::Controller;

++$|; # auto flush

my $daemonize = 0;
my $config_file;
Getopt::Long::Configure ("no_ignore_case");
GetOptions(
  'config=s' => \$config_file,
  'daemon' => \$daemonize,
  'h|help' => \my $help,
);


if ($help || !$config_file) {
  pod2usage(-verbose=>2, -exitval=>0);
}

my $config = SwitchMaster::Config->new()->load($config_file);
my $sm = new SwitchMaster::Controller(driver=>'DBI', config => $config);
$sm->connect();
$sm->run();

__END__

=head1 NAME

    switch_master.pl

=head1 SYNOPSIS

    % switch_master.pl --daemon

=head1 DESCRIPTION

    master n : slave 1 replication manager for mysql

=head1 INSTALL

    % perl cpanm --installdeps .

    or

    % perl cpanm -L extlib --installdeps .

=head1 OPTIONS

=over 4

=item --daemon

    No Implemented

=item -h --help

    Display help

=back

=head1 AUTHOR

    do_aki

=head1 LICENSE

    This program is free software; you can redistribute it and/or modify
    it under the same terms as Perl itself.

