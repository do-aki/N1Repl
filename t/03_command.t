use strict;
use warnings;
use Test::More;
use t::setUp;

use N1Repl::Command;

sub parse {
  my $line = shift;
  N1Repl::Command::parse($line);
}

my $cmd1 = parse("START\n");
ok($cmd1->called('START'), 'call START');

my $cmd2 = parse("STOP\n");
ok($cmd2->called('STOP'), 'call STOP');

my $cmd3 = parse(
  "SWITCH\torig_master_host=m1\torig_master_port=3306\torig_master_log_file=file1\torig_master_log_pos=290"
  . "\tnew_master_host=m2\tnew_master_port=3307\tnew_master_log_file=file2\tnew_master_log_pos=1129\n"
);
ok($cmd3->called('SWITCH'), 'call SWITCH');

is($cmd3->get_param('orig_master_host'),     'm1',    'orig_master_host');
is($cmd3->get_param('orig_master_port'),     3306,    'orig_master_port');
is($cmd3->get_param('orig_master_log_file'), 'file1', 'orig_master_log_file');
is($cmd3->get_param('orig_master_log_pos'),  290,     'orig_master_log_pos');
is($cmd3->get_param('new_master_host'),      'm2',    'new_master_host');
is($cmd3->get_param('new_master_port'),      3307,    'new_master_port');
is($cmd3->get_param('new_master_log_file'),  'file2', 'new_master_log_file');
is($cmd3->get_param('new_master_log_pos'),   1129,    'new_master_log_pos');



my $cmd_nop = parse('***INVALID COMMAND***');
ok($cmd_nop->called('NOP'));


done_testing();

