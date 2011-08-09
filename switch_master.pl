use strict;
use warnings;
use utf8;
use Data::Dumper;
use FindBin;
use lib $FindBin::Bin . '/lib';
use SwitchMaster::DBI;
use SwitchMaster::Command;

require 'common.pl';

++$|; # buffer off

my $DATA_FILE = $FindBin::Bin . '/data/settings.yaml';

our $MYSQL_HOST;
our $MYSQL_USER;
our $MYSQL_PASSWORD;

our $SWITCH_WAIT;
our $CONNECTING_WAIT;
our $CATCHUP_WAIT;
our $MASTER_POS_WAIT;
our $RESTART_IO_THREAD_WAIT;

my $exit_flag = 0;
hook_signals([qw/INT TERM QUIT HUP PIPE/], sub {
  logging('debug', "receive exit request.") unless($exit_flag);
  $exit_flag = 1;
});

load_settings();

my $db = new SwitchMaster::Command($MYSQL_HOST, $MYSQL_USER, $MYSQL_PASSWORD);

while(1) {

  # マスタを切り替えた後に、接続がすぐにはできないことがある
  my $status0 = $db->show_slave_status();
  if ('Connecting' eq $status0->{Slave_IO_Running}) {
    logging('debug', "Slave_IO_Running status is Connecting. wait $CONNECTING_WAIT sec");
    sleep $CONNECTING_WAIT;
    next;
  }

  # 何かしらのエラーがあったら停止
  if (!$status0->{Slave_IO_State} # any error?
      || 'Yes' ne $status0->{Slave_SQL_Running} || 'Yes' ne $status0->{Slave_IO_Running}
      || 0 != $status0->{Last_IO_Errno} || 0 != $status0->{Last_SQL_Errno} ) {
    print Dumper $status0;
    die ('slave stopped');
  }


  if ($exit_flag) {
    logging('info', "exit.");
    exit(0);
  }

  if (0 < ($status0->{Seconds_Behind_Master} or 0)) {
    logging('debug', "Seconds_Behind_Master is $status0->{Seconds_Behind_Master}. wait $CATCHUP_WAIT sec");
    sleep $CATCHUP_WAIT;
    next;
  }

  my $current_master = $status0->{Master_Host};
  if (!get_master_config($current_master)) {
    die("${current_master} is not defined");
  }
  
  
  my $status1;
  my $is_stop_io_thread;
  do {
    $is_stop_io_thread = 1;
    $db->do('STOP SLAVE IO_THREAD');
    $status1 = $db->show_slave_status();
    #print    "SELECT MASTER_POS_WAIT('$status1->{Master_Log_File}', $status1->{Read_Master_Log_Pos})\n";
    my $mpw = $db->master_pos_wait($status1->{Master_Log_File}, $status1->{Read_Master_Log_Pos}, $MASTER_POS_WAIT);
  
    if (-1 == $mpw) {
      # 稀に、トランザクション途中で IO が止まってしまうことがある。
      # その場合は、一度 IO_THREAD を再開し、適当なところで再度止める
      $is_stop_io_thread = 0;
      $db->do('START SLAVE IO_THREAD');
      
      my $s = $db->show_slave_status();
      logging('debug', "Read_Master_Log_Pos is $s->{Read_Master_Log_Pos}, bad Exec_Master_Log_Pos is $s->{Exec_Master_Log_Pos}.");
      logging('info',  "timeout MASTER_POS_WAIT. restart io_thead and wait $RESTART_IO_THREAD_WAIT sec");
      sleep($RESTART_IO_THREAD_WAIT);
    }
  } while(0 == $is_stop_io_thread);

  $db->do('STOP SLAVE');
  #my $status2 = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
  #print $status1->{Master_Log_File},':',$status1->{Read_Master_Log_Pos},' - ', $status1->{Exec_Master_Log_Pos}, "\n";
  #print $status2->{Master_Log_File},':',$status2->{Read_Master_Log_Pos},' - ', $status2->{Exec_Master_Log_Pos}, "\n";

  my $next_master = get_next_master_config($current_master);
  $db->do(make_change_master_to($next_master));
  
  set_master_config($current_master, 'MASTER_LOG_FILE', $status1->{Master_Log_File});
  set_master_config($current_master, 'MASTER_LOG_POS', $status1->{Read_Master_Log_Pos});
#  set_master_config($current_master, 'RELAY_LOG_FILE', $status2->{Relay_Log_File});
#  set_master_config($current_master, 'RELAY_LOG_POS', $status2->{Relay_Log_Pos});
  save_settings();
  
  logging('info', "save host $current_master $status1->{Master_Log_File} : $status1->{Read_Master_Log_Pos} and change host to $next_master->{MASTER_HOST}");

  $db->do('START SLAVE');
  sleep $SWITCH_WAIT;
}
