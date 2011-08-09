use strict;
use warnings;
use utf8;
use IPC::Open2;
use FindBin;
use Data::Dumper;
use lib $FindBin::Bin . '/lib';
use YAML::Tiny;
use DBI;

++$|; # buffer off

my $MYSQL_USER     = 'user';
my $MYSQL_PASSWORD = 'password';
my $DATA_FILE = $FindBin::Bin . '/data/settings.yaml';

my $SWITCH_WAIT            = 2;   # 
my $CONNECTING_WAIT        = 1;   # 
my $CATCHUP_WAIT           = 1;   # Seconds_Behind_Master が 0 でないときに待つ秒 
my $MASTER_POS_WAIT        = 10;  # MASTER_POS_WAIT の 待つ秒
my $RESTART_IO_THREAD_WAIT = 3;   # 

my @CONFIG_MASTERS;

sub load_settings {
  my $dat = YAML::Tiny::LoadFile($DATA_FILE);
  @CONFIG_MASTERS = @{$dat->{masters}};
}

sub save_settings {
  YAML::Tiny::DumpFile($DATA_FILE, {masters => \@CONFIG_MASTERS});
}

sub get_master_config {
  my ($host,$name) = @_;
  for my $m (@CONFIG_MASTERS) {
    if ($host eq $m->{MASTER_HOST}) {
      return (defined $name)? $m->{$name} : $m;
    }
  }
  return undef;
}
sub get_next_master_config {
  my ($host) = @_;
  my $flag = 0;
  for my $m (@CONFIG_MASTERS) {
    return $m if ($flag);
    $flag = 1 if ($host eq $m->{MASTER_HOST});
  }
  return $CONFIG_MASTERS[0];
}
sub set_master_config {
  my ($host, $name, $value) = @_;
  my $conf = get_master_config($host);
  return if (!$conf);
  $conf->{$name} = $value;
}


sub make_change_master_to {
  my $args = shift;
  
  my $raw = sub{ $_[0] };
  my $bool = sub{ $_[0]?1:0 };
  my $quote = sub { qq{'$_[0]'} };
  
  my %params = (
    MASTER_HOST => $quote,
    MASTER_USER => $quote,
    MASTER_PASSWORD => $quote,
    MASTER_PORT => $raw,
    MASTER_CONNECT_RETRY => $raw,
    MASTER_LOG_FILE => $quote,
    MASTER_LOG_POS => $raw,
    RELAY_LOG_FILE => $quote,
    RELAY_LOG_POS => $raw,
    MASTER_SSL => $bool,
    MASTER_SSL_CA => $quote,
    MASTER_SSL_CAPATH => $quote,
    MASTER_SSL_CERT => $quote,
    MASTER_SSL_KEY => $quote,
    MASTER_SSL_CIPHER => $quote,
  );
  
  return 'CHANGE MASTER TO ' . join ','
  , map { $_.'='.$params{$_}($args->{$_}); } 
    grep { exists $params{$_} } keys %{$args};
}

# {{{ signal handler
my $exit_flag = 0;
my @sig_keys = qw/INT TERM QUIT HUP PIPE/;
@SIG{@sig_keys}= (sub {
  logging('debug', "receive exit request.") unless($exit_flag);
  $exit_flag = 1;
}) x @sig_keys;
# }}}


load_settings();
my $dbh = DBI->connect("DBI:mysql:mysql:localhost", $MYSQL_USER, $MYSQL_PASSWORD) or die 'cannot connect';

while(1) {

  # マスタを切り替えた後に、接続がすぐにはできないことがある
  my $status0 = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
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
  
  
  my $status;
  my $is_stop_io_thread;
  do {
    $is_stop_io_thread = 1;
    $dbh->do('STOP SLAVE IO_THREAD');
    $status = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
    #print    "SELECT MASTER_POS_WAIT('$status->{Master_Log_File}', $status->{Read_Master_Log_Pos})\n";
    my $mpw = $dbh->selectcol_arrayref(
      "SELECT MASTER_POS_WAIT('$status->{Master_Log_File}', $status->{Read_Master_Log_Pos}, $MASTER_POS_WAIT)"
    );
  
    if (-1 == $mpw->[0]) {
      # 稀に、トランザクション途中で IO が止まってしまうことがある。
      # その場合は、一度 IO_THREAD を再開し、適当なところで再度止める
      $is_stop_io_thread = 0;
      $dbh->do('START SLAVE IO_THREAD');
      
      my $s = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
      logging('debug', "Read_Master_Log_Pos is $s->{Read_Master_Log_Pos}, bad Exec_Master_Log_Pos is $s->{Exec_Master_Log_Pos}.");
      logging('info',  "timeout MASTER_POS_WAIT. restart io_thead and wait $RESTART_IO_THREAD_WAIT sec");
      sleep($RESTART_IO_THREAD_WAIT);
    }
  } while(0 == $is_stop_io_thread);

  $dbh->do('STOP SLAVE');
  #my $status1 = $dbh->selectrow_hashref('SHOW SLAVE STATUS');
  #print $status->{Master_Log_File},':',$status->{Read_Master_Log_Pos},' - ', $status->{Exec_Master_Log_Pos}, "\n";
  #print $status1->{Master_Log_File},':',$status1->{Read_Master_Log_Pos},' - ', $status1->{Exec_Master_Log_Pos}, "\n";

  my $next_master = get_next_master_config($current_master);
  $dbh->do(make_change_master_to($next_master));
  
  set_master_config($current_master, 'MASTER_LOG_FILE', $status->{Master_Log_File});
  set_master_config($current_master, 'MASTER_LOG_POS', $status->{Read_Master_Log_Pos});
#  set_master_config($current_master, 'RELAY_LOG_FILE', $status->{Relay_Log_File});
#  set_master_config($current_master, 'RELAY_LOG_POS', $status->{Relay_Log_Pos});
  save_settings();
  
  logging('info', "save host $current_master $status->{Master_Log_File} : $status->{Read_Master_Log_Pos} and change host to $next_master->{MASTER_HOST}");

  $dbh->do('START SLAVE');
  sleep $SWITCH_WAIT;
}

sub logging {
  my ($type, $log) = @_;
  print scalar(localtime()), "\t", $log, "\n";  
}
