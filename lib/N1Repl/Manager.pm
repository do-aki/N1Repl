package N1Repl::Manager;

use strict;
use warnings;
use utf8;
use Carp;
use Scalar::Util qw/blessed/;
use Data::Dumper;
use N1Repl::Config;

our $VERSION   = '0.02';

sub new {
  my ($class, %args) = @_;
  my $driver = delete($args{driver}) || 'DBI';

  eval("require N1Repl::Driver::$driver");
  if ($@) {
    Carp::croak("invalid driver '$driver'");
  }

  my $config = $args{config};
  if (!$config || !blessed($config) || !$config->isa('N1Repl::Config')) {
    Carp::croak("invalid config");
  }

  bless {
    driver_name => "N1Repl::Driver::$driver",
    config => $config,
  } => $class;
}

sub connect {
  my ($self, %args) = @_;

  if (scalar %args) {
    $self->{config}->connect_config(%args);
  }

  my %conf = $self->{config}->connect_config;
  $conf{port} = $conf{port} || 3306;

  my $driver;
  eval {
    $driver = $self->{driver_name}->new($conf{host}, $conf{port}, $conf{user}, $conf{password});
  };
  if ($@ || !$driver) {
    Carp::croak("cannot create driver: $@");
  }

  $self->{driver} = $driver;
}

sub logging {
  my ($self, $type, $log) = @_;
  print scalar(localtime()), "\t", $log, "\n";  
}

sub run {
  my $self = shift;
  my $driver = $self->{driver};
  my $config = $self->{config};
  my $checker = $config->command_checker;

  my $exit_flag = 0;
  hook_signals([qw/INT TERM QUIT HUP PIPE/], sub {
    $self->logging('debug', "receive exit request.") unless($exit_flag);
    $exit_flag = 1;
  });
  my $exit_check = sub {
    $exit_flag;
  };

  # slave 設定がない場合は"最初"の設定に接続
  my $status = $driver->show_slave_status();
  if (!defined($status)) {
    my $first_master = $config->master_config->first();
    $driver->do(make_change_master_to($first_master));
    $driver->do('START SLAVE');
  }


  while(1) {
    if ($exit_check->()) {
      $self->logging('info', "exit.");
      exit(0);
    }

    eval {
      my $cmd = $checker->fetch();
      if ($cmd->called('STOP')) {
        $self->wait_for_start($exit_check);
      } elsif ($cmd->called('SWITCH')) {
        $self->shift_master($cmd);
      }

      my $status = $self->check_current_status();
      if ('stopped' eq $status) {
        $self->wait_for_start($exit_check);
      } elsif ('waiting' eq $status) {
        # do nothing
      } elsif ('ready' eq $status) {
        my $master = $self->stop_slave();
        $self->switch_master($master);
      }

    };
    if ($@) {
      $self->logging('info', "exit ($@)");
      exit(0);
    }
  }
}

sub shift_master {
  my $self = shift;
  my $cmd = shift;
  my $driver = $self->{driver};
  my $config = $self->{config};
  # 1. 現在接続中の MASTER を確認
  # 2. 対象でない場合は切り替え
  # 3. MASTER_POS_WAIT
  # 5. 現在の設定(new master で保存後、次のマスタに切り替え

  # check definition
  my $orig_master_host = $cmd->get_param('orig_master_host');
  my $orig_master_port = $cmd->get_param('orig_master_port'),
  my $orig_master_log_file = $cmd->get_param('orig_master_log_file');
  my $orig_master_log_pos  = $cmd->get_param('orig_master_log_pos');

  my $orig_master = $config->master_config->find($orig_master_host, $orig_master_port);
  unless ($orig_master) {
    die ("orig_master ($orig_master_host:$orig_master_port) is not defined");
  }

  # TODO: 切り替え後が現在よりも先の位置かどうかを確認

  my $status = $driver->show_slave_status();
  if ($orig_master->{MASTER_HOST} ne $status->{Master_Host} || $orig_master->{MASTER_PORT} != $status->{Master_Port}) {
    $self->stop_slave();
    $driver->do(make_change_master_to($orig_master));
    $driver->do('START SLAVE');
  }

  $driver->master_pos_wait($orig_master_log_file, $orig_master_log_pos, $config->{MASTER_POS_WAIT});
  $driver->do('STOP SLAVE');

  $orig_master->{MASTER_HOST} = $cmd->get_param('new_master_host');
  $orig_master->{MASTER_PORT} = $cmd->get_param('new_master_port');
  $orig_master->{MASTER_LOG_FILE} = $cmd->get_param('new_master_log_file');
  $orig_master->{MASTER_LOG_POS}  = $cmd->get_param('new_master_log_pos');
  $config->master_config->serialize();

  my $next_master = $config->master_config->next($orig_master);
  $driver->do(make_change_master_to($next_master));
  $driver->do('START SLAVE');
  sleep $config->{SWITCH_WAIT};
}

sub wait_for_start {
  my $self = shift;
  my $exit_check = shift;
  my $config = $self->{config};
  my $driver = $self->{driver};
  my $checker = $config->command_checker;

  while (1) {
    sleep ($config->{COMMAND_CHECK_WAIT});
    return if ($exit_check->());

    my $cmd = $checker->fetch();
    if ($cmd->called('START')) {
      $driver->do('START SLAVE');
      return;
    }
  }
}

sub check_current_status {
  my $self = shift;
  my $driver = $self->{driver};
  my $config = $self->{config};

  # Phase 1 - check current status
  my $status = $driver->show_slave_status();
  unless (exists $status->{Slave_IO_State}) {
    # slave 設定無し
    die ('no slave setting');
  }

  unless ($status->{Slave_IO_State}) {
    # slave 停止中
    $self->logging('debug', "Slave stopped");
    return 'stopped';
  }

  # マスタを切り替えた後に、接続がすぐにはできないことがある
  if ('Connecting' eq $status->{Slave_IO_Running} || $status->{Slave_IO_State} =~ /^Connecting/) {
    $self->logging('debug', "Slave_IO_Running status is Connecting. wait $config->{CONNECTING_WAIT} sec");
    sleep $config->{CONNECTING_WAIT};
    return 'waiting';
  }

  # 何かしらのエラーがあったら停止
  if ('Yes' ne $status->{Slave_SQL_Running} || 'Yes' ne $status->{Slave_IO_Running}
      || '' ne $status->{Last_SQL_Error} || '' ne $status->{Last_IO_Error}
      ||  0 != $status->{Last_SQL_Errno} ||  0 != $status->{Last_IO_Errno} ) {
    print Dumper $status;
    die ('slave error');
  }

  # 遅れを待つ
  if (0 < ($status->{Seconds_Behind_Master} or 0)) {
    $self->logging('debug', "Seconds_Behind_Master is $status->{Seconds_Behind_Master}. wait $config->{CATCHUP_WAIT} sec");
    sleep $config->{CATCHUP_WAIT};
    return 'waiting';
  }

  # check definition
  my $current_master = $config->master_config->find($status->{Master_Host}, $status->{Master_Port});
  unless ($current_master) {
    die("master $status->{Master_Host}:$status->{Master_Port} is not defined");
  }

  return 'ready';
}

sub stop_slave {
  my $self = shift;

  my $driver = $self->{driver};
  my $config = $self->{config};

  # Phase 2 - stop slave
  my $status;
  my $is_stop_io_thread;
  do {
    $is_stop_io_thread = 1;
    $driver->do('STOP SLAVE IO_THREAD');
    $status = $driver->show_slave_status();
    # SELECT MASTER_POS_WAIT('$status->{Master_Log_File}', $status->{Read_Master_Log_Pos})
    my $mpw = $driver->master_pos_wait(
      $status->{Master_Log_File},
      $status->{Read_Master_Log_Pos},
      $config->{MASTER_POS_WAIT}
    );

    if (-1 == $mpw) {
      # 稀に、トランザクション途中で IO が止まってしまうことがある。
      # その場合は、一度 IO_THREAD を再開し、適当なところで再度止める
      $is_stop_io_thread = 0;
      $driver->do('START SLAVE IO_THREAD');

      my $s = $driver->show_slave_status();
      $self->logging('debug', "Read_Master_Log_Pos is $s->{Read_Master_Log_Pos}, bad Exec_Master_Log_Pos is $s->{Exec_Master_Log_Pos}.");
      $self->logging('info',  "timeout MASTER_POS_WAIT. restart io_thead and wait $config->{RESTART_IO_THREAD_WAIT} sec");
      sleep $config->{RESTART_IO_THREAD_WAIT};
    }
  } while(0 == $is_stop_io_thread);

  $driver->do('STOP SLAVE');
  #print $status1->{Master_Log_File},':',$status1->{Read_Master_Log_Pos},' - ', $status1->{Exec_Master_Log_Pos}, "\n";

  my $current_master = $config->master_config->find($status->{Master_Host}, $status->{Master_Port});
  unless ($current_master) {
    die("master $status->{Master_Host}:$status->{Master_Port} is not defined");
  }

  $current_master->{MASTER_LOG_FILE} = $status->{Master_Log_File};
  $current_master->{MASTER_LOG_POS}  = $status->{Read_Master_Log_Pos};
  $config->master_config->serialize();

  return $current_master;
}

sub switch_master {
  my $self = shift;
  my $master = shift;

  my $driver = $self->{driver};
  my $config = $self->{config};

  #my $status2 = $driver->show_slave_status();
  #print $status2->{Master_Log_File},':',$status2->{Read_Master_Log_Pos},' - ', $status2->{Exec_Master_Log_Pos}, "\n";

  # Phase 3 - switch master
  my $next_master = $config->master_config->next($master);
  $driver->do(make_change_master_to($next_master));

  $self->logging('info', 
    "save host $master->{MASTER_HOST}:$master->{MASTER_PORT} ($master->{MASTER_LOG_FILE}:$master->{MASTER_LOG_POS})"
    . " and switch host to $next_master->{MASTER_HOST}:$next_master->{MASTER_PORT}"
  );

  $driver->do('START SLAVE');
  sleep $config->{SWITCH_WAIT};
}


sub hook_signals{
  my ($signals, $handler) = @_;
  @SIG{@$signals}= ($handler) x @$signals;
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


1;
