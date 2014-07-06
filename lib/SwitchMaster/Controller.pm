package SwitchMaster::Controller;

use strict;
use warnings;
use utf8;
use Carp;
use Scalar::Util qw/blessed/;
use Data::Dumper;
use SwitchMaster::Config;

our $VERSION   = '0.02';

sub new {
  my ($class, %args) = @_;
  my $driver = delete($args{driver}) || 'DBI';

  eval("require SwitchMaster::Driver::$driver");
  if ($@) {
    Carp::croak("invalid driver '$driver'");
  }

  my $config = $args{config};
  if (!blessed($config) || !$config->isa('SwitchMaster::Config')) {
    Carp::croak("invalid config") unless $args{config};
  }

  bless {
    driver_name => "SwitchMaster::Driver::$driver",
    config => $args{config},
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

  my $exit_flag = 0;
  hook_signals([qw/INT TERM QUIT HUP PIPE/], sub {
    $self->logging('debug', "receive exit request.") unless($exit_flag);
    $exit_flag = 1;
  });

  while(1) {

    if ($exit_flag) {
      $self->logging('info', "exit.");
      exit(0);
    }

    eval {
      my $master = $self->check_current_status();
      next unless($master);
      my ($master_log_file, $read_master_log_pos) = $self->stop_slave($master);
      $self->switch_master($master, $master_log_file, $read_master_log_pos);
    };
    if ($@) {
      $self->logging('info', "exit ($@)");
      exit(0);
    }
  }
}

sub check_current_status {
  my $self = shift;
  my $driver = $self->{driver};
  my $config = $self->{config};

  # {{{ Phase 1 - check current status
  my $status0 = $driver->show_slave_status();

  # 何かしらのエラーがあったら停止
  if (!$status0->{Slave_IO_State} # any error?
      || 'Yes' ne $status0->{Slave_SQL_Running} || 'Yes' ne $status0->{Slave_IO_Running}
      || 0 != $status0->{Last_IO_Errno} || 0 != $status0->{Last_SQL_Errno} ) {
    print Dumper $status0;
    die ('slave stopped');
  }

  # マスタを切り替えた後に、接続がすぐにはできないことがある
  if ('Connecting' eq $status0->{Slave_IO_Running}) {
    $self->logging('debug', "Slave_IO_Running status is Connecting. wait $config->{CONNECTING_WAIT} sec");
    sleep $config->{CONNECTING_WAIT};
    return undef;
  }

  # 遅れを待つ
  if (0 < ($status0->{Seconds_Behind_Master} or 0)) {
    $self->logging('debug', "Seconds_Behind_Master is $status0->{Seconds_Behind_Master}. wait $config->{CATCHUP_WAIT} sec");
    sleep $config->{CATCHUP_WAIT};
    return undef;
  }

  my $current_master = $config->master_config->find($status0->{Master_Host}, $status0->{Master_Port});
  unless ($current_master) {
    die("master $status0->{Master_Host}:$status0->{Master_Port} is not defined");
  }
  # }}}

  $current_master;
}

sub stop_slave {
  my $self = shift;

  my $driver = $self->{driver};
  my $config = $self->{config};

  # {{{ Phase 2 - stop slave
  my $status1;
  my $is_stop_io_thread;
  do {
    $is_stop_io_thread = 1;
    $driver->do('STOP SLAVE IO_THREAD');
    $status1 = $driver->show_slave_status();
    #print    "SELECT MASTER_POS_WAIT('$status1->{Master_Log_File}', $status1->{Read_Master_Log_Pos})\n";
    my $mpw = $driver->master_pos_wait(
      $status1->{Master_Log_File},
      $status1->{Read_Master_Log_Pos},
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
  # }}}
  #print $status1->{Master_Log_File},':',$status1->{Read_Master_Log_Pos},' - ', $status1->{Exec_Master_Log_Pos}, "\n";

  return ($status1->{Master_Log_File}, $status1->{Read_Master_Log_Pos});
}

sub switch_master {
  my $self = shift;
  my $master = shift;
  my $master_log_file = shift;
  my $read_master_log_pos = shift;

  my $driver = $self->{driver};
  my $config = $self->{config};

  #my $status2 = $driver->show_slave_status();
  #print $status2->{Master_Log_File},':',$status2->{Read_Master_Log_Pos},' - ', $status2->{Exec_Master_Log_Pos}, "\n";

  # {{{ Phase 3 - switch master
    my $next_master = $config->master_config->next($master);
    $driver->do(make_change_master_to($next_master));

    $config->master_config->set_value($master, {
      MASTER_LOG_FILE => $master_log_file,
      MASTER_LOG_POS  => $read_master_log_pos,
  #    RELAY_LOG_FILE  => $status2->{Relay_Log_File}
  #    RELAY_LOG_POS   => $status2->{Relay_Log_Pos},
    })->serialize();

    $self->logging('info', "save host $master->{DISPLAY_NAME} $master_log_file : $read_master_log_pos and change host to $next_master->{DISPLAY_NAME}");

    $driver->do('START SLAVE');
    sleep $config->{SWITCH_WAIT};
  # }}}
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
