package SwitchMaster;

use strict;
use warnings;
use utf8;
use Carp;
use Scalar::Util qw/blessed/;
use Data::Dumper;

our $VERSION   = '0.02';

sub new {
  my ($class, %args) = @_;
  my $driver = delete($args{driver}) || 'DBI';

  eval("require SwitchMaster::Driver::$driver");
  if ($@) {
    Carp::croak("invalid driver '$driver'");
  }

  die ("missing mandatory parameter 'config'") unless $args{config};

  bless {
    driver_name => "SwitchMaster::Driver::$driver",
    config => $args{config},
  } => $class;
}

sub connect {
  my ($self, %args) = @_;

  if (scalar %args) {
    $self->{config}->set_connect_config(%args);
  }

  my %conf = $self->{config}->get_connect_config();
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
    next;
  }

  # 遅れを待つ
  if (0 < ($status0->{Seconds_Behind_Master} or 0)) {
    $self->logging('debug', "Seconds_Behind_Master is $status0->{Seconds_Behind_Master}. wait $config->{CATCHUP_WAIT} sec");
    sleep $config->{CATCHUP_WAIT};
    next;
  }

  my $current_master = {
    host => $status0->{Master_Host},
    port => $status0->{Master_Port},
  };
  if (!$config->get_master_config($current_master)) {
    die("$current_master->{host} is not defined");
  }
# }}}
  
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
  #my $status2 = $driver->show_slave_status();
  #print $status1->{Master_Log_File},':',$status1->{Read_Master_Log_Pos},' - ', $status1->{Exec_Master_Log_Pos}, "\n";
  #print $status2->{Master_Log_File},':',$status2->{Read_Master_Log_Pos},' - ', $status2->{Exec_Master_Log_Pos}, "\n";
# }}}


# {{{ Phase 3 - switch master
  my $next_master = $config->get_next_master_config($current_master);
  $driver->do(make_change_master_to($next_master));
  
  $config->set_master_config($current_master, 'MASTER_LOG_FILE', $status1->{Master_Log_File});
  $config->set_master_config($current_master, 'MASTER_LOG_POS', $status1->{Read_Master_Log_Pos});
#  set_master_config($current_master, 'RELAY_LOG_FILE', $status2->{Relay_Log_File});
#  set_master_config($current_master, 'RELAY_LOG_POS', $status2->{Relay_Log_Pos});
  $config->save();
  
  $self->logging('info', "save host $current_master->{host}:$current_master->{port} $status1->{Master_Log_File} : $status1->{Read_Master_Log_Pos} and change host to $next_master->{MASTER_HOST}:$next_master->{MASTER_PORT}");

  $driver->do('START SLAVE');
  sleep $config->{SWITCH_WAIT};
# }}}
}

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



package SwitchMaster::Config;

use strict;
use warnings;
use utf8;
use FindBin;
use lib $FindBin::Bin . '/lib';
use YAML::Tiny;


# デフォルト値の設定
my %wait_default = (
  SWITCH_WAIT            => 3,   # マスタを切り替えた後に待つ秒数
  CONNECTING_WAIT        => 1,   # マスタへの接続待ちになったときに待つ秒数
  CATCHUP_WAIT           => 1,   # seconds_behind_master が 0 でないときに待つ秒数
  MASTER_POS_WAIT        => 10,  # master_pos_wait の 待つ秒数
  RESTART_IO_THREAD_WAIT => 3,   # io_thread 待ちでロックした際の、 io_thread を動かす秒数
);

my %mysql_default = (
  MYSQL_HOST => '127.0.0.1',
  MYSQL_PORT => 3306,
  MYSQL_USER => $ENV{MYSQL_USER},
  MYSQL_PASSWORD => $ENV{MYSQL_PASSWORD},
);

my @CONFIG_MASTERS;

sub new {
  my ($class, %args) = @_;

  my %self = ((
    _conf_file => $args{conf_file},
    _data_file => $args{data_file},
  ), %wait_default);
  bless \%self => $class;
}

sub get_data_file {
  (shift)->{_data_file};
}

sub set_data_file {
  my ($self, $file) = @_;
  $self->{_data_file} = $file;

  if (-f $self->{_data_file}) {
    my $dat = YAML::Tiny::LoadFile($self->{_data_file});

    for my $m (@{$dat}) {
      $self->add_master(%{$m});
    }
  }
}

sub load {
  my ($self, $file) = @_;

  my $conf;
  eval{
    $conf = YAML::Tiny::LoadFile($file);

    Carp::croak("data_file is not specified in $file") if (!$conf->{data_file});
    $self->set_data_file($conf->{data_file});
  };
  if ($@) {
    die $@;
  }

  while(my ($k,$default) = each(%wait_default)) {
    my $v = $conf->{wait}{$k} || $default;
    $v = $default if ($v <= 0);
    $self->{$k} = $v;
  }

  $self->set_connect_config(%{$conf->{mysql}});
}

sub save {
  my $self = shift;
  YAML::Tiny::DumpFile($self->{_data_file}, \@CONFIG_MASTERS);
}

sub add_master {
  my ($self, %args) = @_;

  for my $k (qw/MASTER_HOST MASTER_LOG_FILE MASTER_LOG_POS/) {
    Carp::croak("missing mandatory parameter '$k'") unless $args{$k};
  }

  unless ($args{DISPLAY_NAME}) {
    $args{DISPLAY_NAME} = $args{MASTER_HOST} . ':' . $args{MASTER_PORT};
  }

  push(@CONFIG_MASTERS, \%args);
}

sub get_connect_config {
  my $self = shift;
  return (
    host => $self->{_connect}{MYSQL_HOST},
    port => $self->{_connect}{MYSQL_PORT},
    user => $self->{_connect}{MYSQL_USER},
    password => $self->{_connect}{MYSQL_PASSWORD},
  );
}

sub set_connect_config {
  my ($self, %conf) = @_;
  while(my($k,$default) = each(%mysql_default)) {
    my $v = $conf{$k} || $default;
    $self->{_connect}{$k} = $v;
  }
}

sub get_master_config {
  my ($self, $host, $name) = @_;

  for my $m (@CONFIG_MASTERS) {
    if ($host->{host} eq $m->{MASTER_HOST} && $host->{port} == $m->{MASTER_PORT}) {
      return (defined $name)? $m->{$name} : $m;
    }
  }
  return undef;
}

sub get_next_master_config {
  my ($self, $host) = @_;
  my $flag = 0;
  for my $m (@CONFIG_MASTERS) {
    return $m if ($flag);
    $flag = 1 if ($host->{host} eq $m->{MASTER_HOST} && $host->{port} == $m->{MASTER_PORT});
  }
  return $CONFIG_MASTERS[0];
}

sub set_master_config {
  my ($self, $host, $name, $value) = @_;

  my $conf = $self->get_master_config($host);
  return if (!$conf);
  $conf->{$name} = $value;
}


1;
