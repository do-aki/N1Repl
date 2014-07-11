package N1Repl::Config;

use strict;
use warnings;
use utf8;
use Carp qw/croak/;
use YAML::Tiny;
use Scalar::Util qw/blessed/;
use N1Repl::MasterConfig;
use N1Repl::CommandChecker;
use N1Repl::CommandPublisher;

# デフォルト値の設定
my %wait_default = (
  SWITCH_WAIT            => 3,   # マスタを切り替えた後に待つ秒数
  CONNECTING_WAIT        => 1,   # マスタへの接続待ちになったときに待つ秒数
  CATCHUP_WAIT           => 1,   # seconds_behind_master が 0 でないときに待つ秒数
  MASTER_POS_WAIT        => 10,  # master_pos_wait の 待つ秒数
  RESTART_IO_THREAD_WAIT => 3,   # io_thread 待ちでロックした際の、 io_thread を動かす秒数
  COMMAND_CHECK_WAIT     => 1,   # 
);

my %mysql_default = (
  MYSQL_HOST => '127.0.0.1',
  MYSQL_PORT => 3306,
  MYSQL_USER => $ENV{MYSQL_USER},
  MYSQL_PASSWORD => $ENV{MYSQL_PASSWORD},
);

sub new {
  my ($class, %args) = @_;

  my %self = ((
    _master_config => undef,
    _command_checker => undef,
    _command_publisher => undef,
  ), %wait_default);
  bless \%self => $class;
}

sub load {
  my ($self, $file) = @_;

  my $conf;
  eval{
    $conf = YAML::Tiny::LoadFile($file);

    Carp::croak("data_file is not specified in $file") if (!$conf->{data_file});
    $self->{_master_config} = new N1Repl::MasterConfig($conf->{data_file});

    Carp::croak("command_file is not specified in $file") if (!$conf->{command_file});
    $self->{_command_checker} = new N1Repl::CommandChecker($conf->{command_file});
    $self->{_command_publisher} = new N1Repl::CommandPublisher($conf->{command_file});
  };
  if ($@) {
    croak $@;
  }

  while(my ($k,$default) = each(%wait_default)) {
    my $v = $conf->{wait}{$k} || $default;
    $v = $default if ($v <= 0);
    $self->{$k} = $v;
  }

  $self->connect_config(%{$conf->{mysql}});
  $self;
}

sub connect_config {
  my ($self, %conf) = @_;

  if (%conf) {
    while(my($k,$default) = each(%mysql_default)) {
      my $v = $conf{$k} || $default;
      $self->{_connect}{$k} = $v;
    }
  }

  return (
    host => $self->{_connect}{MYSQL_HOST},
    port => $self->{_connect}{MYSQL_PORT},
    user => $self->{_connect}{MYSQL_USER},
    password => $self->{_connect}{MYSQL_PASSWORD},
  );
}

sub master_config {
  my ($self, $master_config) = @_;

  if ($master_config) {
    if (!blessed($master_config) || !$master_config->isa('N1Repl::MasterConfig')) {
      Carp::croak("invalid master config");
    }

    $self->{_master_config} = $master_config;
  }

  Carp::croak("not set master config") unless ($self->{_master_config});
  $self->{_master_config};
}

sub command_checker {
  my ($self, $checker) = @_;

  if ($checker) {
    if (!blessed($checker) || !$checker->isa('N1Repl::CommandChecker')) {
      Carp::croak("invalid command checker");
    }

    $self->{_command_checker} = $checker;
  }

  Carp::croak("not set command checker") unless ($self->{_command_checker});
  $self->{_command_checker};
}

sub command_publisher {
  my ($self, $publisher) = @_;

  if ($publisher) {
    if (!blessed($publisher) || !$publisher->isa('N1Repl::CommandPublisher')) {
      Carp::croak("invalid command publisher");
    }

    $self->{_command_publisher} = $publisher;
  }

  Carp::croak("not set command publisher") unless ($self->{_command_publisher});
  $self->{_command_publisher};
}

1;
