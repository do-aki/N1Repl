package SwitchMaster::Config;

use strict;
use warnings;
use utf8;
use YAML::Tiny;
use Scalar::Util qw/blessed/;
use SwitchMaster::MasterConfig;

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

sub new {
  my ($class, %args) = @_;

  my %self = ((
    _master_config => undef,
  ), %wait_default);
  bless \%self => $class;
}

sub load {
  my ($self, $file) = @_;

  my $conf;
  eval{
    $conf = YAML::Tiny::LoadFile($file);

    Carp::croak("data_file is not specified in $file") if (!$conf->{data_file});
    $self->{_master_config} = new SwitchMaster::MasterConfig($conf->{data_file});
  };
  if ($@) {
    die $@;
  }

  while(my ($k,$default) = each(%wait_default)) {
    my $v = $conf->{wait}{$k} || $default;
    $v = $default if ($v <= 0);
    $self->{$k} = $v;
  }

  $self->connect_config(%{$conf->{mysql}});
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
    if (!blessed($master_config) || !$master_config->isa('SwitchMaster::MasterConfig')) {
      Carp::croak("invalid master config");
    }

    $self->{_master_config} = $master_config;
  }

  Carp::croak("no set master config") unless ($self->{_master_config});
  $self->{_master_config};
}

1;
