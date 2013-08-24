use strict;
use warnings;
use utf8;
use FindBin;
use Data::Dumper;
use lib $FindBin::Bin . '/lib';
use YAML::Tiny;

my $DATA_FILE = $FindBin::Bin . '/data/masters.yaml';
my $CONF_FILE = $FindBin::Bin . '/data/settings.yaml';

our $MYSQL_USER;
our $MYSQL_PASSWORD;

# デフォルト値の設定
our $SWITCH_WAIT            = 3;   # マスタを切り替えた後に待つ秒数
our $CONNECTING_WAIT        = 1;   # マスタへの接続待ちになったときに待つ秒数
our $CATCHUP_WAIT           = 1;   # Seconds_Behind_Master が 0 でないときに待つ秒数
our $MASTER_POS_WAIT        = 10;  # MASTER_POS_WAIT の 待つ秒数
our $RESTART_IO_THREAD_WAIT = 3;   # IO_THREAD 待ちでロックした際の、 IO_THREAD を動かす秒数

my @CONFIG_MASTERS;

# {{{ データ永続化関連
sub load_settings {
  my $dat = YAML::Tiny::LoadFile($DATA_FILE);
  
  @CONFIG_MASTERS = @{$dat};
  
  my $conf = YAML::Tiny::LoadFile($CONF_FILE);
  while(my($k,$v) = each(%{$conf->{wait}})) {
    no strict 'refs';
    $$k = $v if (0 < $v);
  }
  
  while(my($k,$v) = each(%{$conf->{mysql}})) {
    no strict 'refs';
    $$k = $v;
  }
}

sub save_settings {
  YAML::Tiny::DumpFile($DATA_FILE, \@CONFIG_MASTERS);
}

sub get_master_config {
  my ($host,$name) = @_;
  for my $m (@CONFIG_MASTERS) {
    if ($host->{host} eq $m->{MASTER_HOST} && $host->{port} == $m->{MASTER_PORT}) {
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
    $flag = 1 if ($host->{host} eq $m->{MASTER_HOST} && $host->{port} == $m->{MASTER_PORT});
  }
  return $CONFIG_MASTERS[0];
}
sub set_master_config {
  my ($host, $name, $value) = @_;
  my $conf = get_master_config($host);
  return if (!$conf);
  $conf->{$name} = $value;
}
# }}}

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

sub hook_signals{
  my ($signals, $handler) = @_;
  @SIG{@$signals}= ($handler) x @$signals;
}

sub logging {
  my ($type, $log) = @_;
  print scalar(localtime()), "\t", $log, "\n";  
}

1;
