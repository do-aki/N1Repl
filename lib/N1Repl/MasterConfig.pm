package N1Repl::MasterConfig;

use strict;
use warnings;
use utf8;
use Carp;
use YAML::Tiny;

sub new {
  my ($class, $store_file) = @_;

  Carp::croak("missing mandatory parameter") unless $store_file;


  my $self = bless {
    store_file => $store_file,
    masters => [],
  } => $class;

  if (-f $store_file) {
    my $dat = YAML::Tiny::LoadFile($store_file);

    for my $m (@{$dat}) {
      $self->add(%{$m});
    }
  }

  $self;
}

sub add {
  my ($self, %args) = @_;

  for my $k (qw/MASTER_HOST MASTER_LOG_FILE MASTER_LOG_POS/) {
    Carp::croak("missing mandatory parameter '$k'") unless $args{$k};
  }

  push(@{$self->{masters}}, \%args);
}

sub find {
  my ($self, $host, $port) = @_;

  my $index = 0;
  for my $m (@{$self->{masters}}) {
    if ($host eq $m->{MASTER_HOST} && $port == $m->{MASTER_PORT}) {
      return wantarray ? ($m, $index) : $m;
    }
    ++$index;
  }
  return undef;
}

sub first {
  my $self = shift;
  $self->{masters}[0];
}

sub next {
  my ($self, $master) = @_;

  my($found, $index) = $self->find($master->{MASTER_HOST}, $master->{MASTER_PORT});

  $index += 1;
  if (!$found || $#{$self->{masters}} < $index) {
    $index = 0;
  }

  $self->{masters}[$index];
}

sub set_value {
  my ($self, $master, $key_value) = @_;

  my $conf = $self->find($master->{MASTER_HOST}, $master->{MASTER_PORT});
  return if (!$conf);

  %$conf = (%$conf, %$key_value);

  $self;
}

sub serialize {
  my $self = shift;
  YAML::Tiny::DumpFile($self->{store_file}, $self->{masters});
}

1;

