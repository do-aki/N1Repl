use strict;
use warnings;
use Test::More tests => 1;
use DBI;
use t::setUp;

my $master = setup_mysqld();
my $slave = setup_mysqld_slave($master);

my $db_m = DBI->connect($master->dsn());
my $db_s = DBI->connect($slave->dsn());

$db_m->do(<<'EOS');
CREATE TABLE test(
  id int not null,
  primary key(id)
)
EOS

$db_m->do(q{insert into test values(1),(2),(3)});

sleep 1;

my $sth = $db_s->prepare('select * from test;');
$sth->execute();
my $result = $sth->fetchall_arrayref();

is_deeply($result, [[1], [2], [3]]);

