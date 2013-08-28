use strict;
use File::Basename;
use FindBin;
use lib dirname($FindBin::Bin) . '/lib';
use Test::More;

BEGIN {
  use_ok 'SwitchMaster';
}

done_testing;
