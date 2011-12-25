use strict;
use warnings;
use Lucy;
use Getopt::Long;
use File::Slurp;
use JSON::XS;

my $coder = JSON::XS->new->utf8();


## This scripts job is to let the Node server 
## know what indexes the searcher can use and thier schema's.
## it may include some version info or other meta data
## in the future but for now just the schema.


## place where 1 or more Lucy indexes live
my $index_dir = "/tmp/indexes/";

my $debug = 0;
&GetOptions(
   'debug' => \$debug, 
   'index_dir' => \$index_dir, 
);

my $list_file = "$index_dir/index_list.json";

opendir(my $dh, $index_dir) || die "can't opendir $index_dir: $!";
 my @dirs = grep { -d "$index_dir/$_" && !/^\./ } readdir($dh);
closedir $dh;

my %indexes;
foreach my $dir (@dirs){
  eval {
    my $searcher = Lucy::Search::IndexSearcher->new( index => "$index_dir/$dir/" );
    my $s = $searcher->get_schema();
    my $dump = $s->dump();
    my $base_index = $dir;
    my $shard = 0;
    my $total_shards = 0;
    if($dir =~ m/^(.*)_LC_\d+_\d+$/){
      ($base_index,$shard,$total_shards) = ($dir =~ m/^(.*)_LC_(\d+)_(\d+)$/);
    
    }
    print "Adding:$dir\t($base_index) ($shard) ($total_shards)\n" if $debug;
    push @{$indexes{$base_index}} , { shard => $shard, 
                                      total_shards => $total_shards,
                                      index => $dir, 
                                      schema => $dump}; 
  };
  print  "Issue with $dir: $@\n" if $@;
}

write_file( $list_file, {binmode => ':utf8'}, $coder->encode(\%indexes) ) ;
