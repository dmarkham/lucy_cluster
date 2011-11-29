use strict;
use warnings;
use Lucy;
use ClusterNode;
use Data::Dumper;

use Getopt::Long;

## the cluster Nodes flow..
## set up a listener that the cluster_searchers will be pinging.
##
## set up the port clients will be using.
##
## Fanout clients requests to the workers



my $debug = 0;
my $hostport = '127.0.0.1:9905';
my $upstream_host;
&GetOptions(
   'debug' => \$debug, 
   'hostport' => \$hostport, 
   'upstream_host' => \$upstream_host, 
   'index_dir' => \$index_dir, 
);



## Listen for my workers/child nodes
my $cn = ClusterNode->new( hostport => $hostport);

if($upstream_host){
  

}




#my $hits = $cn->hits( query => "a" );

