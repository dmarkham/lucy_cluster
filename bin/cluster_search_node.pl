use strict;
use warnings;
use Lucy;
use ClusterNode;
use Data::Dumper;


## the cluster Nodes flow..
## set up a listener that the cluster_searchers will be pinging.
##
## set up the port clients will be using.
##
## Fanout clients requests to the workers

my $cn = ClusterNode->new( hostport => '127.0.0.1:9905');




#my $hits = $cn->hits( query => "a" );

