use strict;
use warnings;
use ClusterClient;


## A searching user must know how to get to a cluster_server
## at least one. I'm still on the fence about a writing user.. 
## we'll see..

my $cc = ClusterClient->new(server => '127.0.0.1:9006');

## I want the client to be able to ask questions about the indexes it has access to


my $searcher = $cc->get_searcher('test');

## get all indexes... 
## shards/nodes are hidden here behind a single searcher.
my @searchers = $cc->list_searchers();

foreach my $searcher (@searchers){
  my @shards = $searcher->list_shards([node => $node]);
  my @nodes = $searcher->list_nodes( [shard => $shard]);
  
  ## standard Lucy Schema just remote
  my $schema = $searcher->get_schema();
  ## standard Lucy Hits. Just happens to be remote.
  my $hits = $searcher->hits(query => 'a', ...);
}
