use strict;
use warnings;
use Cluster::Client;
use Data::Dumper;
## A searching user must know how to get to a cluster_server
## at least one. I'm still on the fence about a writing user..


## A given cc object's endpoint should be eqauls
## we only use one at a time.
#my $cc = Cluster::Client->new(endpoints => ['127.0.0.1:9905','127.0.0.1:9007' ]);
my $cc = Cluster::Client->new(endpoints => ['127.0.0.1:9905' ]);

my $searcher = $cc->get_searcher('test_index1');
print Dumper($searcher);


my $r = $searcher->doc_freq( field => 'title', term => '1' );
print Dumper($r);


exit;
my $r = $searcher->doc_max();

my $r = $searcher->fetch_doc(1);

my $r = $searcher->fetch_doc_vec(1);

my $hits = $searcher->hits( query => 'x' );

while ( my $hit = $hits->next ) {
    print "Hit: $hit->{body}\n";
}

#}
exit;




__END__

## I want the client to be able to ask questions about the indexes it has access to


## get all indexes...
## shards/nodes are hidden here behind a single searcher.
my @searchers = $cc->list_searchers();

foreach my $searcher (@searchers) {
  my @shards = $searcher->list_shards([node => $node]);
  my @nodes = $searcher->list_nodes([shard => $shard]);

  ## standard Lucy Schema just remote
  my $schema = $searcher->get_schema();
  ## standard Lucy Hits. Just happens to be remote.
  my $hits = $searcher->hits(query => 'a', ...);
}
