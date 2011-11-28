use strict;
use warnings;

use Lucy;
use Lucy::Plan::Schema;
use Lucy::Plan::FullTextType;
use Lucy::Analysis::PolyAnalyzer;

my $schema = Lucy::Plan::Schema->new;
my $polyanalyzer = Lucy::Analysis::PolyAnalyzer->new( language => 'en');
my $type = Lucy::Plan::FullTextType->new( analyzer => $polyanalyzer);
$schema->spec_field( name => 'title',   type => $type );
$schema->spec_field( name => 'body', type => $type );


my @shards =  (1,2,3,4,5,6,7,8,9,10);



foreach my $shard_count (@shards){
  my %indexes;  
  for (my $i=0;$i < $shard_count;$i++){
     $indexes{$i} =  Lucy::Index::Indexer->new(
        index  => "/tmp/indexes/shard_test${shard_count}__$i",
        schema => $schema,
        create => 1,
    );
  }
 
  for (my $i=0;$i < 100;$i++){
      
    $indexes{$i % $shard_count }->add_doc( {   title => "$i", body  => "body" });
  }

  map{$_->commit() } values %indexes; 

}
print "Done\n";
