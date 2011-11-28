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


my @shards =  (1,2,3,4,6,10,30);



foreach my $shard_count (@shards){
  my %indexes;  
  for (my $i=0;$i < $shard_count;$i++){
     my $num = ${shard_count} -1;
     $indexes{$i} =  Lucy::Index::Indexer->new(
        index  => "/tmp/indexes/test_index${num}_LC_${i}_${num}",
        schema => $schema,
        create => 1,
    );
  }
 
  for (my $i=0;$i < 100;$i++){
      
    $indexes{$i % $shard_count }->add_doc( {   title => "$i", body  => "body" });
  }

  map{$_->commit() } values %indexes; 

}
print "Done but you have not tested the indexes yet to verify they have the corrent data!\n";
