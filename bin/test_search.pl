use strict;
use Lucy;
use Data::Dumper;
use Storable qw( nfreeze thaw );

my $searcher = Lucy::Search::IndexSearcher->new( index => "/tmp/indexes/test/" );
my $schema = $searcher->get_schema();

#my $reader = $searcher->get_reader->get_snapshot->get_path;
#print Dumper($reader);

my $list = $schema->architecture();
print Dumper($list);
exit;

foreach my $field(@{$list}){
   
  my $type = $schema->fetch_type($field); 
  print nfreeze($type);
  print "$field\t$type\n";


}
print Dumper($list);


