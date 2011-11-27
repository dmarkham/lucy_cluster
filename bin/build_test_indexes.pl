use strict;
use warnings;

use Lucy;
use Lucy::Plan::Schema;
use Lucy::Plan::FullTextType;
use Lucy::Analysis::PolyAnalyzer;


my $schema = Lucy::Plan::Schema->new;
my $polyanalyzer = Lucy::Analysis::PolyAnalyzer->new(
               language => 'en',
           );
my $type = Lucy::Plan::FullTextType->new(
               analyzer => $polyanalyzer,
           );
$schema->spec_field( name => 'title',   type => $type );
$schema->spec_field( name => 'body', type => $type );

my $indexer = Lucy::Index::Indexer->new(
    index  => "/tmp/indexes/test",
    schema => $schema,
    create => 1
);
my %docs = (
    'a' => 'foo',
    'b' => 'bar',
);

while ( my ( $title, $body ) = each %docs ) {
    $indexer->add_doc(
        {   title => $title,
            body  => $body,
        }
    );
}
$indexer->commit;

my $searcher = Lucy::Search::IndexSearcher->new( index => "/tmp/indexes/test" );

my $tokenizer = Lucy::Analysis::RegexTokenizer->new;
my $or_parser = Lucy::Search::QueryParser->new(
    schema   => $schema,
    analyzer => $tokenizer,
    fields   => [ 'title', 'body', ],
);
my $and_parser = Lucy::Search::QueryParser->new(
    schema         => $schema,
    analyzer       => $tokenizer,
    fields         => [ 'title', 'body', ],
    default_boolop => 'AND',
);

my $hits = $searcher->hits( query => "a" );
print $hits->total_hits  ."\n" ;


