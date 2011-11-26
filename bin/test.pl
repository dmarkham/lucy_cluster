use strict;
use warnings;


package MySchema;
use base qw( Lucy::Plan::Schema );
use Lucy::Analysis::RegexTokenizer;

sub new {
    my $self = shift->SUPER::new(@_);
    my $type = Lucy::Plan::FullTextType->new(
        analyzer => Lucy::Analysis::RegexTokenizer->new, );
    $self->spec_field( name => 'title', type => $type );
    $self->spec_field( name => 'body',  type => $type );
    return $self;
}

package main;





use Lucy;
use ClusterNode;
use Data::Dumper;

my $schema  = MySchema->new;


my $cn = ClusterNode->new(index_name => "test");
my $hits = $cn->hits( query => "a" );

