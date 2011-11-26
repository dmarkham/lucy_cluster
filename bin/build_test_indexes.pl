# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

my $schema  = MySchema->new;
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


