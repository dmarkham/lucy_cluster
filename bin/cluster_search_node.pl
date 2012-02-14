use strict;
use warnings;
use Lucy;
use AnyEvent::Socket qw/tcp_server parse_hostport/;
use AnyEvent::Handle;
use AnyEvent::Strict;
use Data::Dumper;
use JSON::XS;
use Storable qw( nfreeze thaw );
use bytes;
no bytes;
no strict qw(refs);
use Getopt::Long;

my $debug                  = 0;
my $node_id = join("_",$$,int(rand(1_000_000))); 
my $coder = JSON::XS->new->utf8();

my $client_hostport = '0:9905';
my $message_id = 1;

&GetOptions('debug'             => \$debug,
            'client_hostport:s'     => \$client_hostport);



my %globals = (max_clients => 2000,
               cur_clients => 0,
              );


my ($host,$port) = parse_hostport($client_hostport);

tcp_server $host, $port, sub {
  my ($fh, $host, $port) = @_;
  
   if($globals{cur_clients} > $globals{max_clients}){
      syswrite $fh, "FULL";
      print "Rejected a Client i have to many\n";
      return;
   } 
   print join("\t","Starting Connection from $host:$port",$globals{cur_clients}+1,"\n");
   my $handle = new AnyEvent::Handle
     fh => $fh, 
     on_error => sub {
        my ($hdl, $fatal, $msg) = @_;
        warn "Error:$msg";
        close_client($hdl);  
      },
     on_read => sub {
        # some data is here, now queue the length-header-read (4 octets)
        shift->unshift_read (chunk => 4, sub {
           # header arrived, decode
           my $len = unpack "N", $_[1];
           # now read the payload
           shift->unshift_read (chunk => $len, sub {
             _dispatch_message(@_);
          }); 
       }); 
     },
     timout =>0,;
   


   $globals{cur_clients}++;
   $globals{clients}{$handle}{handle} = $handle;
};


## keep my workers freash in case they hang 
## or quitly go way
my $w = AE::timer 20, 20, sub { 
  my @list = keys  %{$globals{workers}};
  @list = map { $globals{clients}{$_}{handle} } @list;
  _send(clients =>\@list, response =>  { _action => 'index_status' } );
};


AnyEvent->condvar->recv;

sub update_workers_indexes{
  my %index_to_shards;
  my %index_to_schemas;
  foreach my $hdl (keys %{$globals{workers}}){
    foreach my $index (keys %{$globals{workers}{$hdl}{indexes}}){
      foreach my $data (@{$globals{workers}{$hdl}{indexes}{$index}}){
        $index_to_shards{$index}{total_shards} = $data->{total_shards} +1; 
        if (!exists $index_to_shards{$index}{shards}{$data->{shard}}){
          $index_to_shards{$index}{my_shard_count}++;
        }
        push @{$index_to_shards{$index}{shards}{$data->{shard}}} , {name => $data->{index} , handle => $hdl}; 
        $index_to_schemas{$index} = $data->{schema};
      }
    }
  }
  
  
  $globals{indexes} = \%index_to_shards;
  $globals{schemas} = \%index_to_schemas;
  #print Dumper(\%globals);
}

sub collect_response{
  my $hdl = shift;
  my $message = shift;
  #print Dumper($message);
  #print Dumper($globals{response}); 
  if(exists $globals{response}{$message->{message_id}}){
    my $done = 1;
    my @res;
    foreach my $data (@{$globals{response}{$message->{message_id}}}){
      #print Dumper($data);
      if($data->{index_name} eq $message->{index_name}){
        $data->{data} = $message->{response};
      }
      if(!exists $data->{data}){
        $done = 0;
      }
    }
    if($done){
      #print "DONE:";
      #print Dumper($globals{response}{$message->{message_id}});
      my $data = delete $globals{response}{$message->{message_id}};
      
      ## send  response  
      send_response($data);

    }
  }
  return;

}


sub send_response{
  my $data = shift;
  
  my $client = $data->[0]->{respond_to};
  my $action = $data->[0]->{action};

  print Dumper($data); 
  my @responses;
  foreach my $r ( @{$data}){
    push @responses,$r->{data};
  }
  
  my $result = &$action(\@responses);  
  
  _send(clients =>[$client], response => $result  );

}

sub doc_freq {
    my $responses = shift;
    my $doc_freq  = 0;
    $doc_freq += $_ for @$responses;
    return $doc_freq;
}

sub doc_max {
    my $responses = shift;
    my $doc_max  = 0;
    $doc_max += $_ for @$responses;
    return $doc_max;
}

sub top_docs {
    my $responses  = shift;
    my $total_hits = 0;

    print Dumper($responses);
    # Create HitQueue.
    my $hit_q;
    #if ($sort_spec) {
    #    $hit_q = Lucy::Search::HitQueue->new(
    #        schema    => $self->get_schema,
    #        sort_spec => $sort_spec,
    #        wanted    => $num_wanted,
    #    );
    #}
    #else {
    #    $hit_q = Lucy::Search::HitQueue->new( wanted => $num_wanted, );
    #}



    for ( my $i = 0; $i < scalar(@$responses); $i++ ) {
        #my $base           = $starts->get($i);
        my $sub_top_docs   = $responses->[$i];
        my @sub_match_docs = sort { $a->get_doc_id <=> $b->get_doc_id }
            @{ $sub_top_docs->get_match_docs };
        for my $match_doc (@sub_match_docs) {
            $match_doc->set_doc_id( $match_doc->get_doc_id + 0 );
            $hit_q->insert($match_doc);
        }
        $total_hits += $sub_top_docs->get_total_hits;
    }

    # Return a TopDocs object with the best of the best.
    my $best_match_docs = $hit_q->pop_all;
    return Lucy::Search::TopDocs->new(
        total_hits => $total_hits,
        match_docs => $best_match_docs,
    );
}



sub _dispatch_message{
  my $hdl = shift ;
  my $data = shift;
  my $message;
  eval{ $message = thaw $data;};
  if(!$message){
    print "something is wrong with the message!\n";
    return;
  }
  
  
  ## worker telling us the indexes they have
  if($message->{index_status}){
    my $d = $coder->decode($message->{index_status});
    #print Dumper($d);
    $globals{workers}{$hdl}{indexes} = $d;
    update_workers_indexes();    
    return;
  }

  ## worker responding to a question we asked
  if(exists $message->{response}){
    collect_response($hdl,$message);
    return;
  }
  
  #print Dumper($message);  
  my $action =  $message->{_action};
  if(!$action){
    print "Missing a action!\n";
    print Dumper($message);  
    close_client($hdl);  
    return;
  }
  
  
  if($action eq 'searcher_hello'){
    ## worker with indexes lets get a list
    _send(clients =>[$hdl], response =>  { _action => 'index_status' } );
  }
  elsif($action eq 'get_schema'){
    ## print Dumper($globals{schemas}{$message->{_index}});
    _send(clients =>[$hdl], response => $globals{schemas}{$message->{_index}} );
  }
  else{
    my $known_shards =  $globals{indexes}{$message->{_index}};
    if(!$known_shards){
      _send(clients =>[$hdl], status =>"Error: index not know\n");
    }
   
    $message->{_message_id} = $message_id++;
    
    print Dumper($globals{indexes}{$message->{_index}});
    
    print "Sending Message:"  . Dumper($message);
    foreach my $shard (sort {$a <=> $b} keys %{$known_shards->{shards}}){
      my $server_count = scalar @{$known_shards->{shards}{$shard}};
      my $index = int(rand($server_count));
      my $worker = $globals{clients}{$known_shards->{shards}{$shard}[$index]{handle}}{handle};
      my $index_name = $known_shards->{shards}{$shard}[$index]{name};
      $message->{_index} = $index_name; 
      _send(clients =>[$worker], response => $message );
      push @{$globals{response}{$message->{_message_id}}} ,{
                                                    action => $message->{_action},
                                                    shard => $shard, 
                                                    index_name => $index_name,
                                                    respond_to => $hdl, 
                                                    };  
    }
  }
}




sub close_client{
  my $hdl = shift @_;
  print "Closing Client\n";
  #print Dumper($hdl);
  $globals{cur_clients}--;
  delete $globals{clients}{$hdl};
  if(defined $globals{workers}{$hdl}){
    delete $globals{workers}{$hdl};
    update_workers_indexes();    
  }
  $hdl->destroy;
}

sub _send {
  my %args = @_;
  eval {
    my $clients    = delete $args{clients};
    my $serialized = nfreeze(\%args);
    my $len = pack( 'N', bytes::length($serialized) );
    foreach my $hdl (@{$clients}) {
      $hdl->push_write($len . $serialized);
    }
  };
  if ($@) {
    print Dumper($@);
    return;
  }
  return 1;
}


