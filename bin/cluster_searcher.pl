use strict;
use warnings;
use Lucy;
use Getopt::Long;
use Data::MessagePack;
use ZeroMQ::Raw;
use ZeroMQ::Constants qw/:all/;
use AnyEvent;
use Data::Dumper;

##  This Script is a Dumb generic Searcher it can search any index
##  in the index_dir you tell this script who to connect up to
##  and it's a blocking question/response  handshake

## flow should look like this
## ping  node (it's boss) with a heartbeat
## searcher tells the node about what he has
## node starts sending querys to this searcher (that he can help on)
##



my $debug = 0;

## This is who I will be getting my work from 
my $my_node_hostport = "127.0.0.1:9905";
my $mp = Data::MessagePack->new();


## place where 1 or more Lucy indexes live
my $index_dir = "/tmp/indexes/";


&GetOptions(
   'debug' => \$debug, 
   'node_hostport' => \$my_node_hostport, 
   'index_dir' => \$index_dir, 
);


## our connection to the Node
my $context   = zmq_init();
my $requester = zmq_socket( $context, ZMQ_REQ);
my $rv   = zmq_connect( $requester, 'tcp://' . $my_node_hostport );


## i can only do 1 query at a time.
## but if the server doesnt finish a query 
## i'll need a flag and a way to clean up.
## this will also help me cordinate a reopen
## while not being in then middle of a search

my %global_state;



my $requester_fh = zmq_getsockopt( $requester, ZMQ_FD );
my $w; $w = AE::io $requester_fh, 1, sub {
        while ( my $msg = zmq_recv( $requester, ZMQ_RCVMORE ) ) {
          my $data  = zmq_msg_data($msg);
          my $resp = dispatch($data);
          send_data($requester, $resp);
        }
    };



## this kicks off eveything i have to ping the node first!
## after i say hello he will ask me to do work for him
## after he figuers out what skills and data i have
send_data($requester,{_action => 'hello'});

AnyEvent->condvar->recv;

zmq_close($requester);

exit;


## do the work after getting a message
sub dispatch{
  my $arg = shift;
  
  my %allowed_lucy_methods = (
    doc_max       => \&do_doc_max,
    doc_freq      => \&do_doc_freq,
    top_docs      => \&do_top_docs,
    fetch_doc     => \&do_fetch_doc,
    fetch_doc_vec => \&do_fetch_doc_vec,
  );


  my $data;
  #eval{ $data = $mp->unpack($arg); };
  return {status => 'error'} unless $data;
  my $method = delete $data->{_action};

    
  #my $searcher = Lucy::Search::IndexSearcher->new( 
  #    index => '/path/to/index' 
  #);



  
  if($allowed_lucy_methods{$method}){
    my $response   = $allowed_lucy_methods{$method}->(   );
    #my $frozen     = nfreeze($response);
  } 

 


  
}

sub send_data{
  my($socket,$data)  = @_;
  my $rv = eval {
      my $msg  = zmq_msg_init_data( $mp->pack($data) );
      return  zmq_send( $requester, $msg );
    };
    
  return $rv;
}

