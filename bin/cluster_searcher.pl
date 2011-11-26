use strict;
use warnings;
use Lucy;
use Getopt::Long;
use Data::MessagePack;
use ZeroMQ::Raw;
use ZeroMQ::Constants qw/:all/;
use AnyEvent;
use AnyEvent::Loop;
use Storable qw( nfreeze thaw );
use File::Slurp;
use Data::Dumper;

##  This Script is a Dumb generic Searcher it can search any index
##  in the index_dir you tell this script who to connect up to
##  and it's a blocking question/response  handshake

## flow should look like this
## ping  node (it's boss) with a heartbeat
## node askes searcher about what he has
## node starts sending querys to this searcher (that he can help on)
##

## This is who I will be getting my work from 
my $my_node_hostport = "127.0.0.1:9905";
my $mp = Data::MessagePack->new();


## place where 1 or more Lucy indexes live
my $index_dir = "/tmp/indexes/";


my $debug = 0;
&GetOptions(
   'debug' => \$debug, 
   'node_hostport' => \$my_node_hostport, 
   'index_dir' => \$index_dir, 
);


## our connection to the Node
my $context   = zmq_init();
my $requester = zmq_socket( $context, ZMQ_REQ);
zmq_connect( $requester, 'tcp://' . $my_node_hostport );


## i can only do 1 query at a time.
## but if the server doesnt finish a query 
## i'll need a flag and a way to clean up.
## this will also help me cordinate a reopen
## while not being in then middle of a search

my %global_state;
$global_state{last_seen_node} = time - 60;

my $rand_id = int(rand(1_000_000));

## every 5 seconds check our node
my $heartbeat = AE::timer 10, 5, sub { 
      print "Checking Heartbeat\n";
      if(time - $global_state{last_seen_node} > 20){
        print "Sent Hello\n";
        $global_state{last_seen_node} = time;
        zmq_close($requester);
        $requester = zmq_socket( $context, ZMQ_REQ);
        zmq_connect( $requester, 'tcp://' . $my_node_hostport );
        send_data($requester,{_action => 'hello'});
      }
      else{
        print "Still chatting\n";
      }
    }; 


my $requester_fh = zmq_getsockopt( $requester, ZMQ_FD );
my $w; $w = AE::io $requester_fh, 1, sub {
        while ( my $msg = zmq_recv( $requester, ZMQ_RCVMORE ) ) {
          $global_state{last_seen_node} = time;
          my $data  = zmq_msg_data($msg);
          my $resp = dispatch($data);
          send_data($requester, $resp);
        }
    };


## this kicks off eveything i have to ping the node first!
## after i say hello he will ask me to do work for him
## after he figures out what skills and data i have
send_data($requester,{_action => 'hello'});
AnyEvent::Loop::run;
zmq_close($requester);

exit;


## do the work after getting a message
sub dispatch{
  my $arg = shift;
  
  my %allowed_lucy_methods = (
    doc_max       => 1,
    doc_freq      => 1,
    top_docs      => 1,
    fetch_doc     => 1,
    fetch_doc_vec => 1,
  );

  my $data;
  eval{ $data = $mp->unpack($arg); };
  return {status => 'error'} unless $data;
  print Dumper($data);
  my $method = delete $data->{_action};

  if($method eq 'index_status'){
    ## read a json file from disk some other system keeping
    ## up to date and send it. This file will let the Node server
    ## know what index/shards  i have
    my $utf_text = "";
    eval{$utf_text= read_file( "$index_dir/index_list.json", binmode => ':utf8' )} ;
    return {index_status => $utf_text };
  }
  
  my $searcher = Lucy::Search::IndexSearcher->new( index => '/path/to/index' );

  
  if($allowed_lucy_methods{$method}){
    my $response   = $searcher->$method();
    my $frozen     = nfreeze($response);
  } 
  
}




sub send_data{
  my($socket,$data)  = @_;
  $data->{_worker_id} = $$ . "_$rand_id";
  my $rv = eval {
      my $msg  = zmq_msg_init_data( $mp->pack($data) );
      return  zmq_send( $requester, $msg );
    };
    
  return $rv;
}

