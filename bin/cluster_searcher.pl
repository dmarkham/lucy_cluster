use strict;
use warnings;
use Lucy;
use Data::MessagePack;
use ZeroMQ::Raw;
use Getopt::Long;
use ZeroMQ::Constants qw/:all/;
use Data::Dumper;

print ZeroMQ::version() . "\n";

##  This Script is a Dumb generic Searcher it can search any index
##  in the index_dir you tell this script who to connect up to
##  and it's a blocking question/response  handshake
## we set up 2 channels to the node.
## one for queries. The other for control/heartbeat  


my $debug = 0;

## This is who I will be getting my work from 
my $my_node_hostport = "127.0.0.1:9905";
my $my_hostport = "127.0.0.1:9600";
my $mp = Data::MessagePack->new();

my $index_dir = "/tmp/indexes/";


&GetOptions(
   'debug' => \$debug, 
   'node_hostport' => \$my_node_hostport, 
   'my_hostport' => \$my_hostport, 
   'index_dir' => \$index_dir, 
);

print "$my_hostport\t$my_node_hostport\t$debug\n";


my $context   = zmq_init();
my $requester = zmq_socket( $context, ZMQ_REQ);
my $control  = zmq_socket( $context, ZMQ_REP);

## we listen for commands on this port
my $rv   = zmq_bind( $control, 'tcp://' . $my_hostport );

## we send a hello and then wait for  querys
$rv   = zmq_connect( $requester, 'tcp://' . $my_node_hostport );

send_data($requester,{control_hostport =>$my_hostport});

zmq_poll([
        {
            socket => $requester,
            events => ZMQ_POLLIN,
            callback => sub {
                      print "HERE\n";
                      while ( my $msg = zmq_recv( $requester, ZMQ_RCVMORE) ) {
                        my $string  = zmq_msg_data($msg);
                        print "GOT:$string\n";
                      }
                  }, 
        },
    ], 0 );
sleep 5;


zmq_close($requester);
zmq_close($control);

print "Here\n";

sub send_data{
  my($socket,$data)  = @_;
  my $rv = eval {
      my $msg  = zmq_msg_init_data( $mp->pack($data) );
      return  zmq_send( $requester, $msg );
    };
    
  return $rv;
}






