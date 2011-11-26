use strict;

use Data::MessagePack;
use ZeroMQ::Constants qw/:all/;
use ZeroMQ::Raw;

my $mp = Data::MessagePack->new();


my $context = ZeroMQ::Context->new();
my $context = zmq_init(1);

# Socket to talk to server
my $requester = zmq_socket( $context, ZMQ_REP);
#my $rv   = zmq_setsockopt( $requester, ZMQ_HWM, 100);
#print "ZMQ_HWM\t$rv\n";
my $rv   = zmq_setsockopt( $requester, ZMQ_LINGER, -1);
print "ZMQ_Linger\t$rv\n";



my $rv   = zmq_bind( $requester, "tcp://127.0.0.1:9905" );
my $got_data=1;
my $count;
while($got_data) {
    # Wait for next request from client
    my $string  = zmq_msg_data(zmq_recv( $requester));

    print "Received request: [$string]\n";
    sleep 5;
    send_data($requester,{_action => 'index_status'});
    print "Sent Reply\n"; 

}
zmq_close($requester);
zmq_term($context);

sub send_data{
  my($socket,$data)  = @_;
  my $rv = eval {
      my $msg  = zmq_msg_init_data( $mp->pack($data) );
      return  zmq_send( $requester, $msg );
    };
    print "ERROR:$@\n";
    
  return $rv;
}

