use strict;
use warnings;
use Lucy;
use Data::Dumper;
use ZeroMQ::Raw;
use ZeroMQ::Constants qw/:all/;
use Data::MessagePack;

$|++;

use Getopt::Long;

my $debug                  = 0;
my $down_hostport          = '127.0.0.1:9905';
my $client_listen_hostport = '127.0.0.1:9900';
my $upstream_hostport;
&GetOptions('debug'             => \$debug,
            'upstream_hostport' => \$upstream_hostport,
            'down_hostport'     => \$down_hostport,);

my $mp      = Data::MessagePack->new();
my $context = zmq_init();
my $requester;    ## upstream

my %global_state;
$global_state{last_seen_node} = 0;

my $rand_id = int(rand(1_000_000));

my %socks_to_watch;

my $down_stream = zmq_socket($context, ZMQ_REP);
print zmq_setsockopt($down_stream, ZMQ_LINGER, -1);

print zmq_bind($down_stream, 'tcp://' . $down_hostport);

$socks_to_watch{downstream} = {
  socket   => $down_stream,
  events   => ZMQ_POLLIN,
  callback => sub {
    print "HERE\n";
    while (my $msg = zmq_recv($down_stream, ZMQ_RCVMORE)) {
      $global_state{last_seen_node} = time;
      my $data = zmq_msg_data($msg);
      print "down RECEV:$data\n";
      #my $resp = dispatch($data);
      #send_data($down_stream, $resp);
    }
  },};

while (1) {

  if ($upstream_hostport) {
    print "Checking Heartbeat on $upstream_hostport\n" if $debug;
    ## every N seconds or so this guys will try to
    ## reconnect with the node and get back in sync
    if (time - $global_state{last_seen_node} > 10) {
      print "Sent Hello to node\n" if $debug;
      $global_state{last_seen_node} = time;
      zmq_close($requester) if $requester;
      $requester = zmq_socket($context, ZMQ_REQ);
      my $rv = zmq_setsockopt($requester, ZMQ_LINGER, 0);
      zmq_connect($requester, 'tcp://' . $upstream_hostport);
      send_data($requester, {_action => 'hello'});

    }

    ## poll the nodes requests
    $socks_to_watch{upstream} = {
      socket   => $requester,
      events   => ZMQ_POLLIN,
      callback => sub {
        while (my $msg = zmq_recv($requester, ZMQ_RCVMORE)) {
          $global_state{last_seen_node} = time;
          my $data = zmq_msg_data($msg);

          #my $resp = dispatch($data);
          #send_data($requester, $resp);
        }
      },};
  }

  my @items_to_watch = values %socks_to_watch;
  my $rv = zmq_poll(\@items_to_watch, 1_000_000_000);
}
zmq_close($requester);

exit;

sub send_data {
  my ($socket, $data) = @_;
  $data->{_worker_id} = $$ . "_$rand_id";
  return eval {
    my $msg = zmq_msg_init_data($mp->pack($data));
    return zmq_send($requester, $msg);
  };
}

