use strict;
use warnings;

use Lucy;
use Getopt::Long;
use ZeroMQ::Raw;
use ZeroMQ::Constants qw/:all/;

use Storable qw( nfreeze thaw );
use Data::MessagePack;
use File::Slurp;
use Data::Dumper;

##  This Script is a Dumb generic Searcher it can search any index
##  in the index_dir you tell this script who to connect up to
##  and it's a blocking question/response  handshake

## flow should look like this
## ping  node (it's boss) with a heartbeat
## node askes searcher about what he has
## node starts sending querys to this searcher (that he can help on)

## This is who I will be getting my work from
my $my_node_hostport = "127.0.0.1:9905";
my $mp               = Data::MessagePack->new();

## place where 1 or more Lucy indexes live
my $index_dir = "/tmp/indexes/";

my $debug = 0;
&GetOptions('debug'         => \$debug,
            'node_hostport' => \$my_node_hostport,
            'index_dir'     => \$index_dir,);

## our connection to the Node
my $context = zmq_init();
my $requester;

my %global_state;
$global_state{last_seen_node} = 0;

my $rand_id = int(rand(1_000_000));

while (1) {

  print "Checking Heartbeat on $my_node_hostport\n" if $debug;

  ## every N seconds or so this guys will try to
  ## reconnect with the node and get back in sync
  if (time - $global_state{last_seen_node} > 10) {
    print "Sent Hello to node\n" if $debug;
    $global_state{last_seen_node} = time;
    zmq_close($requester) if $requester;
    $requester = zmq_socket($context, ZMQ_REQ);
    my $rv = zmq_setsockopt($requester, ZMQ_LINGER, 0);
    zmq_connect($requester, 'tcp://' . $my_node_hostport);
    send_data($requester, {_action => 'hello'});
  }

  ## poll the nodes requests
  zmq_poll(
    [
     {socket   => $requester,
      events   => ZMQ_POLLIN,
      callback => sub {
        while (my $msg = zmq_recv($requester, ZMQ_RCVMORE)) {
          $global_state{last_seen_node} = time;
          my $data = zmq_msg_data($msg);
          my $resp = dispatch($data);
          send_data($requester, $resp);
        }
      },
     }
    ],
    500_000);
}

zmq_close($requester);

exit;

## do the work after getting a message
sub dispatch {
  my $arg = shift;

  my %allowed_lucy_methods = (doc_max       => 1,
                              doc_freq      => 1,
                              top_docs      => 1,
                              fetch_doc     => 1,
                              fetch_doc_vec => 1,
                              get_schema    => 1,);

  my $data;
  eval {$data = $mp->unpack($arg);};
  return {status => "error missing data unpack fail? ($@)"} unless $data;
  print "Recived: " . Dumper($data) if $debug;
  my $method = delete $data->{_action};

  if ($method eq 'index_status') {
    ## read a json file from disk some other system keeping
    ## up to date and send it. This file will let the Node server
    ## know what index/shards i have
    my $utf_text = "";
    eval {$utf_text = read_file("$index_dir/index_list.json", binmode => ':utf8')};
    return {status => "error issue with index_list,json ($@)"} unless $utf_text;
    return {index_status => $utf_text};
  }

  my $index = delete $data->{_index};
  return {status => 'error missing _index'} unless $index;

  $index = "$index_dir/$index";
  my $searcher;
  if ($global_state{searchers}{$index}) {
    ## check for need to repopen
    $searcher = $global_state{searchers}{$index}{searcher};
  } else {
    eval {$searcher = Lucy::Search::IndexSearcher->new(index => $index)};
    return {status => "error creating Searcher ($@)"} unless $searcher;

    $global_state{searchers}{$index}{searcher} = $searcher;
    $global_state{searchers}{$index}{created}  = time;
  }

  if ($allowed_lucy_methods{$method}) {
    my $response;
    my $frozen;
    eval {
      my $args = thaw($data->{lucy_args});
      $response = $searcher->$method(%$args);
      if ($method eq 'get_schema') {
        $response = $response->dump();
      }
      $frozen = nfreeze($response);
    };
    return {status => "error search or nfreeeze failed ($@)"} unless $frozen;
    return {response => $frozen};
  }

}

sub send_data {
  my ($socket, $data) = @_;
  $data->{_worker_id} = $$ . "_$rand_id";
  return eval {
    my $msg = zmq_msg_init_data($mp->pack($data));
    return zmq_send($requester, $msg);
  };
}

