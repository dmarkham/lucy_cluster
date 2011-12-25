use strict;
use warnings;
use Lucy;
use Getopt::Long;
use Data::Dumper;
use File::Slurp;
use AnyEvent::Socket qw/parse_hostport/;
use AnyEvent::Handle;
use Storable qw( nfreeze thaw );



## This is who I will be getting my work from
my $my_node_hostport = "127.0.0.1:9905";

## default place where 1 or more Lucy indexes live
my $index_dir = "/tmp/indexes/";

my $debug = 0;
&GetOptions('debug'         => \$debug,
            'node_hostport' => \$my_node_hostport,
            'index_dir'     => \$index_dir,);


my %global_state;
$global_state{last_seen_node} = 0;


my $handle = get_connect(host_port => $my_node_hostport);

## connect back up
my $w = AE::timer 5, 5, sub { 
    if($handle->destroyed){
      $handle = get_connect(host_port => $my_node_hostport);
    } };


AnyEvent->condvar->recv;


## do the work after getting a message
sub dispatch {
  my $args = shift;
  my $data = $args->{response};
  my %allowed_lucy_methods = (doc_max       => 1,
                              doc_freq      => 1,
                              top_docs      => 1,
                              fetch_doc     => 1,
                              fetch_doc_vec => 1,
                              get_schema    => 1,);

  return {status => "error missing data unpack fail?"} unless $data;
  print "Recived: " . Dumper($data) if $debug;
  my $method = delete $data->{_action};
  return {status => "error missing method"} unless $method;
  
  if ($method eq 'index_status') {
    ## read a json file from disk some other system keeping
    ## up to date and send it. This file will let the Node server
    ## know what index/shards i have
    my $utf_text = "";
    eval {$utf_text = read_file("$index_dir/index_list.json", binmode => ':utf8')};
    return {status => "error issue with index_list,json ($@)"} unless $utf_text;
    return { index_status => $utf_text};
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


sub get_connect{
  my %args = @_;
  my $host_port = $args{host_port};
  return unless $host_port;

  my ($host, $port) = parse_hostport($host_port);
  
  my $handle = new AnyEvent::Handle
      connect   => [$host, $port],
      keepalive => 1,
      timeout   => 0,
      on_error  => sub {
        my ($hdl, $fatal, $msg) = @_;
        $msg = "" unless $msg; 
        warn "Error:($host_port) $msg";
      },
      on_connect_error => sub {
        my ($hdl, $fatal, $msg) = @_;
        $msg = "" unless $msg; 
        warn "Connect Error:($host_port) $msg";
      },
      on_timeout => sub {
        my ($hdl, $fatal, $msg) = @_;
        $msg = "" unless $msg; 
        warn "Connect Timeout Error:($host_port) $msg";
      },
      on_connect => sub {
        my $hdl = shift;
        
        my $serialized = nfreeze( {_action => 'searcher_hello'} );
        my $len = pack( 'N', bytes::length($serialized) );
        $hdl->push_write($len . $serialized);
      },
      on_read => sub {
        # some data is here, now queue the length-header-read (4 octets)
        shift->unshift_read (chunk => 4, sub {
           
           my $len = unpack "N", $_[1];
           $_[0]->unshift_read (chunk => $len, sub {
             my $data;
             eval{
                $data = thaw $_[1];
             }; 
             if($data){
               my $return = dispatch($data);
               my $serialized = nfreeze( $return  );
               my $len = pack( 'N', bytes::length($serialized) );
               $_[0]->push_write($len . $serialized);
             }
          });
       });
     }, 
     on_prepare => sub {
        5;
      };

  return $handle;
}







