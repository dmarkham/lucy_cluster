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

use Getopt::Long;

my $debug                  = 0;
my $node_id = join("_",$$,int(rand(1_000_000))); 
my $coder = JSON::XS->new->utf8();

my $client_hostport = '0:9905';
&GetOptions('debug'             => \$debug,
            'client_hostport:s'     => \$client_hostport);



my %globals = (max_clients => 2000,
               cur_clients => 0,
              );


my ($host,$port) = parse_hostport($client_hostport);
print "vars: $host\t$port\n";
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
        push @{$index_to_shards{$index}{$data->{shard}}} , $hdl; 
        $index_to_schemas{$index} = $data->{schema};
      }
    }
  }
  print Dumper(\%index_to_shards);
  $globals{indexes} = \%index_to_shards;
  $globals{schemas} = \%index_to_schemas;
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
  
  
  if($message->{index_status}){
    my $d = $coder->decode($message->{index_status});
    #print Dumper($d);
    $globals{workers}{$hdl}{indexes} = $d;
    update_workers_indexes();    
    return;
  }

  #print Dumper($message);  
  my $action = delete $message->{_action};
  if(!$action){
    print "Missing a action!\n";
    close_client($hdl);  
    return;
  }
  
  
  if($action eq 'searcher_hello'){
    ## worker with indexes lets get a list
    _send(clients =>[$hdl], response =>  { _action => 'index_status' } );
  }
  elsif($action eq 'get_schema'){
    print Dumper($globals{schemas}{$message->{index}});
    _send(clients =>[$hdl], response => $globals{schemas}{$message->{index}} );
  }
  else{
    print "no known action:$action\n";
    close_client($hdl);  
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


