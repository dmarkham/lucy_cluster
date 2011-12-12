package Cluster::Client;
use strict;
use warnings;
use AnyEvent::Socket qw/parse_hostport/;
use AnyEvent::Handle;
use Data::Dumper;
use Storable qw( nfreeze thaw );

my %globals;

sub new {
  my ($thing, %args) = @_;

  my $class = ref($thing) || $thing;
  my $self = {endpoints => $args{endpoints}};
  $self = bless $self, $class;
  $self->connect;
  return $self;
}

sub connect{
  my $self = shift;

  my %seen;
  foreach my $h (keys %{$globals{$self}{clients}}){
    my $handle = $globals{$self}{clients}{$h}{handle};  
    my $con = join(":",$handle->{connect});
    $seen{$con} =1;
  }
  print Dumper(\%seen);
  
  foreach my $host_port (@{$self->{endpoints}}) {
    my ($host, $port) = parse_hostport($host_port);
    if($seen{"$host:$port"}){
      print "$host:$port is all good\n";
      next;
    }
    my $handle = new AnyEvent::Handle
      connect   => [$host, $port],
      keepalive => 1,
      timeout   => 0,
      on_error  => sub {
      my ($hdl, $fatal, $msg) = @_;
      $msg = "" unless $msg; 
      warn "Error:($host_port) $msg";
      close_client($hdl,$self);
      },
      on_connect_error => sub {
      my ($hdl, $fatal, $msg) = @_;
      $msg = "" unless $msg; 
      warn "Connect Error:($host_port) $msg";
      close_client($hdl,$self);
      },
      on_prepare => sub {
      5;
      };
    $globals{$self}{clients}{$handle}{handle} = $handle;
  }
  #print Dumper(\%globals);
}


sub get_searcher {
  my $self = shift;
  my $cv   = AnyEvent->condvar;
  my $result;
  my $handle = $self->pick_endpoint(); 
  #print Dumper($handle); 
  $handle->timeout(20);
  _send(clients => [$handle],'_action' => 'hello' ); 
  $handle->on_read (sub {
      # some data is here, now queue the length-header-read (4 octets)
      shift->unshift_read (chunk => 4, sub {
          # header arrived, decode
          my $len = unpack "N", $_[1];
          # now read the payload
          shift->unshift_read (chunk => $len, sub {
          eval{
              $result = thaw $_[1];
          }; 
          $cv->send;
         });
      });
   }); 

  $cv->recv;
  $handle->timeout(0);
  return $result;
}

sub pick_endpoint {
  my $self = shift;
  
  foreach my $ep ( keys $globals{$self}{clients}){
    #print Dumper($globals{$self}{clients}{$ep}{handle});
    return $globals{$self}{clients}{$ep}{handle};
  }
  return;
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

sub close_client {
  my $hdl = shift @_;
  my $self = shift @_;
  print "Closing Connection\n";
  #print Dumper($hdl);
  my $removed = delete $globals{$self}{clients}{$hdl};
  print Dumper($removed);
  $hdl->destroy;
}

sub wait {
  my $self = shift;
  AnyEvent->condvar->recv;
}

sub DESTROY {
  my $self = shift;
  #print "DESTROY:\n";
  foreach my $hdl (@{$globals{$self}{clients}}) {
    my $handle = $globals{$self}{clients}{$hdl}{handle};
    close_client($handle,$self);
  }
  delete $globals{$self};
}

1;

