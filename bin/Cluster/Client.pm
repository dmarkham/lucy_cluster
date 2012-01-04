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
  #print Dumper(\%seen);
  
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
      on_timeout => sub {
        my ($hdl, $fatal, $msg) = @_;
        $msg = "" unless $msg; 
        warn "Connect Timeout Error:($host_port) $msg";
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
  my $index = shift;
  my $handle = $self->pick_endpoint(); 
  return unless $handle;
  my $cv   = AnyEvent->condvar;
  $handle->{cv} = $cv;
  #print Dumper($handle); 
  $handle->timeout(20);
  $self->ask(clients => [$handle], '_action' => 'get_schema', _index => $index ); 
  #print "Sent\n";
  
  #print Dumper($handle);
  $cv->recv;
  $handle->timeout(0);
  my $result = $self->get_result($handle);
  my $schema = Lucy::Plan::Schema->new;
  $schema = $schema->load($result->{response});
  my $searcher = Cluster::Client::Searcher->new(cluster_client => $self, schema => $schema, index => $index);
  
  return $searcher;
}

sub get_result{
  my $self = shift;
  my $handle = shift;
  my $result = delete $globals{$self}{clients}{$handle}{response};
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

sub ask {
  my $self = shift;
  my %args = @_;
  eval {
    my $clients    = delete $args{clients};
    my $async    = delete $args{async};
    my $serialized = nfreeze(\%args);
    my $len = pack( 'N', bytes::length($serialized) );
    foreach my $hdl (@{$clients}) {
      $hdl->push_write($len . $serialized);
      my $result;
      $hdl->on_read (sub {
        # some data is here, now queue the length-header-read (4 octets)
        shift->unshift_read (chunk => 4, sub {
           # header arrived, decode
           my $len = unpack "N", $_[1];
           # now read the payload
           shift->unshift_read (chunk => $len, sub {
           eval{
              $result = thaw $_[1];
           }; 
           $globals{$self}{clients}{$hdl}{response} = $result;
           $hdl->{cv}->send() if $hdl->{cv};
          });
       });
     }); 

    }
  };
  
  if ($@) {
    print "ERROR:" . Dumper($@);
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
  #print Dumper($removed);
  if($hdl->{cv}){
    $hdl->{cv}->send;
  }
  $hdl->destroy;
}

sub wait {
  my $self = shift;
  AnyEvent->condvar->recv;
}

sub DESTROY {
  my $self = shift;
  delete $globals{$self};
}

1;


package Cluster::Client::Searcher;

BEGIN { our @ISA = qw( Lucy::Search::Searcher ) }
use Lucy;
use Carp;
use AnyEvent;
use strict;


my %global;
sub new {
    my ( $either, %args ) = @_;
    my $cc = delete $args{cluster_client};
    my $index = delete $args{index};
    my $self         = $either->SUPER::new(%args);
    $global{$$self}{cc}     = $cc;
    $global{$$self}{index}     = $index;
    
    confess("No cluster client: $!") unless $cc;
    return $self;
}

sub _rpc{
  my $self = shift;
  my $args = shift;
  my $cc = $global{$$self}{cc};
  my $index = $global{$$self}{index};
  my $handle = $cc->pick_endpoint(); 
  return unless $handle;
  my $cv   = AnyEvent->condvar;
  $handle->{cv} = $cv;
  $handle->timeout(20);
  $cc->ask(%{$args}, clients => [$handle],  _index => $index ); 
  #print "Sent\n";
  
  #print Dumper($handle);
  $cv->recv;
  $handle->timeout(0);
  my $result = $cc->get_result($handle);
  return $result;
}

sub top_docs {
    my $self = shift;
    my %args = ( @_, _action => 'top_docs' );
    return $self->_rpc( \%args );
}

sub fetch_doc {
    my ( $self, $doc_id ) = @_;
    my %args = ( doc_id => $doc_id, _action => 'fetch_doc' );
    return $self->_rpc( \%args );
}

sub fetch_doc_vec {
    my ( $self, $doc_id ) = @_;
    my %args = ( doc_id => $doc_id, _action => 'fetch_doc_vec' );
    return $self->_rpc( \%args );
}

sub doc_max {
    my $self = shift;
    my %args = ( _action => 'doc_max' );
    return $self->_rpc( { _action => 'doc_max' } );
}

sub doc_freq {
    my $self = shift;
    my %args = ( @_, _action => 'doc_freq' );
    return $self->_rpc( \%args );
}




1;

