# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

use strict;
use warnings;

package ClusterNode;
BEGIN { our @ISA = qw( Lucy::Search::Searcher ) }
use Carp;
use Storable qw( nfreeze thaw );
use Data::MessagePack;
use ZeroMQ::Constants qw/:all/;
use ZeroMQ::Raw;
use Data::Dumper;


my $mp = Data::MessagePack->new();
my $context = zmq_init(1);



my %global_state;

sub new {
    my ( $either, %args ) = @_;
    
    my $index = delete $args{index_name};
    return unless $index;
    my $hostport = delete $args{hostport};
    $hostport = "127.0.0.1:9905" unless $hostport; 
    
    my $self  = $either->SUPER::new(%args);
    $global_state{$$self}->{index} = $index; 
    $global_state{$$self}->{hostport} = $hostport; 
    my $requester = zmq_socket( $context, ZMQ_REP);
    zmq_setsockopt( $requester, ZMQ_LINGER, -1);
    zmq_bind( $requester, 'tcp://'  . $hostport);
    return unless $hostport;
    $global_state{$$self}->{socket} = $requester; 
    
    return $self;
}

sub DESTROY {
    my $self = shift;
    zmq_close($global_state{$$self}->{socket}) if $global_state{$$self}->{socket};
    delete $global_state{$$self};
    $self->SUPER::DESTROY;
}


sub _rpc {
  my ( $self, $lucy_args ) = @_;
  

  my %args;
  $args{_index} = $global_state{$$self}->{index};
  $args{_uuid} = $$;
  $args{_action} = delete $lucy_args->{_action};
  $lucy_args = nfreeze($lucy_args);
  $args{lucy_args} = $lucy_args;

  ## some smart logic to send querys to 1 copy of each shard 
  ##
  print Dumper(\%args);
  send_data($global_state{$$self}->{socket},\%args);
  zmq_poll([
            {
              socket => $global_state{$$self}->{socket},
              events => ZMQ_POLLIN,
              callback => sub {
                while ( my $msg = zmq_recv( $global_state{$$self}->{socket}, ZMQ_RCVMORE ) ) {
                  my $data = $mp->unpack(zmq_msg_data($msg));
                  print Dumper($data);
                }
              },
            } 
        ], 15_000_000);

  

}

sub top_docs {
    my $self = shift;
    my %args = ( @_, _action => 'top_docs' );
    print Dumper(\%args);
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
    return $self->_rpc( \%args );
}

sub doc_freq {
    my $self = shift;
    my %args = ( @_, _action => 'doc_freq' );
    return $self->_rpc( \%args );
}

sub send_data{
  my($socket,$data)  = @_; 
  return  eval {
      my $msg  = zmq_msg_init_data( $mp->pack($data) );
      return  zmq_send( $socket, $msg );
    };  
}


1;

