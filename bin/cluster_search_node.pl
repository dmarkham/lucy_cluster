use strict;
use warnings;
use Lucy;
use AnyEvent::Socket qw/tcp_server parse_hostport/;
use AnyEvent::Handle;
use AnyEvent::Strict;
use Data::Dumper;
use Storable qw( nfreeze thaw );
use bytes;
no bytes;

use Getopt::Long;

my $debug                  = 0;
my $node_id = join("_",$$,int(rand(1_000_000))); 

my $client_hostport = '127.0.0.1:9905';
&GetOptions('debug'             => \$debug,
            'client_hostport'     => \$client_hostport);



my %globals = (max_clients => 2000,
               cur_clients => 0,
              );


my ($host,$port) = parse_hostport($client_hostport);
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
      on_read => sub{
        my ($hdl) = @_;
        print "BUFF:$hdl->{rbuf}\n"; 
        if(!$globals{clients}{$hdl}{read_size} && bytes::length($hdl->{rbuf}) >= 4){
          my $message_size = substr $hdl->{rbuf},0,4,'';
          my $message_length = unpack( 'N', $message_size );
          if($message_length < 5_000_000){ 
            $globals{clients}{$hdl}{read_size} = $message_length;
          }
          else{
            print "Message to big closing ($message_length)\n";
            close_client($hdl);  
            return;
          }
        }
        if($globals{clients}{$hdl}{read_size} && bytes::length($hdl->{rbuf}) >= $globals{clients}{$hdl}{read_size}){
          _dispatch_message($hdl);
        }
      },
      timout =>0,;
   $globals{cur_clients}++;
   $globals{clients}{$handle}{handle} = $handle;
};

AnyEvent->condvar->recv;

sub _dispatch_message{
  my $hdl = shift @_;
  my $data = delete $hdl->{rbuf};
  my $message;
  eval{ $message = thaw $data;};
  if(!$message){
    print "Something is wrong with the message!\n";
    return;
  }
  _send(clients =>[$hdl], result => "Thank You");
  print Dumper($message);  
}

sub close_client{
  my $hdl = shift @_;
  print "Closing Client\n";
  print Dumper($hdl);
  $globals{cur_clients}--;
  delete $globals{clients}{$hdl};
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












