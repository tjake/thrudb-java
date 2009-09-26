#!/usr/bin/perl

use strict;
use warnings;

use Parse::MediaWikiDump;


use lib 'gen-perl';

use Thrift;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::BinaryProtocol;
use Thrudex::Thrudex;
use Data::Dumper;

binmode(STDOUT, ':utf8');
binmode(STDERR, ':utf8');


my $socket    = new Thrift::Socket ('localhost', 11299);
my $transport = new Thrift::FramedTransport ($socket);
my $protocol  = new Thrift::BinaryProtocol ($transport);
my $client    = new Thrudex::ThrudexClient ($protocol);

$socket->setRecvTimeout(50000);

$transport->open;

$client->admin("create_index", "wikipedia");

my $dump = Parse::MediaWikiDump::Pages->new(\*STDIN);


my $counter = 0;
while(my $page = $dump->next) {

    $counter++;

    warn("Indexed $counter\n");# if $counter % 1000 == 0;

    my $text = $page->text;

    my $doc = new Thrudex::Document();
    $doc->index("wikipedia");
    $doc->key($page->title);
    $doc->payload($$text);

    my $field = new Thrudex::Field();
    $field->key( "title" );
    $field->value( $page->title );
    $field->sortable(1);

    push(@{$doc->{fields}}, $field);

    $field = new Thrudex::Field();
    $field->key( "body" );
    $field->value( $$text );

    push(@{$doc->{fields}}, $field);

    eval{
        $client->put($doc);
    }; if($@){
        warn(Dumper($@));
        exit(0);
    }
}
warn("Finished indexing $counter articles\n");


