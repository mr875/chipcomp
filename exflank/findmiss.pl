#!/usr/bin/perl
# perl script for use as a template. 2 args (files). what lines are in file 1 that are not in file 2
use strict;

my $allrsids = shift or die "Usage: $0 <full rsid list> <already collected flanks>\n";
my $donersids = shift or die "Usage: $0 <full rsid list> <already collected flanks>\n";
$0 =~ s/\.pl//; $0 =~ tr/.\///d;
my $out = "$0_rsids.txt";
open(ar, '<', $allrsids) or die $!;
open(out,'>',$out) or die $!;
my $header = <ar>;
while(<ar>){
    chomp;
    my $id = "$_\t";
    open(dr, '<', $donersids) or die $!;
    if (grep { $_ =~ /$id/ } <dr>){
        ;
    } else {
        print out "$_\n";
    }
    close(dr)
}
close(ar);
close(out);
