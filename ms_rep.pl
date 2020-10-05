#!/usr/bin/perl
# perl script that creates a datasource as a subset of an original datasource by grepping lines from a file (usually a log file that contains variants that did not get added the first time)
# mainly can be used as a template for adhoc perl scripts
# For extracting MSExome long ids: perl -ne 'print if /Data too long for column \047id\047/;' log_file | perl -ne '/(skipping )(.+)(: 1406)/ && print "$2\n";'
use strict;

my $logfile = shift or die "Usage: $0 <log file> <source (chip) file>\n";
my $sourcefile = shift or die "Usage: $0 <log file> <source (chip) file>\n";
$0 =~ s/\.pl//; $0 =~ tr/.\///d;
my $out = "$0_output.txt";
open(lf, '<', $logfile) or die $!;
open(sf, '<', $sourcefile) or die $!;
open(out,'>',$out) or die $!;
my $header = <sf>;
print out $header;
close(sf);
while(<lf>){
    $_ =~ /(skipping )(.+)(: 1406)/ || next;
    my $id = $2;
    open(sf, '<', $sourcefile) or die $!;
    print out grep { $_ =~ /$id/ } <sf>;
    close(sf)
}
close(lf);
close(out);
