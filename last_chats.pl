#!/usr/bin/perl -w

use warnings;
use strict;
use DBI;
use Encode qw( decode_utf8 );
use POSIX;
use File::Copy;
use Date::Manip qw(UnixDate);

my $VERSION = '1.0.0';

my $gaptime = (30 * 60);
my $timezone = strftime('%Z', localtime());

# Fixes unicode dumping to stdio...hopefully you have a utf-8 terminal by now.
binmode(STDOUT, ":utf8");
binmode(STDERR, ":utf8");

my $dbinfo = undef;
my $xmppuser = undef;
my $maildir = undef;
my $maxlines = undef;
my $cmdok = 1;

my $debug = 0;
sub dbgprint {
    print @_ if $debug;
}

sub usage {
    die("USAGE: $0 <dbinfo> <xmppuser> <maildir> <maxlines>\n");
}

# localize to current timezone and return as unix epoch format.
sub make_timestamp {
    my ($str, $timezone) = @_;
    return int(UnixDate("$str $timezone", '%s'));
}

sub split_date_time {
    my $timestamp = shift;
    my $date = UnixDate("epoch $timestamp", '%Y-%m-%d');
    my $time = UnixDate("epoch $timestamp", '%H:%M');
    dbgprint("split $timestamp => '$date', '$time'\n");
    return ($date, $time);
}

sub utc_to_local {
    my $utc = shift;
    return UnixDate("$utc UTC", '%Y-%m-%d %H:%M:%S %Z');
}

sub fail {
    my $err = shift;
    die("$err\n");
}


my $startid = 0;
my %startids = ();

my %missing_nicknames = ();
my %nicknames = ();
sub get_nickname {
    my $name = shift;
    my $alias = $nicknames{$name};
    if (not defined $alias) {
        if (not defined $missing_nicknames{$name}) {
            $missing_nicknames{$name} = 1;
            #print STDERR "WARNING: No nickname for '$name'\n";
            fail("ERROR: No nickname for '$name'\n");
        }
        $alias = $name;
    }
    my ($short) = $alias =~ /\A(.*?)(\s|\@|\Z)/;
    dbgprint("get_nickname: '$name' => '$alias' => '$short'\n");
    return ($alias, $short);
}

sub signal_catcher {
    my $sig = shift;
    fail("Caught signal ${sig}!");
}
$SIG{INT} = \&signal_catcher;
$SIG{TERM} = \&signal_catcher;
$SIG{HUP} = \&signal_catcher;

# mainline!

foreach (@ARGV) {
    $debug = 1, next if ($_ eq '--debug');
    $debug = 0, next if ($_ eq '--no-debug');
    $dbinfo = $_, next if (not defined $dbinfo);
    $xmppuser = $_, next if (not defined $xmppuser);
    $maildir = $_, next if (not defined $maildir);
    $maxlines = int($_), next if not (defined $maxlines);
    usage();
}
$maxlines = 10 if (not defined $maxlines);
usage() if (not defined $dbinfo);
usage() if (not defined $xmppuser);
usage() if (not defined $maildir);

open DBINFO,'<',$dbinfo or die("Couldn't open '$dbinfo': $!\n");
my ($dsn, $dbuser, $dbpass) = <DBINFO>;
chomp($dsn, $dbuser, $dbpass);
close(DBINFO);

if (open(NICKNAMES,'<',"$maildir/xmpp_nicknames.txt")) {
    while (<NICKNAMES>) {
        chomp;
        if (/\A\s*(.*?)\s*=\s*(.*?)\s*\Z/) {
            $nicknames{$1} = $2;
            dbgprint("Assigned name '$1' to nickname '$2'\n");
        }
    }
    close(NICKNAMES);
}

my ($xmppuserfullalias, $xmppuseralias) = get_nickname($xmppuser);

my $link = DBI->connect($dsn, $dbuser, $dbpass, {
    'RaiseError' => 1,
    'mysql_enable_utf8' => 1
});

my $sql = 'select A.* from (select m.id as msgid, m.utc as msgutc, m.dir,' .
          ' m.body, c.id as cid, c.with_user, c.with_server, c.with_resource,' .
          ' c.utc, c.change_utc' .
          ' from archive_messages as m' .
          ' inner join archive_collections as c on (m.coll_id = c.id)' .
          " where (c.us = '$xmppuser')" .
          ' and m.utc between DATE_SUB(UTC_TIMESTAMP(), interval 1 day) and UTC_TIMESTAMP()' .
          " order by m.id desc limit $maxlines) as A order by A.msgid";
dbgprint("sql = '$sql'\n");
my $sth = $link->prepare($sql);
$sth->execute() or fail "can't execute the query: ${sth->errstr}";

my $lastspeaker = '';
my $lastdate = '';
my $lasttime = '';
my $lastwith = '';
my $alias = undef;
my $thisxmppuseralias = $xmppuseralias;
my $newestmsgid = 0;

$startid = undef;

while (my @row = $sth->fetchrow_array()) {
    my ($msgid, $utc, $dir, $body, $coll_id, $with_user, $with_srvr, $with_rsrc, $coll_utc, $change_utc) = @row;
    $with_rsrc = '' if (not defined $with_rsrc);
    $with_rsrc = '/' . $with_rsrc if ($with_rsrc ne '');
    my $with = "${with_user}\@${with_srvr}";

    if ($debug) {
        dbgprint("New row:\n");
        foreach(@row) {
            dbgprint("  $_\n");
        }
    }

    $newestmsgid = $msgid if ($msgid > $newestmsgid);

    my $person = $with;
    my ($fullalias,$short) = get_nickname($with);

    if ($short ne $xmppuseralias) {
        $alias = $short;
        $thisxmppuseralias = $xmppuseralias;
    } elsif ($fullalias ne $xmppuserfullalias) {
        $alias = $fullalias;
        $thisxmppuseralias = $xmppuserfullalias;
    } else {
        $alias = "$short ($with)";
        $thisxmppuseralias = "$xmppuseralias ($xmppuser)";
    }

    if ($with ne $fullalias) {
        $person = "$fullalias ($with)";
    }

    my $localdate = utc_to_local($coll_utc);

    $lastwith = $with;

    # replace "/me does something" with "*does something*" ...
    $body =~ s#\A/me (.*)\Z#*$1*#;

    my $speaker = $dir ? $xmppuserfullalias : $fullalias;
    my ($d, $t) = split_date_time(make_timestamp($utc, 'UTC'));

    if ($d ne $lastdate) {
        print "\n$d\n";
        $lastspeaker = '';
    }
    print "\n$speaker:\n" if ($lastspeaker ne $speaker);
    print "$t  $body\n";

    $lastdate = $d;
    $lasttime = $t;
    $lastspeaker = $speaker;
}
$sth->finish();
$link->disconnect();

exit 0;

