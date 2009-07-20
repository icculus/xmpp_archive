#!/usr/bin/perl -w

use warnings;
use strict;
use DBI;
use Encode qw( decode_utf8 );
use POSIX;
use File::Copy;
use Date::Manip qw(UnixDate);

my $VERSION = '1.0.5';

my $gaptime = (30 * 60);
my $timezone = strftime('%Z', localtime());

# Fixes unicode dumping to stdio...hopefully you have a utf-8 terminal by now.
binmode(STDOUT, ":utf8");
binmode(STDERR, ":utf8");

my $dbinfo = undef;
my $xmppuser = undef;
my $maildir = undef;
my $cmdok = 1;

my $debug = 0;
sub dbgprint {
    print @_ if $debug;
}

sub usage {
    die("USAGE: $0 <dbinfo> <xmppuser> <maildir>\n");
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

sub utc_to_rfc822 {
    my $utc = shift;
    return UnixDate("$utc UTC", '%a, %d %b %Y %H:%M %Z');
}

sub utc_to_local {
    my $utc = shift;
    return UnixDate("$utc UTC", '%Y-%m-%d %H:%M:%S %Z');
}

sub talk_gap {
    my ($lastdate, $lasttime, $utc) = @_;
    my $a = make_timestamp("$lastdate $lasttime:00", $timezone);
    my $b = make_timestamp($utc, 'UTC');
    my $time1 =  ($a < $b) ? $a : $b;
    my $time2 =  ($a < $b) ? $b : $a;
    return (($time2 - $time1) >= $gaptime);
}


my $tmpemail = undef;
my $writing = 0;

sub fail {
    my $err = shift;
    close(TMPEMAIL) if ($writing);
    $writing = 0;
    unlink($tmpemail) if (defined $tmpemail);
    die("$err\n");
}


my $startid = 0;
my %startids = ();
my $lastarchivefname = undef;
my $lastarchivetmpfname = undef;

sub flush_startid {
    my ($with, $msgid) = @_;
    $startids{$with} = $msgid;
    return 0 if (not open(LASTID,'>',$lastarchivetmpfname));
    my $startval = 0;
    $startval = $startid if (defined $startid);
    print LASTID "$startval\n";
    foreach(keys %startids) {
        my $key = $_;
        my $val = $startids{$key};
        print LASTID "$key\n$val\n";
    }
    close(LASTID);

    if (not move($lastarchivetmpfname, $lastarchivefname)) {
        unlink($lastarchivetmpfname);
        dbgprint("Rename '$lastarchivetmpfname' to '$lastarchivefname' failed: $!");
        return 0;
    }
    return 1;
}

my $outmsgid = undef;
my $outwith = undef;
my $outid = undef;
my $outtimestamp = undef;
sub flush_conversation {
    my $trash = shift;
    return if (not defined $tmpemail);
    if ($writing) {
        close(TMPEMAIL);
        $writing = 0;
        if ($trash) {
            dbgprint("Trashed conversation in '$tmpemail'\n");
            unlink($tmpemail);
            return;
        }

        fail("message id went backwards?!") if ($startids{$outwith} > $outmsgid);

        my $size = (stat($tmpemail))[7];
        my $t = $outtimestamp;
        my $outfile = "$t.$outid.xmpp-chatlog.$xmppuser-$outwith,S=$size";
        $outfile =~ s#/#_#g;
        $outfile = "$maildir/new/$outfile";
        if (move($tmpemail, $outfile)) {
            utime $t, $t, $outfile;  # force it to collection creation time.
            dbgprint "archived '$outfile'\n";
            system("cat $outfile") if ($debug);
        } else {
            unlink($outfile);
            fail("Rename '$tmpemail' to '$outfile' failed: $!");
        }

        # !!! FIXME: this may cause duplicates if there's a power failure RIGHT HERE.
        if (not flush_startid($outwith, $outmsgid)) {
            unlink($outfile);
            fail("didn't flush startids");
        }
    }
}

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
    fail("Caught signal #${sig}!");
}
$SIG{INT} = \&signal_catcher;
$SIG{TERM} = \&signal_catcher;
$SIG{HUP} = \&signal_catcher;

# mainline!

my $redo = 0;

foreach (@ARGV) {
    $debug = 1, next if ($_ eq '--debug');
    $debug = 0, next if ($_ eq '--no-debug');
    $redo = 1, next if ($_ eq '--redo');
    $redo = 0, next if ($_ eq '--no-redo');
    $dbinfo = $_, next if (not defined $dbinfo);
    $xmppuser = $_, next if (not defined $xmppuser);
    $maildir = $_, next if (not defined $maildir);
    usage();
}
usage() if (not defined $dbinfo);
usage() if (not defined $xmppuser);
usage() if (not defined $maildir);

open DBINFO,'<',$dbinfo or die("Couldn't open '$dbinfo': $!\n");
my ($dsn, $dbuser, $dbpass) = <DBINFO>;
chomp($dsn, $dbuser, $dbpass);
close(DBINFO);

$tmpemail = "$maildir/tmp/xmpp-chatlog-tmp-$$.txt";

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

$lastarchivetmpfname = "$maildir/tmp_xmpp_last_archive_msgids.txt";
unlink($lastarchivetmpfname);

$lastarchivefname = "$maildir/xmpp_last_archive_msgids.txt";
unlink($lastarchivefname) if ($redo);
if (open(LASTID,'<',$lastarchivefname)) {
    my $totalid = <LASTID>;
    chomp($totalid);
    $startid = $totalid if ($totalid =~ /\A\d+\Z/);
    dbgprint("startid (total) == $totalid\n");
    while (not eof(LASTID)) {
        my $user = <LASTID>;
        chomp($user);
        my $id = <LASTID>;
        chomp($id);
        if ($id =~ /\A\d+\Z/) {
            $startids{$user} = $id;
            dbgprint("startid '$user' == $id\n");
        }
    }
    close(LASTID);
}

my $link = DBI->connect($dsn, $dbuser, $dbpass, {
    'RaiseError' => 1,
    'mysql_enable_utf8' => 1
});

my $sql = 'select m.id, m.utc, m.dir, m.body,' .
          ' c.id, c.with_user, c.with_server, c.with_resource,' .
          ' c.utc, c.change_utc' .
          ' from archive_messages as m' .
          ' inner join archive_collections as c on (m.coll_id = c.id)' .
          " where (m.id > '$startid')" .
          " and (c.us = '$xmppuser')" .
          ' order by c.with_user, c.with_server, m.id';
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

    if (not defined $startids{$with}) {
        dbgprint("Flushing new per-user startid for '$with'\n");
        if (not flush_startid($with, 0)) {
            fail("didn't flush new startid for '$with'");
        }
    }

    if ( (time() - make_timestamp($utc, 'UTC')) < $gaptime ) {
        dbgprint("timestamp '$utc' is less than $gaptime seconds old.\n");
        if ((not defined $startid) or ($msgid < $startid)) {
            $startid = ($msgid-1);
            dbgprint("forcing global startid to $startid\n");
        }
        # trash this conversation, it might still be ongoing.
        flush_conversation(1) if ($with eq $lastwith);
        next;
    }

    $newestmsgid = $msgid if ($msgid > $newestmsgid);

    # this happens if we had a conversation that was still ongoing when a 
    #  newer conversation got archived. Next run, the newer conversation
    #  gets pulled from the database again so we can recheck the older
    #  conversation.
    if ($msgid <= $startids{$with}) {
        dbgprint("msgid $msgid is already archived.\n");
        next;
    }

    # Try to merge collections that appear to be the same conversation...
    if (($with ne $lastwith) or (talk_gap($lastdate, $lasttime, $utc))) {
        flush_conversation(0);

        open(TMPEMAIL,'>',$tmpemail) or fail("Failed to open '$tmpemail': $!");
        binmode(TMPEMAIL, ":utf8");
        $outtimestamp = make_timestamp($change_utc, 'UTC');
        $outwith = $with;
        $outid = $coll_id;
        $writing = 1;

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

        my $emaildate = utc_to_rfc822($coll_utc);
        my $localdate = utc_to_local($coll_utc);

        print TMPEMAIL "Return-Path: <$xmppuser>\n";
        print TMPEMAIL "Delivered-To: $xmppuser\n";
        print TMPEMAIL "MIME-Version: 1.0\n";
        print TMPEMAIL "Content-Type: text/plain; charset=\"utf-8\"\n";
        print TMPEMAIL "Content-Transfer-Encoding: binary\n";
        print TMPEMAIL "X-Mailer: archive_jabber.pl $VERSION\n";
        print TMPEMAIL "From: $xmppuser\n";
        print TMPEMAIL "To: $xmppuser\n";
        print TMPEMAIL "Date: $emaildate\n";
        print TMPEMAIL "Subject: Chat with $person at $localdate ...\n";

        $lastwith = $with;
        $lastspeaker = '';
        $lastdate = '';
        $lasttime = '';
    }

    # replace "/me does something" with "*does something*" ...
    $body =~ s#\A/me (.*)\Z#*$1*#;

    my $speaker = $dir ? $thisxmppuseralias : $alias;
    my ($d, $t) = split_date_time(make_timestamp($utc, 'UTC'));

    if ((defined $lastdate) and ($lastdate ne $d)) {
        print TMPEMAIL "\n$d\n";
        $lastspeaker = '';  # force it to redraw.
    }

    print TMPEMAIL "\n$speaker:\n" if ($lastspeaker ne $speaker);
    print TMPEMAIL "$t  $body\n";

    $lastdate = $d;
    $lasttime = $t;
    $lastspeaker = $speaker;
    $outmsgid = $msgid;
}
$sth->finish();
$link->disconnect();

if (defined $startid) {
    dbgprint("Final startid is $startid\n");
} else {
    $startid = $newestmsgid;
    dbgprint("No definite global startid; using $startid\n");
}

flush_conversation(0);

exit 0;

