# xmpp_archive

This is an extremely special-case utility, and I don't even use it any more.

At the time, I was running ejabberd on my server so people could contact me
via XMPP federation, and this script would read the MySQL database for the
data ejabberd stored for JEP-0136 support, and break that data up
appropriately into Maildir files.

In laymen's terms: it took my Google Talk chat logs and archived them to a
mail folder.

This was a really handy thing that would run in a cronjob and try to keep
individual units of conversation split into separate emails, but to be
clear, I don't host an XMPP server any more--everyone I talked with moved
on to other services, like iMessage and later, Slack and Discord...and I
don't know if the database schema has changed since, etc, so use at your
own risk.

Still, this was useful at one time, so I'm leaving it here in case it is
useful again.

--ryan.
