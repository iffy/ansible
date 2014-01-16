# (c) 2012, Michael DeHaan <michael.dehaan@gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
#

from twisted.internet.protocol import ProcessProtocol, Protocol, Factory
from twisted.conch.endpoints import SSHCommandClientEndpoint
from twisted.conch.ssh.keys import Key
from twisted.internet import defer, reactor, error
from twisted.web.client import FileBodyProducer

from StringIO import StringIO

from crochet import setup, wait_for_reactor
setup()


import pipes

from ansible.callbacks import vvv, vvvv


class AuthenticationLacking(Exception):
    pass




class EveryoneIsAKnownHostsFile(object):
    """
    XXX untested
    """

    def verifyHostKey(self, ui, hostname, ip, key):
        return defer.succeed(True)



class _CommandProtocol(Protocol):

    output_childFD = 1
    err_childFD = 2


    def __init__(self, process_protocol):
        self.done = defer.Deferred()
        self.process_protocol = process_protocol


    def connectionMade(self):
        self.process_protocol.makeConnection(self)
        
        # ---------------------------------------------------------------------
        # XXX terrible monkey patch
        self.transport.extReceived = self.extReceived
        # XXX FIX THIS TERRIBLE MONKEY PATCH
        # ---------------------------------------------------------------------


    def connectionLost(self, reason):
        self.process_protocol.processEnded(reason)
        self.done.callback(self)


    def dataReceived(self, data):
        self.process_protocol.childDataReceived(self.output_childFD, data)

    def extReceived(self, dataType, data):
        self.process_protocol.childDataReceived(self.err_childFD, data)


    # ITransport

    def write(self, data):
        self.transport.write(data)


    def closeStdin(self):
        self.transport.conn.sendEOF(self.transport)


class _StdinConsumer(ProcessProtocol):
    """
    I write a file to stdin from a producer.
    """

    def __init__(self, producer):
        self.producer = producer

    def connectionMade(self):
        d = self.producer.startProducing(self)
        d.addCallback(self._doneProducing)


    def write(self, data):
        vvv('writing %d %r' % (len(data), data,))
        self.transport.write(data.encode('utf-8'))
        vvv('wrote')


    def _doneProducing(self, _):
        vvv('done producing')
        self.transport.closeStdin()



class SSHConnection(object):
    """
    XXX
    """


    def __init__(self, master_proto):
        """
        @param master_proto: XXX
        """
        self.master_proto = master_proto


    def close(self):
        self.master_proto.transport.loseConnection()
        return self.master_proto.done


    @wait_for_reactor
    def spawnProcess(self, protocol, command):
        vvvv('SPAWN %r' % (command,))
        
        # copy the script over
        from hashlib import sha1
        filename = '/tmp/script%s' % (sha1(command).hexdigest(),)
        new_command = '/bin/sh %s' % (pipes.quote(filename),)
        d = self.copyFile(filename, FileBodyProducer(StringIO(command)))

        # then run it
        d.addCallback(lambda _: self._spawnProcess(protocol, new_command))
        return d

    def _spawnProcess(self, protocol, command):
        vvvv('ACTUALLY SPAWN: %r' % (command,))
        factory = Factory()
        factory.protocol = lambda: _CommandProtocol(protocol)
        e = SSHCommandClientEndpoint.existingConnection(
                self.master_proto.transport.conn,
                command)
        d = e.connect(factory)
        vvvv('STARTING')
        return d.addCallbacks(self._commandStarted, self._commandFailedToStart)


    def _commandStarted(self, proto):
        vvvv('STARTED')
        return proto.done


    def _commandFailedToStart(self, err):
        vvvv('Command failed to start')


    def copyFile(self, path, producer):
        """
        XXX
        """
        vvvv('PUT %r %r' % (path, producer))
        # XXX brutish version
        brute = _StdinConsumer(producer)
        return self._spawnProcess(brute, 'cat > %s' % (pipes.quote(path),))


class SimpleProtocol(ProcessProtocol):
    """
    XXX

    @ivar done: A callback that will be called back with a ProcessDone instance
        or else errback with a ProcessTerminated instance.
    """

    _stdin_closed = False
    rc = None

    def __init__(self, stdin=None, outputReceivers=None, triggers=None):
        """
        @param catchall: A function to be called with output not sent to
            one of C{outputReceivers}.
        @param outputReceivers: A dictionary of file descriptor numbers
            to functions that will be called with each piece of data received
            on that file descriptor.
        @param triggers: A list of triggers.  See L{addTrigger} for more
            information.
        """
        self.done = defer.Deferred()

        self.stdin = stdin
        self.output = {0:'', 1:'', 2:'', 3:''}


    def connectionMade(self):
        vvvv('connectionMade')
        if self.stdin:
            self.transport.write(self.stdin)

        self.transport.closeStdin()


    def childDataReceived(self, childFD, data):
        vvvv('childDataReceived %r %s' % (childFD, len(data)))
        self.output[childFD] += data


    def processEnded(self, status):
        vvvv('processEnded %r' % (status,))
        if isinstance(status.value, error.ProcessTerminated):
            self.rc = 1
            self.done.errback(status.value)
        else:
            self.rc = 0
            self.done.callback(status.value)





class _PersistentProtocol(Protocol):


    def __init__(self):
        self.done = defer.Deferred()


    def connectionLost(self, reason):
        self.done.callback(self)




class SSHConnectionMaker(object):
    """
    I make L{SSHConnection}s from URIs.
    """


    def __init__(self, askForPassword=None):
        """
        @param askForPassword: Either C{None} if passwords can't be asked for,
            or else a function that takes a string prompt and returns a
            (potentially deferred) password.
        """
        self.askForPassword = askForPassword


    @wait_for_reactor
    def getConnection(self, host, port, user, password, private_key_file):
        if password is None:
            if self.askForPassword:
                d = defer.maybeDeferred(self.askForPassword, 'Password?\n')
                return d.addCallback(self._gotPassword, host, port, user, private_key_file)
            elif not private_key_file:
                return defer.fail(AuthenticationLacking(
                        "You must supply either a password or an identity."))
        return self._connect(host, port, user, password, private_key_file)


    def _gotPassword(self, password, host, port, user, private_key_file):
        return self._connect(host, port, user, password, private_key_file)


    def _connect(self, host, port, user, password, private_key_file):
        keys = [Key.fromFile(private_key_file)]
        ep = SSHCommandClientEndpoint.newConnection(
                reactor,
                b'/bin/cat',
                user,
                host,
                port=port,
                password=password,
                keys=keys,
                agentEndpoint=None,
                knownHosts=EveryoneIsAKnownHostsFile())
        factory = Factory()
        factory.protocol = _PersistentProtocol
        return ep.connect(factory).addCallback(self._connected)


    def _connected(self, proto):
        return SSHConnection(proto)


class _ConnectedConnection(object):

    has_pipelining = True

    def __init__(self, conn, runner, host, port, user, password, private_key_file):
        self._conn = conn
        self.runner = runner
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.private_key_file = private_key_file

    def exec_command(self, cmd, tmp_path, sudo_user, sudoable=False,
                     executable='/bin/sh', in_data=None):
        """
        Run a remote command.
        """
        vvv('exec_command %r %r %r %r %r' % (cmd, len(in_data or ''), tmp_path, sudo_user, sudoable),
            host=self.host)
        proto = SimpleProtocol(in_data)
        if sudoable:
            cmd = 'sudo ' + cmd
        try:
            self._conn.spawnProcess(proto, cmd)
        except Exception as e:
            vvv(str(e))
        return proto.rc, proto.stdin, proto.output[1], proto.output[2]
        

    def put_file(self, in_path, out_path):
        """
        Transfer a file from local to remote.
        """
        vvv('PUT', host=self.host)
        self._conn.copyFile(out_path, FileBodyProducer(open(in_path, 'rb')))

    def fetch_file(self, in_path, out_path):
        """
        Transfer a file from remote to local.
        """
        vvv('FETCH', host=self.host)
        print 'fetch_file %r %r' % (in_path, out_path)

    def close(self):
        """
        Close the connection.  Actually, we're not going to :)
        """
        vvv('NOT CLOSING', host=self.host)


class Connection(object):
    """
    Twisted Conch connnection
    """

    _conn = None
    _conns = {}


    def __init__(self, runner, host, port, user, password, private_key_file,
                 *args, **kwargs):
        self.runner = runner
        self.host = host
        self.ipv6 = ':' in self.host
        self.port = port
        self.user = user
        self.password = password
        self.private_key_file = private_key_file
        self.has_pipelining = True

        self._conn_key = (host, port, user, password, private_key_file)


    def connect(self):
        vvv("ESTABLISH CONNECTION FOR USER: %s" % (self.user,), host=self.host)

        if self._conn_key not in self._conns:
            maker = SSHConnectionMaker()
            txconn = maker.getConnection(self.host, self.port, self.user,
                                         self.password, self.private_key_file)
            conn = _ConnectedConnection(
                txconn,
                self.runner, self.host, self.port, self.user, self.password,
                self.private_key_file)
            self._conns[self._conn_key] = conn
            vvv("CONNECTED", host=self.host)
        else:
            vvv("REUSING OPEN CONNECTION", host=self.host)

        return self._conns[self._conn_key]

    
    

