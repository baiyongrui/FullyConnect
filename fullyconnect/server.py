import sys
import os
import logging
import signal
import asyncio
import uvloop

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../'))
from fullyconnect import shell, daemon, tcprelay, udprelay


def main():

    config = shell.get_config()

    daemon.daemon_exec(config)

    if config['port_password']:
        if config['password']:
            logging.warning('warning: port_password should not be used with '
                            'server_port and password. server_port and password '
                            'will be ignored')
    else:
        config['port_password'] = {}
        server_port = config['server_port']
        if type(server_port) == list:
            for a_server_port in server_port:
                config['port_password'][a_server_port] = config['password']
        else:
            config['port_password'][str(server_port)] = config['password']

    tcp_servers = []
    udp_servers = []

    port_password = config['port_password']
    del config['port_password']
    for port, password in port_password.items():
        a_config = config.copy()
        a_config['server_port'] = int(port)
        a_config['password'] = password
        logging.info("starting server at %s:%d" %
                     (a_config['server'], int(port)))
        tcp_servers.append(tcprelay.TCPRelayServer(a_config))
        udp_servers.append(udprelay.UDPRelayServer(a_config))

    def run_server():
        def child_handler(signum, _):
            logging.warning('received SIGQUIT, doing graceful shutting down..')
            list(map(lambda s: s.close(next_tick=True),
                     tcp_servers + udp_servers))
        signal.signal(getattr(signal, 'SIGQUIT', signal.SIGTERM),
                      child_handler)

        def int_handler(signum, _):
            sys.exit(1)
        signal.signal(signal.SIGINT, int_handler)

        try:
            loop = uvloop.new_event_loop()
            asyncio.set_event_loop(loop)
            list(map(lambda s: s.add_to_loop(loop), tcp_servers + udp_servers))

            daemon.set_user(config.get('user', None))
            loop.run_forever()
        except Exception as e:
            shell.print_exception(e)
            sys.exit(1)
        finally:
            # TODO should close server?
            loop.close()

    if int(config['workers']) > 1:
        if os.name == 'posix':
            children = []
            is_child = False
            for i in range(0, int(config['workers'])):
                r = os.fork()
                if r == 0:
                    logging.info('worker started')
                    is_child = True
                    run_server()
                    break
                else:
                    children.append(r)
            if not is_child:
                def handler(signum, _):
                    for pid in children:
                        try:
                            os.kill(pid, signum)
                            os.waitpid(pid, 0)
                        except OSError:  # child may already exited
                            pass
                    sys.exit()
                signal.signal(signal.SIGTERM, handler)
                signal.signal(signal.SIGQUIT, handler)
                signal.signal(signal.SIGINT, handler)

                # master
                for a_tcp_server in tcp_servers:
                    a_tcp_server.close()
                for a_udp_server in udp_servers:
                    a_udp_server.close()

                for child in children:
                    os.waitpid(child, 0)
        else:
            logging.warn('worker is only available on Unix/Linux')
            run_server()
    else:
        run_server()


if __name__ == '__main__':
    main()