import socket
import serializer
import sys

class Worker:
    def __init__(self, ip, port):
        self.__data = {}
        self.serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serv.bind((ip, port))
        self.serv.listen(1)  # мастер может быть только один

    def listen(self):
        # while True:
        print("WORKER INFO: Waiting connection from master node", file=sys.stderr)
        conn, addr = self.serv.accept()
        print("WORKER INFO: Master {} connected".format(addr), file=sys.stderr)
        num_commands = 0
        while True:
            data = conn.recv(serializer.COMMAND_SIZE)
            if not data: break
            try:
                cmd, *args = serializer.encode_command(data)
                print("WORKER INFO: command {} {}".format(cmd, args), file=sys.stderr)
                num_commands += 1
                if cmd == "SELECT":
                    if len(args) == 0:
                        for k, v in self.__data.items():
                            encoded_data = serializer.decode_answer(key=k,
                                                                    value=v)
                            conn.send(encoded_data)
                        conn.send(b'0')
                    elif args[0] in self.__data:
                        encoded_data = serializer.decode_answer(key=args[0],
                                                                value=self.__data[args[0]])
                        conn.send(encoded_data)
                    else:
                        conn.send(b'0')
                elif cmd == "INSERT":
                    self.__data[args[0]] = args[1]
                    print("WORKER INFO: INSERTED key={}, value={}".format(*args), file=sys.stderr)
                else:
                    raise RuntimeError("Wrong command {}".format((cmd, *args)))
            except Exception as e:
                print("WARNING: Execute '{}' failed.\n".format(data) +
                      "Exception - '{}'".format(e), file=sys.stderr)
        print("WORKER INFO: Connection closed.", file=sys.stderr)
        print("WORKER INFO: Num commands {}. Data contains {}".format(num_commands, len(self.__data)),
              file=sys.stderr)
        conn.close()


if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("ip")
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    print(vars(args), file=sys.stderr)
    worker = Worker(args.ip, args.port)
    #while True:
    try:
        worker.listen()
    except KeyboardInterrupt:
        print("WORKER INFO: Stoping worker {}:{}".format(args.ip, args.port))
        #break
    exit(0)