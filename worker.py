import socket
import serializer

class Worker:
    def __init__(self, ip, port, buff_size=8192):
        self.__data = {}
        self.buff_size=buff_size
        self.serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serv.bind((ip, port))
        self.serv.listen(1)  # мастер может быть только один

    def listen(self):
        # while True:
        print("INFO: Waiting connection from master node")
        conn, addr = self.serv.accept()
        print("INFO: Master {} connected".format(addr))
        while True:
            data = conn.recv(self.buff_size)
            if not data: break
            try:
                cmd, *args = serializer.encode_command(data)
                if cmd == "SELECT":
                    if len(args) == 0:
                        for k, v in self.__data.items():
                            encoded_data = serializer.decode_answer(key=k,
                                                                    value=v)
                            conn.send(encoded_data)
                    #conn.send(None)
                    if args[0] in self.__data:
                        encoded_data = serializer.decode_answer(key=args[0],
                                                                value=self.__data[args[0]])
                        conn.send(encoded_data)
                if cmd == "INSERT":
                    self.__data[args[0]] = args[1]
                raise RuntimeError("Wrong command {}".format((cmd, *args)))
            except Exception() as e:
                print("WARNING: Execute '{}' failed.\n"+
                      "Exception - '{}'".format(data, e))
        print("INFO: Connection closed.")
        conn.close()


if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("ip")
    parser.add_argument("port", type=int)
    parser.add_argument("--buff_size", default=8192, type=int)
    args = parser.parse_args()

    worker = Worker(args.ip, args.port, args.buff_size)
    while True:
        try:
            worker.listen()
        except KeyboardInterrupt():
            print("INFO: Stoping worker {}:{}".format(args.ip, args.port))
